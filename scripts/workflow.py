#!/usr/bin/env python3
"""One-shot branch workflow and merge races (2026-10-05, review R1 MC4 / R4 MC4-MC5 / G5).

Part 1 (trace): a deterministic end-to-end lifecycle on a 2,000-row TPC-H base -- create a branch
pinned at a snapshot id, record corrections and upserts on it while main receives ingestion and
compaction, merge with a concurrent commit on main (retryable, re-planned), recreate the branch from
the merge snapshot and merge again, a true conflict with its net-effect witness, and a merge whose
history was removed (unsupported: missing history, target unchanged).  Every step records its outcome
category, latency, re-sequencing and cleanup, and every emitted snapshot is checked by the oracle.

Part 2 (races): for generated scenarios (all seven mixes), a commit is injected while the merge is
between planning and its catalog commit -- on an unrelated branch, on the source branch, or on the
target branch -- at a random emission stage.  The merge pins the source snapshot; a lost commit is
re-sequenced; a moved target is retryable and the caller re-plans (at most 3 attempts).  Every final
merge is checked against the serial orders of the operations it actually merged.

Records: results/<campaign>/raw/<id>.json (fingerprint over code, config and id), campaign.json,
environment.json.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.envinfo import sha256_file, write_environment  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.scenarios import MIXES, SourcePool, generate, scenario_seed  # noqa: E402
from snapmerge.sources import load_source, source_paths  # noqa: E402
from snapmerge.writer import LogicalOp, apply_op, write_base  # noqa: E402

STAGES = ("after_delete_files", "after_manifest_list", "before_catalog_commit")
TARGETS = ("other", "source", "target")
MAX_ATTEMPTS = 3


def code_files() -> list[Path]:
    return sorted((ROOT / "src" / "snapmerge").glob("*.py")) + [Path(__file__).resolve()]


def code_fingerprint() -> str:
    h = hashlib.sha256()
    for p in code_files():
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def atomic_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


def fresh_rows(base: pa.Table, n: int, salt: int) -> pa.Table:
    t = base.slice(0, n)
    return t.set_column(t.schema.get_field_index("rid"), "rid",
                        pc.add(t.column("rid"), pa.scalar(5 * 10**12 + salt * 10**6, pa.int64())))


def check(oracle: Oracle, table, sid: int, base: pa.Table, ops_a, ops_c) -> dict:
    orc = oracle.serial(base, ops_a, ops_c)
    chk = oracle.check_snapshot(table, sid, ["o_ac", "o_ca"])
    return {"true_conflict": orc["true_conflict"],
            "eq_ac": chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"],
            "eq_ca": chk["duckdb_eq_o_ca"] and chk["pyiceberg_eq_o_ca"], "readers_agree": chk["readers_agree"]}


def outcome(out) -> dict:
    j = out.to_json()
    return {k: j[k] for k in ("verdict", "category", "rule", "latency_ms", "source_snapshot", "snapshot_id")} | \
        {"resequenced": out.details.get("resequenced", 0), "witness": out.witness}


# ============================================================================ part 1: trace

def trace(pool: SourcePool, work: Path, oracle: Oracle) -> list[dict]:
    sc = generate(pool, "filter_corrections", scenario_seed(20261005, "tpch_lineitem", "trace", 0), 2000)
    base = sc.base
    shutil.rmtree(work, ignore_errors=True)
    cat = C.open_catalog(work)
    t = C.create_table(cat, "t.t", base.schema, ("l_shipdate", "month"))
    t = write_base(t, base, chunks=2)
    steps = []
    b0 = C.head_id(t, "main")
    t = C.create_branch(t, "fix", b0)
    steps.append({"step": "create branch fix pinned at the base snapshot", "snapshot": b0})
    # corrections on the branch: a copy-on-write correction and a merge-on-read upsert
    p0, p1 = sc.meta["partitions"][0], sc.meta["partitions"][1]
    from snapmerge.sources import partition_bounds
    ttype = base.schema.field("l_shipdate").type

    def ppred(p):
        lo, hi = partition_bounds(pool.spec, p, ttype)
        return X.and_(X.cmp("l_shipdate", ">=", lo), X.cmp("l_shipdate", "<", hi))

    in_p0 = base.filter(X.eval_arrow(ppred(p0), base))
    corr = in_p0.slice(0, 5)
    corr = corr.set_column(corr.schema.get_field_index("l_discount"), "l_discount",
                           pa.array([0.09] * corr.num_rows, pa.float64()))
    fix_ops = [LogicalOp("OVERWRITE", filter=X.and_(ppred(p0), X.isin("rid", corr.column("rid").to_pylist())),
                         rows=corr, mode="cow"),
               LogicalOp("UPSERT", rows=fresh_rows(in_p0, 3, 1), keys=fresh_rows(in_p0, 3, 1).column("rid").to_pylist(),
                         mode="mor")]
    for i, op in enumerate(fix_ops):
        t = apply_op(t, "fix", op, f"fix{i}")
    steps.append({"step": "record a COW correction and a MOR upsert on fix", "ops": [o.summary() for o in fix_ops]})
    main_ops = [LogicalOp("APPEND", rows=fresh_rows(base.filter(X.eval_arrow(ppred(p1), base)), 20, 2)),
                LogicalOp("REPLACE", filter=ppred(p0), sort_by="l_partkey")]
    for i, op in enumerate(main_ops):
        t = apply_op(t, "main", op, f"main{i}")
    steps.append({"step": "main ingests into another partition and compacts the corrected partition",
                  "ops": [o.summary() for o in main_ops]})
    late = LogicalOp("APPEND", rows=fresh_rows(base.filter(X.eval_arrow(ppred(p1), base)), 2, 3))
    fired = []

    def hook(stage):
        if stage == "before_catalog_commit" and not fired:
            apply_op(cat.load_table("t.t"), "main", late, "main-late")
            fired.append(stage)

    pinned = C.head_id(t, "fix")
    out = snapmerge(t, "main", pinned, crash_hook=hook)
    steps.append({"step": "merge fix into main while main receives a concurrent commit", **outcome(out)})
    t = cat.load_table("t.t")
    out = snapmerge(t, "main", pinned)
    t = (out.table or t).refresh()
    main_all = main_ops + [late]
    steps.append({"step": "re-plan against the moved target and retry", **outcome(out),
                  "check": check(oracle, t, out.snapshot_id, base, main_all, fix_ops) if out.emitted else None})
    merge1 = out.snapshot_id
    # recreate the branch from the merge snapshot, edit again, merge again (base = merge snapshot)
    t.manage_snapshots().remove_branch("fix").commit()
    t = C.create_branch(t.refresh(), "fix", merge1)
    fix2 = [LogicalOp("DELETE", filter=X.and_(ppred(p1), X.cmp("l_discount", "=", 0.0)), mode="mor")]
    t = apply_op(t, "fix", fix2[0], "fix2")
    main2 = [LogicalOp("APPEND", rows=fresh_rows(in_p0, 4, 4))]
    t = apply_op(t, "main", main2[0], "main2")
    out = snapmerge(t, "main", "fix")
    t = (out.table or t).refresh()
    merged1_rows = C.scan_pyiceberg(t, merge1)
    steps.append({"step": "recreate fix from the merge snapshot, edit, merge again", **outcome(out),
                  "check": check(oracle, t, out.snapshot_id, merged1_rows.select(base.schema.names).cast(base.schema),
                                 main2, fix2) if out.emitted else None})
    # a true conflict: the branch appends a row that main's later delete matches
    head = C.head_id(t, "main")
    t = C.create_branch(t, "bad", head)
    row = fresh_rows(in_p0, 1, 5)
    row = row.set_column(row.schema.get_field_index("l_discount"), "l_discount", pa.array([0.11], pa.float64()))
    t = apply_op(t, "bad", LogicalOp("APPEND", rows=row), "bad0")
    t = apply_op(t, "main", LogicalOp("DELETE", filter=X.cmp("l_discount", ">=", 0.105), mode="mor"), "main3")
    out = snapmerge(t, "main", "bad")
    steps.append({"step": "a true conflict is reported with a net-effect witness", **outcome(out)})
    # missing history: the branch's inserted file was removed by an over-eager cleanup
    t = C.create_branch(out.table.refresh(), "gone", C.head_id(t, "main"))
    t = apply_op(t, "gone", LogicalOp("APPEND", rows=fresh_rows(in_p0, 2, 6)), "gone0")
    t = apply_op(t, "main", LogicalOp("DELETE", filter=X.cmp("l_discount", ">=", 0.105), mode="mor"), "main4")
    lost = [f for f in C.load_state(t, C.head_id(t, "gone")).data_files() if f.snapshot_id == C.head_id(t, "gone")]
    for f in lost:
        Path(C.local_path(f.path)).unlink()
    before = C.head_id(t, "main")
    out = snapmerge(t, "main", "gone")
    steps.append({"step": "merge after history was removed fails closed", **outcome(out),
                  "target_unchanged": C.head_id(out.table.refresh(), "main") == before})
    return steps


# ============================================================================ part 2: races

def race(pool: SourcePool, mix: str, i: int, seed_base: int, work: Path, oracle: Oracle) -> dict:
    seed = scenario_seed(seed_base, "tpch_lineitem", mix, i)
    sc = generate(pool, mix, seed, 2000)
    rng = np.random.default_rng(seed ^ 0xACE)
    target_kind = TARGETS[i % 3]
    stage = STAGES[int(rng.integers(0, 3))]
    shutil.rmtree(work, ignore_errors=True)
    cat = C.open_catalog(work)
    t = C.create_table(cat, "t.t", sc.base.schema, ("l_shipdate", "month"))
    t = write_base(t, sc.base, chunks=2)
    bid = C.head_id(t, "main")
    for b in ("A", "C", "other"):
        t = C.create_branch(t, b, bid)
    for side in ("A", "C"):
        for k, op in enumerate(sc.ops[side]):
            t = apply_op(t, side, op, f"{side}{k}")
    t = C.create_branch(t, "m", C.head_id(t, "A"))
    inject = LogicalOp("APPEND", rows=fresh_rows(sc.base, int(rng.integers(1, 4)), i))
    branch = {"other": "other", "source": "C", "target": "m"}[target_kind]
    fired = []

    def hook(s):
        if s == stage and not fired:
            apply_op(cat.load_table("t.t"), branch, inject, "X0")
            fired.append(s)

    pinned = C.head_id(t, "C")
    attempts = []
    out = snapmerge(t, "m", pinned, crash_hook=hook)
    attempts.append(outcome(out))
    while out.to_json()["category"] == "retryable" and len(attempts) < MAX_ATTEMPTS:
        out = snapmerge(cat.load_table("t.t"), "m", pinned)
        attempts.append(outcome(out))
    rec = {"id": f"{mix}-{i:04d}", "mix": mix, "seed": seed, "injected_on": target_kind, "stage": stage,
           "fired": bool(fired), "attempts": attempts, "final_category": attempts[-1]["category"],
           "orphans_after": None}
    ops_a = list(sc.ops["A"]) + ([inject] if fired and target_kind == "target" else [])
    if out.emitted:
        rec["check"] = check(oracle, out.table.refresh(), out.snapshot_id, sc.base, ops_a, sc.ops["C"])
    else:
        rec["true_conflict"] = oracle.serial(sc.base, ops_a, sc.ops["C"])["true_conflict"]
    return rec


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--per-mix", type=int, default=30)
    ap.add_argument("--seed-base", type=int, default=20261005)
    ap.add_argument("--sf", type=float, default=0.1)
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--work-root", default=str(ROOT / "work"))
    a = ap.parse_args(argv)
    rdir = Path(a.results_root) / a.campaign
    code_fp = code_fingerprint()
    srcs = source_paths("tpch_lineitem", sf=a.sf)
    cfg = {"workflow": "v1", "per_mix": a.per_mix, "seed_base": a.seed_base, "sf": a.sf, "mixes": list(MIXES),
           "source_sha256": {p.name: sha256_file(p) for p in srcs if p.exists()}}
    meta_path = rdir / "campaign.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta["code_fingerprint"] != code_fp or meta["config"] != cfg:
            print(f"ABORT: {rdir} was produced by different code or config", file=sys.stderr)
            return 2
    else:
        atomic_json(meta_path, {"campaign": a.campaign, "config": cfg, "code_fingerprint": code_fp,
                                "created": time.strftime("%Y-%m-%dT%H:%M:%S")})
    write_environment(rdir, campaign=a.campaign, config=cfg, code_fingerprint=code_fp, code_files=code_files(),
                      source_files=srcs, argv=sys.argv if argv is None else list(argv))
    spec, src = load_source("tpch_lineitem", sf=a.sf)
    pool = SourcePool(spec, src)
    oracle = Oracle()
    work = Path(a.work_root) / a.campaign
    ids = ["trace"] + [f"{m}-{i:04d}" for m in MIXES for i in range(a.per_mix)]
    new = skipped = 0
    for rid in ids:
        path = rdir / "raw" / f"{rid}.json"
        fp = hashlib.sha256(json.dumps([code_fp, cfg, rid]).encode()).hexdigest()[:24]
        if path.exists() and json.loads(path.read_text()).get("fingerprint") == fp:
            skipped += 1
            continue
        try:
            if rid == "trace":
                rec = {"id": "trace", "steps": trace(pool, work / "trace", oracle)}
            else:
                mix, i = rid.rsplit("-", 1)
                rec = race(pool, mix, int(i), a.seed_base, work / "race", oracle)
        except Exception as e:
            rec = {"id": rid, "error": repr(e), "traceback": traceback.format_exc()[-2500:]}
        rec["fingerprint"], rec["code_fingerprint"] = fp, code_fp
        atomic_json(path, rec)
        new += 1
        if rid == "trace" or new % 20 == 0:
            print(f"[{new}] {rid} {rec.get('final_category', '')} {rec.get('error', '')[:200]}", flush=True)
    oracle.close()
    shutil.rmtree(work, ignore_errors=True)
    print(f"done: new={new} skipped={skipped} total={len(ids)} pending={len(ids) - new - skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
