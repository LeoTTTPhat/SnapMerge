#!/usr/bin/env python3
"""Exhaustive small physical-state enumerator (2026-10-05, review R2 MC5).

Enumerates every pair (target sequence, source sequence) from a fixed menu of physical operation
sequences over duplicate-rich base states and file layouts (two values with multiplicities 0-2 in a
contended month, one untouched row in another month, one or two base files), and for every case
  * runs SnapMerge (and records its outcome category);
  * on a merge, checks both readers against both serial orders AND the per-position liveness of the
    merged snapshot (manifests + delete files, catalog.live_rows) against the multiplicities N*(v) of
    Corollary 4.3 computed directly from the formula with the net operations of Lemma 4.1, and checks
    that every delete file references a live data file with a sequence number at least its own;
  * on a conflict, records whether the case is a true conflict (oracle) and the conflict class.
The menu covers MOR and COW deletes, appends of duplicate and fresh rows, compaction (once and
twice), upserts (MOR, COW), same-side MOR deletes on a file the side added, a COW delete followed by
compaction, and a copy-on-write overwrite committed as ONE snapshot (mixed provenance).

Records: results/<campaign>/raw/<case>.json with a fingerprint over (code, config, case), plus
campaign.json and environment.json, like the other runners; --time-budget-min stops between cases.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import itertools
import json
import os
import shutil
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.envinfo import write_environment  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.writer import LogicalOp, apply_op, write_base  # noqa: E402

SCHEMA = pa.schema([("rid", pa.int64()), ("d", pa.date32()), ("q", pa.float64()), ("s", pa.string())])
NAMES = SCHEMA.names
A_ROW = (1, dt.date(2024, 1, 5), 0.0, "x")
B_ROW = (2, dt.date(2024, 1, 6), 1.0, "y")
OTHER = (3, dt.date(2024, 2, 5), 2.0, "z")
FRESH = (9, dt.date(2024, 1, 7), 0.0, "w")
JAN = X.and_(X.cmp("d", ">=", dt.date(2024, 1, 1)), X.cmp("d", "<", dt.date(2024, 2, 1)))


def tab(*rows) -> pa.Table:
    return pa.table({n: [r[i] for r in rows] for i, n in enumerate(NAMES)}, schema=SCHEMA)


def menu() -> dict[str, list[tuple[LogicalOp, str]]]:
    """name -> [(operation, writer)]"""
    up = (1, dt.date(2024, 1, 5), 5.0, "x")
    return {
        "none": [],
        "mor_del_q0": [(LogicalOp("DELETE", filter=X.cmp("q", "=", 0.0), mode="mor"), "split")],
        "cow_del_q0": [(LogicalOp("DELETE", filter=X.cmp("q", "=", 0.0), mode="cow"), "split")],
        "mor_del_q1": [(LogicalOp("DELETE", filter=X.cmp("q", "=", 1.0), mode="mor"), "split")],
        "cow_del_le1": [(LogicalOp("DELETE", filter=X.cmp("q", "<=", 1.0), mode="cow"), "split")],
        "append_dup": [(LogicalOp("APPEND", rows=tab(A_ROW)), "split")],
        "append_fresh": [(LogicalOp("APPEND", rows=tab(FRESH)), "split")],
        "compact": [(LogicalOp("REPLACE", filter=JAN, sort_by="q"), "split")],
        "compact2": [(LogicalOp("REPLACE", filter=JAN, sort_by="q"), "split"),
                     (LogicalOp("REPLACE", filter=JAN, sort_by="rid"), "split")],
        "upsert_mor": [(LogicalOp("UPSERT", rows=tab(up), keys=[1], mode="mor"), "split")],
        "upsert_cow": [(LogicalOp("UPSERT", rows=tab(up), keys=[1], mode="cow"), "split")],
        "append_then_mor_del": [(LogicalOp("APPEND", rows=tab(A_ROW)), "split"),
                                (LogicalOp("DELETE", filter=X.cmp("q", "=", 0.0), mode="mor"), "split")],
        "cow_del_q1_compact": [(LogicalOp("DELETE", filter=X.cmp("q", "=", 1.0), mode="cow"), "split"),
                               (LogicalOp("REPLACE", filter=JAN, sort_by="q"), "split")],
        "single_overwrite_q1": [(LogicalOp("OVERWRITE", filter=X.cmp("q", "=", 1.0),
                                           rows=tab((2, dt.date(2024, 1, 6), 1.5, "y")), mode="cow"), "single")],
    }


BASES = {  # name -> (rows, chunks)
    "a1b1_1f": ([A_ROW, B_ROW, OTHER], 1), "a1b1_2f": ([A_ROW, OTHER, B_ROW], 2),
    "a2b1_1f": ([A_ROW, A_ROW, B_ROW, OTHER], 1), "a2b1_2f": ([A_ROW, B_ROW, A_ROW, OTHER], 2),
    "a2b2_2f": ([A_ROW, B_ROW, OTHER, A_ROW, B_ROW], 2), "a0b2_1f": ([B_ROW, B_ROW, OTHER], 1),
    "a1b0_2f": ([A_ROW, OTHER, A_ROW], 2), "a2b2_1f": ([A_ROW, A_ROW, B_ROW, B_ROW, OTHER], 1),
}


def holds(pred, row: tuple) -> bool:
    if pred is None or X.is_false(pred):
        return False
    return bool(X.eval_arrow(pred, tab(row)).to_pylist()[0])


def n_star(base: list[tuple], ops_a: list[LogicalOp], ops_c: list[LogicalOp]) -> Counter:
    def net(ops):
        preds, ins = [], Counter()
        for op in ops:
            p = op.delete_pred()
            ins = Counter({k: c for k, c in ins.items() if not holds(p, k)})
            preds.append(p)
            r = op.insert_rows()
            if r is not None:
                ins.update(tuple(x[n] for n in NAMES) for x in r.to_pylist())
        return preds, ins

    pa_, ia = net(ops_a)
    pc_, ic = net(ops_c)
    B = Counter(base)
    out = Counter()
    for v in set(B) | set(ia) | set(ic):
        a, c = any(holds(p, v) for p in pa_), any(holds(p, v) for p in pc_)
        n = (B[v] if not a and not c else 0) + (ia[v] if not c else 0) + (ic[v] if not a else 0) + \
            (ia[v] if a and c else 0)
        if n:
            out[v] = n
    return out


def run_case(base_name: str, a_name: str, c_name: str, work: Path, oracle: Oracle) -> dict:
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    rows, chunks = BASES[base_name]
    m = menu()
    cat = C.open_catalog(work)
    t = C.create_table(cat, "t.t", SCHEMA, ("d", "month"))
    t = write_base(t, tab(*rows), chunks=chunks)
    bid = C.head_id(t, "main")
    t = C.create_branch(t, "A", bid)
    t = C.create_branch(t, "C", bid)
    for side, name in (("A", a_name), ("C", c_name)):
        for i, (op, writer) in enumerate(m[name]):
            t = apply_op(t, side, op, f"{side}{i}", writer=writer)
    ops_a = [op for op, _ in m[a_name]]
    ops_c = [op for op, _ in m[c_name]]
    orc = oracle.serial(tab(*rows), ops_a, ops_c)
    t = C.create_branch(t, "m", C.head_id(t, "A"))
    out = snapmerge(t, "m", "C")
    rec = {"case": {"base": base_name, "A": a_name, "C": c_name}, "true_conflict": orc["true_conflict"],
           "verdict": out.verdict, "category": out.to_json()["category"], "rule": out.rule,
           "latency_ms": round(out.latency_s * 1e3, 3), "hits": out.details.get("hits", {})}
    if out.emitted:
        t2 = out.table.refresh()
        chk = oracle.check_snapshot(t2, out.snapshot_id, ["o_ac", "o_ca"])
        st = C.load_state(t2, out.snapshot_id)
        live = Counter()
        for f in st.data_files():
            r, _ = C.live_rows(st, f)
            live.update(tuple(x[n] for n in NAMES) for x in r.select(NAMES).to_pylist())
        exp = n_star(rows, ops_a, ops_c)
        dangling = [d.path for d in st.delete_files() if d.ref_path not in st.files
                    or d.data_seq < st.files[d.ref_path].data_seq]
        rec.update({"eq_ac": chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"],
                    "eq_ca": chk["duckdb_eq_o_ca"] and chk["pyiceberg_eq_o_ca"],
                    "readers_agree": chk["readers_agree"],
                    "positions_match_formula": live == exp, "bad_delete_files": len(dangling),
                    "live_instances": sum(live.values()), "expected_instances": sum(exp.values())})
        rec["sound"] = bool(rec["eq_ac"] and rec["eq_ca"] and rec["positions_match_formula"] and not dangling)
    else:
        rec["witness"] = out.witness
    return rec


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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--work-root", default=str(ROOT / "work"))
    ap.add_argument("--time-budget-min", type=float, default=0)
    ap.add_argument("--limit", type=int, default=0, help="only the first N cases (smoke)")
    a = ap.parse_args(argv)
    rdir = Path(a.results_root) / a.campaign
    code_fp = code_fingerprint()
    cfg = {"enumerator": "physical_v1", "bases": sorted(BASES), "menu": sorted(menu()), "limit": a.limit}
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
                      source_files=[], argv=sys.argv if argv is None else list(argv))
    cases = [(b, x, y) for b in sorted(BASES) for x, y in itertools.product(sorted(menu()), repeat=2)]
    if a.limit:
        cases = cases[:a.limit]
    oracle = Oracle()
    t0 = time.time()
    new = skipped = 0
    for b, x, y in cases:
        cid = f"{b}__{x}__{y}"
        path = rdir / "raw" / f"{cid}.json"
        fp = hashlib.sha256(json.dumps([code_fp, cfg, cid]).encode()).hexdigest()[:24]
        if path.exists() and json.loads(path.read_text()).get("fingerprint") == fp:
            skipped += 1
            continue
        if a.time_budget_min and time.time() - t0 > a.time_budget_min * 60:
            break
        try:
            rec = run_case(b, x, y, Path(a.work_root) / a.campaign / "case", oracle)
        except Exception as e:
            rec = {"case": {"base": b, "A": x, "C": y}, "verdict": "error", "error": repr(e),
                   "traceback": traceback.format_exc()[-2000:]}
        rec["fingerprint"], rec["code_fingerprint"], rec["case_id"] = fp, code_fp, cid
        atomic_json(path, rec)
        new += 1
        if new % 50 == 0:
            print(f"[{new}] {cid} {rec.get('verdict')} sound={rec.get('sound')}", flush=True)
    oracle.close()
    shutil.rmtree(Path(a.work_root) / a.campaign, ignore_errors=True)
    print(f"done: new={new} skipped={skipped} total={len(cases)} pending={len(cases) - new - skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
