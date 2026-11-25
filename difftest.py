#!/usr/bin/env python3
"""Differential-test campaign runner (checkpointed, resumable).

Per scenario: generate (seeded) -> write base + both branches through the Iceberg writer ->
check each branch head against the oracle's one-sided application (writer check) -> compute the
serial-order oracle (A;C and C;A) -> run SnapMerge (and variants) and every baseline into its own
branch -> read every emitted merge snapshot through DuckDB and PyIceberg and compare with both
orders -> write one JSON per scenario atomically (tmp + os.replace).

Raw results: results/<campaign>/raw/<scenario id>.json (one per scenario), campaign.json,
environment.json (versions, source and code sha256, config, one entry per invocation), and
summary.json (--summarize).  Older campaigns used scenarios/ instead of raw/; --summarize reads both.

Resume contract: each scenario JSON carries a fingerprint over (code, campaign config incl. the
source files' sha256, scenario id, seed).  Existing JSONs with the same fingerprint are skipped.  A
campaign directory created by different code or config is never overwritten: the runner aborts and
asks for a new campaign name.
Memory guard: MEMGUARD_BATCH_SCALE scales DuckDB's memory_limit; MEMGUARD_DELAY_SECONDS adds a
pause between scenarios.  Scenario contents never depend on these variables.
Timing: before every timed policy call the in-process metadata caches are cleared
(catalog.clear_metadata_caches), so no policy profits from the one that ran before it; the 1-minute
load average is recorded with each policy record (the host is shared).

Revision options (2026-10-05): --order random runs the policies of each scenario in a seeded random
order (counterbalancing the OS page cache, which favours later policies); --provenance-check runs the
provenance-aware writer check (snapmerge.provenance) on every operation; --writer single commits
copy-on-write operations as one snapshot (foreign-writer emulation); --metadata-version 1 writes the
submitted version's properties; --n-parts/--hist/--sel/--compact-p are the generator's sensitivity
knobs (scenarios.py).  New policies: replay_intent, replay_intent_exact, rebase_occ, recompute
(snapmerge.intent) and the variant snapmerge[append_rule] (the submitted version's insert rule).

Example:
  python difftest.py --campaign pilot --source tpch_lineitem --sf 0.01 --per-mix 40 --base-rows 1000
  python difftest.py --campaign pilot --summarize
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

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

import numpy as np  # noqa: E402

from snapmerge import catalog as C  # noqa: E402
from snapmerge import provenance  # noqa: E402
from snapmerge.baselines import POLICIES as _BASE_POLICIES  # noqa: E402
from snapmerge.intent import INTENT_POLICIES  # noqa: E402
from snapmerge.envinfo import sha256_file, write_environment  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.rules import RuleConfig  # noqa: E402
from snapmerge.scenarios import MIXES, SourcePool, generate, scenario_seed  # noqa: E402
from snapmerge.sources import load_source, source_paths  # noqa: E402
from snapmerge.writer import apply_op, write_base  # noqa: E402

POLICIES = dict(_BASE_POLICIES, **INTENT_POLICIES)
BASELINES = ["replay", "lakefs", "nessie_normal", "nessie_force", "fast_forward", "cherry_pick", "rowlevel"]
NEEDS_CON = ("rowlevel", "recompute")
SNAPMERGE_VARIANTS = {
    "full": RuleConfig(),
    "no_delete_union": RuleConfig(delete_union=False),
    "no_compaction_rebase": RuleConfig(compaction_rebase=False),
    "no_bounded_reads": RuleConfig(bounded_reads=False),
    "no_net_effect": RuleConfig(net_effect=False),
    "filelevel": RuleConfig(mode="filelevel"),
    "append_rule": RuleConfig(discovery="append_rule"),
}


def atomic_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


RAW = "raw"


def code_files() -> list[Path]:
    return sorted((ROOT / "src" / "snapmerge").glob("*.py")) + [ROOT / "difftest.py"]


def code_fingerprint() -> str:
    h = hashlib.sha256()
    for p in code_files():
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def loadavg() -> float | None:
    try:
        return round(os.getloadavg()[0], 2)
    except OSError:
        return None


def scenario_fingerprint(code_fp: str, cfg: dict, sid: str, seed: int) -> str:
    blob = json.dumps({"code": code_fp, "cfg": cfg, "sid": sid, "seed": seed}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def memory_settings() -> tuple[int, float]:
    scale = float(os.environ.get("MEMGUARD_BATCH_SCALE", "1") or 1)
    delay = float(os.environ.get("MEMGUARD_DELAY_SECONDS", "0") or 0)
    return max(256, int(2048 * scale)), delay


# ============================================================================ one scenario

def _policy_record(o: Oracle, out, true_conflict: bool, check_reads: bool = True) -> dict:
    rec = out.to_json()
    if out.emitted and check_reads:
        chk = o.check_snapshot(out.table, out.snapshot_id, ["o_ac", "o_ca"])
        eq_ac = chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"]
        eq_ca = chk["duckdb_eq_o_ca"] and chk["pyiceberg_eq_o_ca"]
        rec["check"] = chk
        rec["eq_ac"], rec["eq_ca"] = eq_ac, eq_ca
        rec["readers_agree"] = chk["readers_agree"]
        rec["serial_equivalent"] = bool(eq_ac or eq_ca)
        rec["soundness_violation"] = not (eq_ac or eq_ca)
        rec["missed_conflict"] = bool(true_conflict)
        rec["rows_extra_vs_ac"] = chk["vs_o_ac"]["only_left"]
        rec["rows_lost_vs_ac"] = chk["vs_o_ac"]["only_right"]
        rec["rows_extra_vs_ca"] = chk["vs_o_ca"]["only_left"]
        rec["rows_lost_vs_ca"] = chk["vs_o_ca"]["only_right"]
        rec["outcome"] = "FN" if true_conflict else "TN"
    elif not out.emitted:
        rec["outcome"] = "TP" if true_conflict else "FP"
    return rec


def _recording_footprint(table, base_id: int) -> dict:
    """Metadata cost of recording (re-review 2026-10-07): the snapshots both branches committed after
    the base, the bytes of their summary properties and of the snapmerge.* properties among them, and
    the size of the table's current metadata file (which stores every snapshot summary)."""
    snaps = [s for s in table.metadata.snapshots if s.snapshot_id != base_id and s.sequence_number
             > table.metadata.snapshot_by_id(base_id).sequence_number]
    total = sm = 0
    for s in snaps:
        props = C.summary_props(s)
        for k, v in props.items():
            n = len(str(k)) + len(str(v))
            total += n
            if str(k).startswith("snapmerge."):
                sm += n
    try:
        meta_bytes = os.path.getsize(C.local_path(table.metadata_location))
    except OSError:
        meta_bytes = None
    return {"snapshots": len(snaps), "summary_bytes": total, "snapmerge_bytes": sm, "metadata_file_bytes": meta_bytes}


def run_scenario(sc, cfg: dict, work: Path, oracle: Oracle, crash_mid: bool = False) -> dict:
    t_start = time.perf_counter()
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    cat = C.open_catalog(work)
    spec_partition = tuple(cfg["partition"])
    table = C.create_table(cat, "sm.t", sc.base.schema, spec_partition)
    record = cfg.get("record", True)
    writer = cfg.get("writer", "split")
    mdv = cfg.get("metadata_version", 2)
    table = write_base(table, sc.base, chunks=cfg.get("base_chunks", 2), record=record)
    base_id = C.head_id(table, "main")
    table = C.create_branch(table, "A", base_id)
    table = C.create_branch(table, "C", base_id)
    t_write = time.perf_counter()
    prov = {"A": [], "C": []}
    names = sc.base.schema.names
    t_apply = t_prov = 0.0  # re-review 2026-10-07: writer time separate from the harness's provenance check
    for side in ("A", "C"):
        for i, op in enumerate(sc.ops[side]):
            before = C.head_id(table, side)
            t1 = time.perf_counter()
            table = apply_op(table, side, op, f"{side}{i}", record=record and side in cfg.get("record_sides", "AC"),
                             writer=writer, schema_version=mdv)
            t2 = time.perf_counter()
            t_apply += t2 - t1
            if cfg.get("provenance_check"):
                prov[side].append(provenance.check_op(table, before, C.head_id(table, side), op, names))
                t_prov += time.perf_counter() - t2
    t_ops = time.perf_counter()
    recording = _recording_footprint(table, base_id)
    orc = oracle.serial(sc.base, sc.ops["A"], sc.ops["C"])
    t_oracle = time.perf_counter()
    writer_check = {}
    for side, exp in (("A", "o_a"), ("C", "o_c")):
        chk = oracle.check_snapshot(table, C.head_id(table, side), [exp])
        writer_check[side] = {"ok": chk[f"duckdb_eq_{exp}"] and chk[f"pyiceberg_eq_{exp}"],
                              "readers_agree": chk["readers_agree"], "rows": chk["rows_duckdb"]}
    t_wcheck = time.perf_counter()
    true_conflict = orc["true_conflict"]
    policies = {}
    a_head = C.head_id(table, "A")
    run_order = list(cfg["policies"])
    if cfg.get("order") == "random":  # counterbalanced: a seeded permutation per scenario
        np.random.default_rng(sc.seed ^ 0x5EED5EED).shuffle(run_order)
    for order, pname in enumerate(run_order):
        branch = "m_" + pname.replace("[", "_").replace("]", "")
        table = C.create_branch(table, branch, a_head)
        C.clear_metadata_caches()  # every timed policy starts with cold in-process metadata caches
        load = loadavg()
        try:
            if pname.startswith("snapmerge"):
                variant = pname[len("snapmerge["):-1] if "[" in pname else "full"
                out = snapmerge(table, branch, "C", SNAPMERGE_VARIANTS[variant], policy=pname)
            elif pname in NEEDS_CON:
                out = POLICIES[pname](table, branch, "C", con=oracle.con)
            else:
                out = POLICIES[pname](table, branch, "C")
            if out.table is not None:
                table = out.table
            policies[pname] = _policy_record(oracle, out, true_conflict)
        except Exception as e:  # an exception is a result, not a skip
            policies[pname] = {"policy": pname, "verdict": "error", "error": repr(e),
                               "traceback": traceback.format_exc()[-2000:]}
        policies[pname]["order"] = order
        policies[pname]["loadavg_1m"] = load
        table = table.refresh()
    if crash_mid:
        os._exit(3)
    rec = {
        "scenario": sc.summary(),
        "oracle": orc,
        "writer_check": writer_check,
        "policies": policies,
        "snapshots": len(table.metadata.snapshots),
        "loadavg_1m_start": None,
        "timing_s": {"write_base": round(t_write - t_start, 3), "ops": round(t_ops - t_write, 3),
                     "apply": round(t_apply, 4), "provenance_check": round(t_prov, 4),
                     "oracle_serial": round(t_oracle - t_ops, 4), "writer_check": round(t_wcheck - t_oracle, 4),
                     "total": round(time.perf_counter() - t_start, 3)},
        "recording": recording,
    }
    if cfg.get("provenance_check"):
        rec["provenance_check"] = {s: {"ok": all(x["ok"] for x in v), "ops": v} for s, v in prov.items()}
    return rec


# ============================================================================ campaign

def scenario_list(cfg: dict) -> list[tuple[str, str, int]]:
    out = []
    for mix in cfg["mixes"]:
        for i in range(cfg["per_mix"]):
            sid = f"{cfg['source']}-{mix}-{i:05d}"
            out.append((sid, mix, scenario_seed(cfg["seed_base"], cfg["source"], mix, i)))
    k, n = cfg.get("shard", [0, 1])
    return [x for j, x in enumerate(out) if j % n == k]


def build_config(a) -> dict:
    policies = [f"snapmerge[{v}]" if v != "full" else "snapmerge" for v in a.variants.split(",") if v]
    policies += [p for p in a.policies.split(",") if p]
    from snapmerge.sources import SPECS

    spec = SPECS[a.source]
    months = a.months.split(",")
    srcs = source_paths(a.source, sf=a.sf, months=tuple(months))
    return {
        "source": a.source, "sf": a.sf, "months": months, "mixes": a.mixes.split(","),
        "per_mix": a.per_mix, "seed_base": a.seed_base, "base_rows": a.base_rows, "policies": policies,
        "record": not a.no_record, "base_chunks": a.base_chunks, "partition": list(spec.partition),
        "source_sha256": {p.name: sha256_file(p) for p in srcs if p.exists()},
        "writer": a.writer, "metadata_version": a.metadata_version, "provenance_check": a.provenance_check,
        "record_sides": a.record_sides,
        "order": a.order, "knobs": {"n_parts": a.n_parts, "hist": a.hist, "sel": a.sel, "compact_p": a.compact_p},
    }


def scenario_dir(results_dir: Path) -> Path:
    raw = results_dir / RAW
    return raw if raw.exists() or not (results_dir / "scenarios").exists() else results_dir / "scenarios"


def _ratio(a: int, b: int) -> float | None:
    return round(a / b, 4) if b else None


def _median(xs: list[float]) -> float | None:
    xs = sorted(xs)
    if not xs:
        return None
    m = len(xs) // 2
    return round(xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2, 3)


def policy_metrics(prs: list[tuple[dict, dict]]) -> dict:
    """Metrics of one policy over (scenario record, policy record) pairs.

    conflict_recall      = TP / true conflicts (errors and merges on true conflicts are misses)
    conflict_precision   = TP / reported conflicts
    auto_merge_rate      = merges / scenarios (sound or not)
    sound_auto_merge_rate_of_commuting = merges equal to a serial order on commuting scenarios /
                           commuting scenarios (Figure 1 and the day-10 gate use this one)
    rows_*_vs_closest    = per merge, lost/extra rows against the serial order it is closer to
    """
    n = len(prs)
    n_tc = sum(1 for r, _ in prs if r["oracle"]["true_conflict"])
    n_comm = n - n_tc
    v = [x.get("outcome") for _, x in prs]
    tp, fp, fn, tn = (v.count(k) for k in ("TP", "FP", "FN", "TN"))
    errs = [(r, x) for r, x in prs if x.get("verdict") == "error"]
    merged = [(r, x) for r, x in prs if x.get("verdict") == "merged"]
    unsound = [(r, x) for r, x in merged if not x.get("serial_equivalent")]
    sound_comm = sum(1 for r, x in merged if not r["oracle"]["true_conflict"] and x.get("serial_equivalent"))
    closest_lost = closest_extra = 0
    for _, x in merged:
        ac = (x.get("rows_lost_vs_ac", 0), x.get("rows_extra_vs_ac", 0))
        ca = (x.get("rows_lost_vs_ca", ac[0]), x.get("rows_extra_vs_ca", ac[1]))
        best = min(ac, ca, key=lambda t: t[0] + t[1])
        closest_lost += best[0]
        closest_extra += best[1]
    lat = [x["latency_ms"] for _, x in prs if "latency_ms" in x]
    lat_m = [x["latency_ms"] for _, x in merged if "latency_ms" in x]
    io = [x["io"] for _, x in prs if "io" in x]
    return {
        "n": n, "true_conflicts": n_tc, "commuting": n_comm, "TP": tp, "FP": fp, "FN": fn, "TN": tn,
        "errors": len(errs), "errors_on_true_conflicts": sum(1 for r, _ in errs if r["oracle"]["true_conflict"]),
        "merged": len(merged), "oracle_checks": sum(1 for _, x in merged if "check" in x),
        "auto_merge_rate": _ratio(len(merged), n),
        "auto_merge_rate_of_commuting": _ratio(tn, n_comm),
        "sound_auto_merge_rate_of_commuting": _ratio(sound_comm, n_comm),
        "conflict_recall": _ratio(tp, n_tc),
        "conflict_precision": _ratio(tp, tp + fp),
        "soundness_violations": len(unsound),
        "unsound_merges_on_commuting": sum(1 for r, _ in unsound if not r["oracle"]["true_conflict"]),
        "unsound_merges_on_true_conflicts": sum(1 for r, _ in unsound if r["oracle"]["true_conflict"]),
        "readers_disagree": sum(1 for _, x in merged if x.get("readers_agree") is False),
        "rows_lost_vs_ac": sum(x.get("rows_lost_vs_ac", 0) for _, x in merged),
        "rows_extra_vs_ac": sum(x.get("rows_extra_vs_ac", 0) for _, x in merged),
        "rows_lost_vs_ca": sum(x.get("rows_lost_vs_ca", 0) for _, x in merged),
        "rows_extra_vs_ca": sum(x.get("rows_extra_vs_ca", 0) for _, x in merged),
        "rows_lost_vs_closest": closest_lost, "rows_extra_vs_closest": closest_extra,
        "merges_with_row_loss": sum(1 for _, x in merged if min(x.get("rows_lost_vs_ac", 0),
                                                               x.get("rows_lost_vs_ca", 0)) > 0),
        "mean_latency_ms": round(sum(lat) / len(lat), 3) if lat else None,
        "median_latency_ms": _median(lat),
        "median_latency_ms_merged": _median(lat_m),
        "mean_bytes_read": round(sum(i.get("data_bytes_read", 0) + i.get("delete_bytes_read", 0) for i in io)
                                 / max(1, len(io)), 1),
        "mean_bytes_written": round(sum(i.get("data_bytes_written", 0) + i.get("delete_bytes_written", 0) for i in io)
                                    / max(1, len(io)), 1),
        "mean_metadata_bytes_written_approx": round(sum(i.get("metadata_bytes_written", 0) for i in io)
                                                    / max(1, len(io)), 1),
        "mean_manifests_written": round(sum(i.get("manifests_written", 0) for i in io) / max(1, len(io)), 3),
    }


def summarize(results_dir: Path) -> dict:
    recs = [json.loads(p.read_text()) for p in sorted(scenario_dir(results_dir).glob("*.json"))]
    out: dict = {"scenarios": len(recs), "by_policy": {}, "by_mix": {}}
    writer_bad = sum(1 for r in recs for s in ("A", "C") if not r["writer_check"][s]["ok"])
    out["writer_check_failures"] = writer_bad
    out["writer_checks"] = 2 * len(recs)
    out["true_conflicts"] = sum(r["oracle"]["true_conflict"] for r in recs)

    def agg(rs):
        pols = sorted({p for r in rs for p in r["policies"]})
        return {p: policy_metrics([(r, r["policies"][p]) for r in rs if p in r["policies"]]) for p in pols}

    out["by_policy"] = agg(recs)
    for mix in sorted({r["scenario"]["mix"] for r in recs}):
        sub = [r for r in recs if r["scenario"]["mix"] == mix]
        out["by_mix"][mix] = agg(sub)
    loads = [x.get("loadavg_1m") for r in recs for x in r["policies"].values() if x.get("loadavg_1m") is not None]
    out["loadavg_1m"] = {"min": min(loads), "median": _median(loads), "max": max(loads)} if loads else None
    tot = [r["timing_s"]["total"] for r in recs if "timing_s" in r]
    out["scenario_seconds"] = {"sum": round(sum(tot), 1), "median": _median(tot)} if tot else None
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--source", default="tpch_lineitem")
    ap.add_argument("--sf", type=float, default=0.1)
    ap.add_argument("--months", default="2023-01")
    ap.add_argument("--mixes", default=",".join(MIXES))
    ap.add_argument("--per-mix", type=int, default=10)
    ap.add_argument("--seed-base", type=int, default=20261002)
    ap.add_argument("--base-rows", type=int, default=2000)
    ap.add_argument("--base-chunks", type=int, default=2)
    ap.add_argument("--variants", default="full", help="SnapMerge variants: " + ",".join(SNAPMERGE_VARIANTS))
    ap.add_argument("--policies", default=",".join(BASELINES))
    ap.add_argument("--no-record", action="store_true", help="writer records no logical metadata (file-level fallback)")
    ap.add_argument("--writer", default="split", choices=["split", "single"],
                    help="copy-on-write encoding: PyIceberg delete+append parts, or one snapshot (foreign writer)")
    ap.add_argument("--metadata-version", type=int, default=2, choices=[1, 2])
    ap.add_argument("--record-sides", default="AC", choices=["AC", "A", "C"],
                    help="sides whose writer records metadata (partial metadata availability)")
    ap.add_argument("--provenance-check", action="store_true", help="provenance-aware writer check per operation")
    ap.add_argument("--order", default="random", choices=["fixed", "random"],
                    help="policy order per scenario (random = seeded counterbalancing)")
    ap.add_argument("--n-parts", type=int, default=4)
    ap.add_argument("--hist", type=int, default=1)
    ap.add_argument("--sel", type=float, default=1.0)
    ap.add_argument("--compact-p", type=float, default=0.0)
    ap.add_argument("--shard", default="0/1")
    ap.add_argument("--max-new", type=int, default=0, help="stop after this many new scenarios (0 = no limit)")
    ap.add_argument("--time-budget-min", type=float, default=0, help="stop starting new scenarios after this")
    ap.add_argument("--keep-work", action="store_true")
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--work-root", default=str(ROOT / "work"))
    ap.add_argument("--allow-generate", action="store_true", help="generate TPC-H in memory if not prepared")
    ap.add_argument("--summarize", action="store_true")
    a = ap.parse_args(argv)

    rdir = Path(a.results_root) / a.campaign
    if a.summarize:
        s = summarize(rdir)
        atomic_json(rdir / "summary.json", s)
        print(json.dumps(s, indent=1))
        return 0

    cfg = build_config(a)
    k, n = (int(x) for x in a.shard.split("/"))
    code_fp = code_fingerprint()
    meta_path = rdir / "campaign.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta["code_fingerprint"] != code_fp or meta["config"] != cfg:
            print(f"ABORT: {rdir} was produced by different code or config "
                  f"({meta['code_fingerprint']} vs {code_fp}); use a new --campaign name.", file=sys.stderr)
            return 2
    else:
        meta = {"campaign": a.campaign, "config": cfg, "code_fingerprint": code_fp,
                "created": time.strftime("%Y-%m-%dT%H:%M:%S"), "python": sys.version.split()[0]}
        atomic_json(meta_path, meta)
    write_environment(rdir, campaign=a.campaign, config=cfg, code_fingerprint=code_fp, code_files=code_files(),
                      source_files=source_paths(a.source, sf=a.sf, months=tuple(cfg["months"])),
                      argv=sys.argv if argv is None else list(argv))
    cfg_run = dict(cfg, shard=[k, n])
    mem_mb, delay = memory_settings()
    spec, src = load_source(a.source, sf=a.sf, months=tuple(cfg["months"]), allow_generate=a.allow_generate)
    pool = SourcePool(spec, src)
    oracle = Oracle(memory_limit_mb=mem_mb)
    crash_after = int(os.environ.get("SNAPMERGE_CRASH_AFTER", "0") or 0)
    crash_mid_at = int(os.environ.get("SNAPMERGE_CRASH_MID_AT", "0") or 0)
    done_new = skipped = 0
    t0 = time.time()
    todo = scenario_list(cfg_run)
    for sid, mix, seed in todo:
        path = rdir / RAW / f"{sid}.json"
        fp = scenario_fingerprint(code_fp, cfg, sid, seed)
        if path.exists():
            try:
                old = json.loads(path.read_text())
            except json.JSONDecodeError:
                old = None
            if old is not None and old.get("fingerprint") == fp:
                skipped += 1
                continue
            if old is not None:
                print(f"ABORT: {path} has a different fingerprint; refusing to overwrite", file=sys.stderr)
                return 2
        if a.max_new and done_new >= a.max_new:
            break
        if a.time_budget_min and (time.time() - t0) > a.time_budget_min * 60:
            break
        sc = generate(pool, mix, seed, a.base_rows, scenario_id=sid, source=a.source, **cfg["knobs"])
        work = Path(a.work_root) / a.campaign / sid
        crash_mid = bool(crash_mid_at) and (done_new + 1 == crash_mid_at)
        load0 = loadavg()
        rec = run_scenario(sc, cfg, work, oracle, crash_mid=crash_mid)
        rec["loadavg_1m_start"] = load0
        rec["fingerprint"] = fp
        rec["code_fingerprint"] = code_fp
        rec["memguard"] = {"batch_scale": os.environ.get("MEMGUARD_BATCH_SCALE"),
                           "restart": os.environ.get("MEMGUARD_RESTART"), "duckdb_memory_limit_mb": mem_mb,
                           # minor revision 2026-10-08 (M3): the process-tree guard and its invocation, set by
                           # scripts/run_revision_chain.sh (None when run outside the chain)
                           "guard_limit_gb": os.environ.get("SNAPMERGE_GUARD_LIMIT_GB"),
                           "guard_invocation": os.environ.get("SNAPMERGE_GUARD_INVOCATION")}
        atomic_json(path, rec)
        if not a.keep_work:
            shutil.rmtree(work, ignore_errors=True)
        done_new += 1
        sm = rec["policies"].get("snapmerge", {})
        print(f"[{done_new}] {sid} true_conflict={rec['oracle']['true_conflict']} "
              f"snapmerge={sm.get('verdict')}/{sm.get('outcome')} {rec['timing_s']['total']}s", flush=True)
        if crash_after and done_new >= crash_after:
            os._exit(3)
        if delay:
            time.sleep(delay)
    oracle.close()
    total = len(todo)
    have = len(list((rdir / RAW).glob("*.json"))) if (rdir / RAW).exists() else 0
    meta["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    meta["completed_files"] = have
    atomic_json(meta_path, meta)
    pending = total - skipped - done_new
    print(f"done: new={done_new} skipped={skipped} shard_total={total} files={have} pending={pending}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
