#!/usr/bin/env python3
"""Analysis stage: aggregate fingerprint-verified raw results into results/summary.json,
results/analysis.md and evaluation charts under results/figures/.

Inputs (read-only): results/<campaign>/raw/*.json (difftest, scale and counterexample runners),
results/<campaign>/campaign.json and environment.json (configuration, recorded code sha256), the
current code files and source data files, and (for resource telemetry only, labelled as such) the
memory-guard state files runs/05_snapmerge_branch_merge_*.json.  No merge is re-run.

Fingerprint gate (a record is analysed only if all hold):
  1. The campaign's recorded code fingerprint is reproduced from the CURRENT code files with the
     runner's own formula, either exactly or after removing the documented additive deltas listed in
     ADDITIVE_DELTAS (whole top-level functions added after the campaign ran).  For the equivalent
     case every per-file sha256 recorded in environment.json must match the normalised current file
     and no code file of that runner may reference the removed symbol (so the campaign's code path
     is byte-identical to the current one).
  2. Every source data file recorded for the campaign hashes (sha256, now) to the recorded value.
  3. The record's stored fingerprint equals the fingerprint recomputed with the runner's formula from
     (verified code fingerprint, campaign config, record identity), and the record identity (scenario
     id, seed, size/kind/rep/policy, case spec) is one the campaign configuration plans.
Campaigns or records failing any check are listed in summary.json["gate"] and never analysed.

Statistics: every difftest scenario has its own seed, so "across seeds" = across scenarios.  Rates
are ratios of sums; 95% intervals are percentile bootstraps over scenarios (B resamples, stratified
by mix, fixed RNG seed); comparisons against SnapMerge use the SAME resamples (paired bootstrap).
For counts at the boundary (0 of n or n of n) the exact Clopper-Pearson interval is reported too.
Scale groups have 5 repetitions (seeded scenario per rep): means with Student-t 95% intervals and
paired bootstrap intervals of geometric-mean latency ratios over reps.

Figures are drawn from results/summary.json only (re-read from disk after it is written).

Usage:
  .venv/bin/python 05_snapmerge_branch_merge/analyze.py            # gate, aggregate, write, plot
  .venv/bin/python 05_snapmerge_branch_merge/analyze.py --figures-only
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import resource
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent
RESULTS = Path(os.environ.get("SNAPMERGE_RESULTS", str(ROOT / "results")))  # override only for tests
RUNS = ROOT.parent / "runs"
FIGDIR = ROOT / "results" / "figures"
PROJECT = "05_snapmerge_branch_merge"

B_DEFAULT = 10_000
SEED_DEFAULT = 20261002

# Documented additive code deltas (plan.md ledger, 2026-10-02, stage Experiment part 2): the
# function below was added to catalog.py after the difftest and counterexample campaigns ran; it is
# called only by scale.py and tests/test_experiment_fixes.py.  The gate verifies both facts.
ADDITIVE_DELTAS = {"catalog.py": ["pyiceberg_reader_groups"]}

MIXES = ["partition_disjoint_ingest", "filter_corrections", "upsert_keys", "deletes_mor", "deletes_cow",
         "compaction_on_main", "adversarial_overlap"]
MIX_LABEL = {"partition_disjoint_ingest": "Partition-disjoint ingest", "filter_corrections": "Filter corrections",
             "upsert_keys": "Upsert by key", "deletes_mor": "Deletes (MOR)", "deletes_cow": "Deletes (COW)",
             "compaction_on_main": "Compaction on main", "adversarial_overlap": "Adversarial overlap",
             "all": "All mixes"}
POLICY_LABEL = {
    "snapmerge": "SnapMerge", "snapmerge[no_net_effect]": "SnapMerge w/o net-effect test",
    "snapmerge[no_delete_union]": "SnapMerge w/o delete union",
    "snapmerge[no_compaction_rebase]": "SnapMerge w/o compaction rebase",
    "snapmerge[no_bounded_reads]": "SnapMerge w/o bounded reads",
    "snapmerge[filelevel]": "SnapMerge file-level rules only",
    "replay": "Replay (#13959)", "cherry_pick": "Iterated cherry-pick", "rowlevel": "Row-level 3-way",
    "lakefs": "lakeFS policy", "nessie_normal": "Nessie NORMAL", "nessie_force": "Nessie FORCE",
    "fast_forward": "Fast-forward only",
    # revision (2026-10-05): append-snapshot insert rule and the information-matched controls
    "snapmerge[append_rule]": "SnapMerge, append-snapshot insert rule",
    "replay_intent": "Intent replay (pairwise)", "replay_intent_exact": "Exact replay",
    "rebase_occ": "Rebase-OCC", "recompute": "Recompute",
}
POLICY_ORDER = ["snapmerge", "snapmerge[no_net_effect]", "snapmerge[no_compaction_rebase]",
                "snapmerge[no_delete_union]", "snapmerge[no_bounded_reads]", "snapmerge[filelevel]",
                "replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "nessie_force", "fast_forward"]
ABLATIONS = ["snapmerge[no_net_effect]", "snapmerge[no_compaction_rebase]", "snapmerge[no_delete_union]",
             "snapmerge[no_bounded_reads]"]
# snapmerge[append_rule] reproduces the submitted version's insert rule (a known bug, R2); it is reported on its own and
# never pooled with the SnapMerge configurations.
REPRO_VARIANTS = {"snapmerge[append_rule]"}
# Campaigns of the 2026-10-05 revision (review package).  The library changed (insertion provenance,
# exact matching, NULL semantics, outcome categories), so every paper number comes from these new
# campaigns; the v1/v2/v3 campaigns of the submitted version stay on disk untouched and are reported by
# the gate as superseded (their recorded code no longer matches the current source).
MAIN = "difftest_main_tpch_v5"
NYC = "difftest_main_nyc_v5"  # second data source (NYC TLC yellow taxi 2023-01)
ABLATION = "ablation_v5"
FALLBACK = "fallback_filelevel_v5"
SCALE = "scale_v6_tpch"
CEX = "counterexamples_v4"
PILOT = "pilot_v2"  # not rerun in the revision (its role is covered by the smoke runs and the main campaign)
# 2026-10-08 minor revision (M1/M2 library repair: conservative commit-outcome resolution, own-snapshot check,
# staged/enforced read budget): every campaign was re-run under a new name with the same seeds and settings.
WRITER_SINGLE = "writer_single_v3"
MIXED = "mixed_metadata_v3"
ENUM = "physical_enum_v3"
WORKFLOW = "workflow_v3"
CONTENDED = "contended_v3"
CONTENDED_UNPART = "contended_unpart_v3"
CONTENDED_SF10 = "contended_sf10_v3"
HISTORY = "history_v3"
SENS_LEVELS = {  # campaign -> (knob, level); the default level is the first 60 scenarios per mix of MAIN
    "sens_parts8_v3": ("n_parts", 8), "sens_parts16_v3": ("n_parts", 16), "sens_parts32_v3": ("n_parts", 32),
    "sens_hist2_v3": ("hist", 2), "sens_hist4_v3": ("hist", 4), "sens_sel025_v3": ("sel", 0.25),
    "sens_sel4_v3": ("sel", 4.0), "sens_compact05_v3": ("compact_p", 0.5), "sens_compact1_v3": ("compact_p", 1.0)}
SENS_KEYS = ("parts8", "parts16", "parts32", "hist2", "hist4", "sel025", "sel4", "compact05", "compact1")
ROLES = {MAIN: "main", NYC: "second_source", ABLATION: "ablation", FALLBACK: "fallback", SCALE: "scale",
         CEX: "counterexamples", WRITER_SINGLE: "writer_single", MIXED: "mixed_metadata",
         CONTENDED: "contended", CONTENDED_UNPART: "contended", CONTENDED_SF10: "contended",
         HISTORY: "history", ENUM: "enumerator", WORKFLOW: "workflow",
         **{k: "sensitivity" for k in SENS_LEVELS}}
# superseded on 2026-10-07 (re-review: identity guard, phase checks, read estimate, commit outcomes, counters)
REREVIEW_SUPERSEDED = {"difftest_main_tpch_v3", "difftest_main_nyc_v3", "ablation_v3", "fallback_filelevel_v3",
                       "scale_v4_tpch", "counterexamples_v2", "writer_single_v1", "mixed_metadata_v1", "contended_v1",
                       "contended_unpart_v1", "contended_sf10_v1", "history_v1", "physical_enum_v1", "workflow_v1",
                       *{f"sens_{k}_v1" for k in SENS_KEYS}}
# superseded on 2026-10-08 (minor revision: M1/M2 library repair); these produced the reviewed paper version
MINOR_SUPERSEDED = {"difftest_main_tpch_v4", "difftest_main_nyc_v4", "ablation_v4", "fallback_filelevel_v4",
                    "scale_v5_tpch", "counterexamples_v3", "writer_single_v2", "mixed_metadata_v2", "contended_v2",
                    "contended_unpart_v2", "contended_sf10_v2", "history_v2", "physical_enum_v2", "workflow_v2",
                    *{f"sens_{k}_v2" for k in SENS_KEYS}}
_CURRENT = {"difftest_main_tpch": MAIN, "difftest_main_nyc": NYC, "ablation": ABLATION, "fallback_filelevel": FALLBACK,
            "scale": SCALE, "counterexamples": CEX, "writer_single": WRITER_SINGLE, "mixed_metadata": MIXED,
            "contended": CONTENDED, "contended_unpart": CONTENDED_UNPART, "contended_sf10": CONTENDED_SF10,
            "history": HISTORY, "physical_enum": ENUM, "workflow": WORKFLOW,
            **{f"sens_{k}": f"sens_{k}_v3" for k in SENS_KEYS}}


def _current_of(name: str) -> str:
    stem = name.rsplit("_v", 1)[0] if not name.startswith("scale_") else "scale"
    return _CURRENT[stem]


SUPERSEDED = {"difftest_main_tpch_v2": MAIN, "difftest_main_nyc_v2": NYC, "ablation_v2": ABLATION,
              "fallback_filelevel_v2": FALLBACK, "scale_v3_tpch": SCALE, "counterexamples_v1": CEX, PILOT: None,
              "scale_v2_small": SCALE,
              **{k: _current_of(k) for k in REREVIEW_SUPERSEDED | MINOR_SUPERSEDED}}
# Regression comparisons (same seeds): current campaign -> earlier campaigns (verdict + serial equivalence per policy);
# the first entry of each list is the campaign that produced the reviewed paper version
PREVIOUS = {MAIN: ["difftest_main_tpch_v4", "difftest_main_tpch_v3", "difftest_main_tpch_v2"],
            NYC: ["difftest_main_nyc_v4", "difftest_main_nyc_v3"],
            WRITER_SINGLE: ["writer_single_v2", "writer_single_v1"], ABLATION: ["ablation_v4", "ablation_v3"],
            FALLBACK: ["fallback_filelevel_v4", "fallback_filelevel_v3"], MIXED: ["mixed_metadata_v2", "mixed_metadata_v1"],
            **{f"sens_{k}_v3": [f"sens_{k}_v2", f"sens_{k}_v1"] for k in SENS_KEYS}}
RATIO_METRICS = ["conflict_recall", "conflict_precision", "sound_auto_merge_rate_of_commuting", "auto_merge_rate",
                 "false_conflict_rate_of_commuting", "unsound_share_of_merges", "mean_bytes_read",
                 "mean_bytes_written", "mean_metadata_bytes_written_approx",
                 "lograt_latency_vs_snapmerge_both_sound", "lograt_latency_vs_snapmerge_all"]
COUNT_METRICS = {  # metric -> (count key for k, count key for n) for exact intervals
    "conflict_recall": ("TP", "true_conflicts"),
    "sound_auto_merge_rate_of_commuting": ("sound_merges_on_commuting", "commuting"),
    "auto_merge_rate": ("merged", "n"),
    "false_conflict_rate_of_commuting": ("FP", "commuting"),
    "unsound_share_of_merges": ("soundness_violations", "merged"),
}


# ============================================================================ small helpers

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp{os.getpid()}")
    tmp.write_text(text)
    os.replace(tmp, path)


def rnd(x, k=4):
    if x is None:
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return round(float(x), k)


def clopper_pearson(k: int, n: int) -> list[float] | None:
    from scipy.stats import beta

    if not n:
        return None
    lo = 0.0 if k == 0 else float(beta.ppf(0.025, k, n - k + 1))
    hi = 1.0 if k == n else float(beta.ppf(0.975, k + 1, n - k))
    return [round(lo, 5), round(hi, 5)]


def t_interval(xs) -> list[float] | None:
    from scipy.stats import t as student_t

    xs = np.asarray(xs, float)
    if len(xs) < 2:
        return None
    m, s = xs.mean(), xs.std(ddof=1)
    h = float(student_t.ppf(0.975, len(xs) - 1)) * s / math.sqrt(len(xs))
    return [m - h, m + h]


def peak_rss_gb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(r / 1e9 if sys.platform == "darwin" else r * 1024 / 1e9, 3)


# ============================================================================ fingerprint gate

def strip_toplevel_function(src: str, name: str) -> str:
    """Remove a top-level `def name(...)` block and the blank lines that follow it (up to the next
    top-level statement)."""
    lines = src.splitlines(keepends=True)
    start = next((i for i, ln in enumerate(lines) if ln.startswith(f"def {name}(")), None)
    if start is None:
        return src
    end = start + 1
    while end < len(lines) and (not lines[end].strip() or lines[end][0] in " \t"):
        end += 1
    return "".join(lines[:start] + lines[end:])


def runner_code_files(kind: str) -> list[Path]:
    src = sorted((ROOT / "src" / "snapmerge").glob("*.py"))
    if kind == "difftest":
        return src + [ROOT / "difftest.py"]
    if kind == "scale":
        return src + [ROOT / "scale.py"]
    if kind == "counterexamples":
        return [ROOT / "scripts" / "minimize_counterexamples.py", ROOT / "tests" / "test_adversarial_fuzz.py",
                ROOT / "tests" / "test_adversarial_edge.py"] + src
    if kind == "enumerator":
        return src + [ROOT / "scripts" / "physical_enum.py"]
    if kind == "workflow":
        return src + [ROOT / "scripts" / "workflow.py"]
    raise ValueError(kind)


def code_fingerprint(files: list[Path], strip: dict[str, list[str]] | None = None) -> tuple[str, dict]:
    """The runners' formula: sha256 over (file name, file bytes) in order, first 16 hex digits."""
    h = hashlib.sha256()
    per_file = {}
    for p in files:
        b = p.read_bytes()
        for sym in (strip or {}).get(p.name, []):
            b = strip_toplevel_function(b.decode(), sym).encode()
        h.update(p.name.encode())
        h.update(b)
        per_file[p.name] = sha256_bytes(b)
    return h.hexdigest()[:16], per_file


def verify_code(kind: str, recorded_fp: str, env: dict | None) -> dict:
    files = runner_code_files(kind)
    cur_fp, cur_files = code_fingerprint(files)
    if cur_fp == recorded_fp:
        return {"status": "exact", "verified_code_fingerprint": recorded_fp, "current_fingerprint": cur_fp}
    norm_fp, norm_files = code_fingerprint(files, ADDITIVE_DELTAS)
    rec_files = (env or {}).get("code_sha256") or {}
    differing = sorted(n for n in set(rec_files) | set(cur_files) if rec_files.get(n) != cur_files.get(n))
    out = {"status": "mismatch", "current_fingerprint": cur_fp, "recorded_fingerprint": recorded_fp,
           "files_differing_from_record": differing if rec_files else "no per-file record (environment.json absent)"}
    if norm_fp != recorded_fp or not rec_files:
        return out
    bad = sorted(n for n in rec_files if rec_files[n] != norm_files.get(n))
    if bad or set(rec_files) != set(norm_files):
        out["normalised_files_differing"] = bad
        return out
    refs = []
    for p in files:
        txt = p.read_text()
        for f, syms in ADDITIVE_DELTAS.items():
            for sym in syms:
                if sym in (strip_toplevel_function(txt, sym) if p.name == f else txt):
                    refs.append(f"{p.name} references {sym}")
    if refs:
        out["removed_symbol_referenced"] = refs
        return out
    return {"status": "equivalent", "verified_code_fingerprint": recorded_fp, "current_fingerprint": cur_fp,
            "files_differing_from_record": differing,
            "removed_unreferenced_functions": {f: s for f, s in ADDITIVE_DELTAS.items() if f in differing},
            "note": "current code = campaign code + top-level functions not referenced by this runner's code files"}


def _relocate(path: str) -> Path:
    """Recorded source paths are absolute on the recording host; in the anonymized artifact they read
    <REPO>/... (2026-10-07). A path that does not exist is re-rooted under this checkout, so the SHA-256
    check still applies after the repository moves."""
    p = Path(path)
    if p.exists():
        return p
    for marker in ("<REPO>/", "/05_snapmerge_branch_merge/"):
        if marker in path:
            q = ROOT / path.split(marker, 1)[1]
            if q.exists():
                return q
    return p


def verify_sources(recorded: dict[str, str]) -> dict:
    out = {"files": {}, "ok": True}
    for path, sha in recorded.items():
        p = _relocate(path)
        cur = sha256_file(p) if p.exists() else None
        ok = cur == sha
        out["files"][str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)] = {
            "recorded_sha256": sha, "current_sha256": cur, "match": ok}
        out["ok"] &= ok
    return out


def scenario_seed(seed_base: int, source: str, mix: str, idx: int) -> int:  # = scenarios.scenario_seed
    return int(hashlib.sha256(f"{seed_base}|{source}|{mix}|{idx}".encode()).hexdigest()[:15], 16)


def fp_difftest(code_fp: str, cfg: dict, sid: str, seed: int) -> str:  # = difftest.scenario_fingerprint
    blob = json.dumps({"code": code_fp, "cfg": cfg, "sid": sid, "seed": seed}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def fp_scale(code_fp: str, size: str, kind: str, rep: int, policy: str) -> str:  # = scale.run
    return hashlib.sha256(f"{code_fp}|{size}|{kind}|{rep}|{policy}".encode()).hexdigest()[:24]


def fp_counterexample(code_fp: str, cfg: dict, spec: dict) -> str:  # = minimize_counterexamples.main
    return hashlib.sha256(json.dumps([code_fp, cfg, spec], sort_keys=True).encode()).hexdigest()[:24]


CASE_CFG_ORDER = {"enumerator": ["enumerator", "bases", "menu", "limit"],  # key order of the runners' cfg dicts
                  "workflow": ["workflow", "per_mix", "seed_base", "sf", "mixes", "source_sha256"]}


def fp_case(code_fp: str, cfg: dict, case_id: str, kind: str) -> str:  # = physical_enum.main, workflow.main
    """The runners hash json.dumps([code_fp, cfg, id]) WITHOUT sort_keys, so the config's keys must be in
    the runner's insertion order (campaign.json stores them sorted)."""
    order = CASE_CFG_ORDER[kind]
    ordered = {k: cfg[k] for k in order if k in cfg} | {k: v for k, v in cfg.items() if k not in order}
    return hashlib.sha256(json.dumps([code_fp, ordered, case_id]).encode()).hexdigest()[:24]


def campaign_kind(rdir: Path) -> tuple[str | None, dict | None, dict | None]:
    meta = json.loads((rdir / "campaign.json").read_text()) if (rdir / "campaign.json").exists() else None
    env = json.loads((rdir / "environment.json").read_text()) if (rdir / "environment.json").exists() else None
    cfg = (meta or {}).get("config") or (env or {}).get("config") or {}
    if meta and "code_fingerprint" in meta and "enumerator" in cfg:
        return "enumerator", meta, env
    if meta and "code_fingerprint" in meta and "workflow" in cfg:
        return "workflow", meta, env
    if meta and "code_fingerprint" in meta and "mixes" in cfg:
        return "difftest", meta, env
    if meta and "code_fingerprint" in meta and "sizes" in cfg:
        return "scale", meta, env
    if env and "fuzz_unsound" in cfg:
        return "counterexamples", meta, env
    return None, meta, env


def raw_dir(rdir: Path) -> Path:
    for name in ("raw", "scenarios", "records", "seeds"):
        if (rdir / name).exists():
            return rdir / name
    return rdir / "raw"


def gate_campaign(rdir: Path) -> dict:
    kind, meta, env = campaign_kind(rdir)
    rd = raw_dir(rdir)
    files = sorted(rd.glob("*.json")) if rd.exists() else []
    g = {"campaign": rdir.name, "runner": kind, "raw_files": len(files), "accepted": [], "rejected": []}
    if kind is None:
        g.update(status="not read", reason="no fingerprint scheme recognised (no campaign/environment config "
                                           "of the difftest, scale or counterexample runners)")
        return g
    recorded_fp = (meta or {}).get("code_fingerprint") or (env or {}).get("code_fingerprint")
    g["recorded_code_fingerprint"] = recorded_fp
    g["code"] = verify_code(kind, recorded_fp, env)
    srcs = {k: v["sha256"] for k, v in ((env or {}).get("source_files") or {}).items()}
    g["sources"] = verify_sources(srcs)
    if kind == "difftest":
        cfg_names = (meta["config"].get("source_sha256") or {})
        by_name = {Path(k).name: v for k, v in srcs.items()}
        g["sources"]["config_names_match"] = all(by_name.get(n) == s for n, s in cfg_names.items()) and bool(cfg_names)
        g["sources"]["ok"] &= g["sources"]["config_names_match"]
    if g["code"]["status"] == "mismatch" or not g["sources"]["ok"]:
        g.update(status="rejected", reason="code fingerprint not reproducible from the current source"
                 if g["code"]["status"] == "mismatch" else "source data sha256 changed")
        if rdir.name in SUPERSEDED:
            g["superseded_by"] = SUPERSEDED[rdir.name]
            g["reason"] = ("superseded by the 2026-10-08 minor-revision campaigns (M1/M2 library repair)"
                           if rdir.name in MINOR_SUPERSEDED else
                           "superseded by the 2026-10-07 re-review campaigns (the library changed again)"
                           if rdir.name in REREVIEW_SUPERSEDED else
                           "superseded by the 2026-10-05 revision campaigns (the library changed)")
        g["rejected"] = [{"file": f.name, "reason": "campaign rejected"} for f in files]
        return g
    code_fp = g["code"]["verified_code_fingerprint"]
    cfg = (meta or {}).get("config") or env["config"]
    planned = set()
    if kind == "difftest":
        plan = {}
        for mix in cfg["mixes"]:
            for i in range(cfg["per_mix"]):
                sid = f"{cfg['source']}-{mix}-{i:05d}"
                plan[sid] = scenario_seed(cfg["seed_base"], cfg["source"], mix, i)
        planned = set(plan)
        pols = set(cfg["policies"])
    elif kind == "scale":
        reps_policy = (cfg.get("revision") or {}).get("reps_policy") or {}
        for size in cfg["sizes"].split(","):
            for k in cfg["kinds"].split(","):
                for p in cfg["policies"].split(","):
                    for rep in range(reps_policy.get(p, cfg["reps"])):
                        planned.add((size, k, rep, p))
    elif kind == "enumerator":
        import itertools
        menu = cfg["menu"]
        planned = {f"{b}__{x}__{y}" for b in cfg["bases"] for x, y in itertools.product(menu, repeat=2)}
        if cfg.get("limit"):
            planned = set(sorted(planned)[:cfg["limit"]])
    elif kind == "workflow":
        planned = {"trace"} | {f"{m}-{i:04d}" for m in cfg["mixes"] for i in range(cfg["per_mix"])}
    seen = set()
    for f in files:
        try:
            r = json.loads(f.read_text())
        except json.JSONDecodeError:
            g["rejected"].append({"file": f.name, "reason": "unparseable JSON"})
            continue
        why = None
        if kind == "difftest":
            sid, seed = r["scenario"]["scenario_id"], r["scenario"]["seed"]
            if f.stem != sid or sid not in plan or plan[sid] != seed:
                why = "scenario id/seed not planned by the campaign config"
            elif r.get("fingerprint") != fp_difftest(code_fp, cfg, sid, seed):
                why = "fingerprint mismatch"
            elif r.get("code_fingerprint") != code_fp:
                why = "record code fingerprint differs"
            elif set(r["policies"]) != pols:
                why = "policy set differs from config"
            key = sid
        elif kind == "scale":
            key = (r["size"], r["kind"], r["rep"], r["policy"])
            stag = r["size"].replace(":", "_")
            if f.name != f"{stag}__{r['kind']}__r{r['rep']}__{r['policy']}.json" or key not in planned:
                why = "record identity not planned by the campaign config"
            elif r.get("fingerprint") != fp_scale(code_fp, *key):
                why = "fingerprint mismatch"
        elif kind in ("enumerator", "workflow"):
            key = r.get("case_id") if kind == "enumerator" else r.get("id")
            if f.stem != key or key not in planned:
                why = "record identity not planned by the campaign config"
            elif r.get("fingerprint") != fp_case(code_fp, cfg, key, kind):
                why = "fingerprint mismatch"
            elif r.get("code_fingerprint") != code_fp:
                why = "record code fingerprint differs"
        else:
            key = r["case"]["name"]
            if f.stem != key:
                why = "file name differs from case name"
            elif r.get("fingerprint") != fp_counterexample(code_fp, cfg, r["case"]):
                why = "fingerprint mismatch"
            elif r.get("code_fingerprint") != code_fp:
                why = "record code fingerprint differs"
        if why:
            g["rejected"].append({"file": f.name, "reason": why})
        else:
            g["accepted"].append(f)
            seen.add(key)
    g["planned"] = len(planned) if planned else None
    g["missing_planned"] = len(planned - seen) if planned else None
    g["status"] = "accepted" if g["accepted"] and not g["rejected"] else ("partial" if g["accepted"] else "rejected")
    return g


def run_gate() -> dict[str, dict]:
    out = {}
    for rdir in sorted(p for p in RESULTS.iterdir() if p.is_dir()):
        if rdir.name in ("experiment_logs",) or rdir.name.startswith("previous_"):  # previous_*: archived analysis outputs
            continue
        out[rdir.name] = gate_campaign(rdir)
    return out


def gate_json(g: dict) -> dict:
    d = {k: v for k, v in g.items() if k not in ("accepted", "rejected")}
    d["accepted_records"] = len(g["accepted"])
    d["rejected_records"] = len(g["rejected"])
    d["rejected_reasons"] = dict(Counter(x["reason"] for x in g["rejected"]))
    d["rejected_examples"] = g["rejected"][:5]
    return d


# ============================================================================ bootstrap engine

def weight_chunks(strata: np.ndarray, B: int, seed: int, chunk: int = 500):
    """Multinomial resampling weights, stratified (each stratum resampled to its own size)."""
    rng = np.random.default_rng(seed)
    groups = [np.flatnonzero(strata == s) for s in sorted(set(strata.tolist()))]
    n = len(strata)
    done = 0
    while done < B:
        c = min(chunk, B - done)
        W = np.zeros((c, n))
        for idx in groups:
            W[:, idx] = rng.multinomial(len(idx), np.full(len(idx), 1.0 / len(idx)), size=c)
        yield W
        done += c


def weighted_medians(W: np.ndarray, idx: np.ndarray, vals: np.ndarray) -> np.ndarray:
    order = np.argsort(vals, kind="stable")
    v = vals[order]
    cs = np.cumsum(W[:, idx[order]], axis=1)
    tot = cs[:, -1:]
    k = np.argmax(cs >= tot / 2.0, axis=1)
    out = v[k].astype(float)
    out[tot[:, 0] == 0] = np.nan
    return out


def bootstrap(strata, NUM, DEN, med_specs, B, seed):
    Rs, Ms = [], []
    for W in weight_chunks(strata, B, seed):
        with np.errstate(divide="ignore", invalid="ignore"):
            Rs.append(((W @ NUM) / (W @ DEN)).astype(np.float32))
        if med_specs:
            Ms.append(np.column_stack([weighted_medians(W, idx, v) if len(idx) else np.full(len(W), np.nan)
                                       for idx, v in med_specs]).astype(np.float32))
    R = np.vstack(Rs)
    M = np.vstack(Ms) if Ms else None
    return R, M


def qci(x: np.ndarray) -> list | None:
    x = x[~np.isnan(x)]
    if not len(x):
        return None
    lo, hi = np.quantile(x, [0.025, 0.975])
    return [float(lo), float(hi)]


# ============================================================================ difftest campaigns

def _pol_fields(x: dict) -> dict:
    v = x.get("verdict")
    io = x.get("io") or {}
    merged = v == "merged"
    se = bool(x.get("serial_equivalent")) if merged else False
    return {
        "verdict": v, "merged": merged, "conflict": v == "conflict", "error": v == "error",
        "se": se, "checked": merged and ("check" in x), "readers_agree": x.get("readers_agree"),
        "lost_ac": x.get("rows_lost_vs_ac", 0), "extra_ac": x.get("rows_extra_vs_ac", 0),
        "lost_ca": x.get("rows_lost_vs_ca", x.get("rows_lost_vs_ac", 0)),
        "extra_ca": x.get("rows_extra_vs_ca", x.get("rows_extra_vs_ac", 0)),
        "lost_ca_raw": x.get("rows_lost_vs_ca", 0),
        "latency": x.get("latency_ms"), "has_io": bool(io),
        "bytes_read": io.get("data_bytes_read", 0) + io.get("delete_bytes_read", 0),
        "bytes_written": io.get("data_bytes_written", 0) + io.get("delete_bytes_written", 0),
        "meta_read": io.get("metadata_bytes_read", 0), "meta_written": io.get("metadata_bytes_written", 0),
        "manifests_written": io.get("manifests_written", 0), "rule": x.get("rule"),
        "load": x.get("loadavg_1m"), "order": x.get("order"),
    }


def load_difftest(files: list[Path]) -> dict:
    scen, pols = [], {}
    for i, f in enumerate(files):
        r = json.loads(f.read_text())
        wc = r["writer_check"]
        scen.append({"sid": r["scenario"]["scenario_id"], "mix": r["scenario"]["mix"], "seed": r["scenario"]["seed"],
                     "tc": bool(r["oracle"]["true_conflict"]),
                     "writer_ok": [bool(wc[s]["ok"]) for s in ("A", "C")],
                     "writer_readers_agree": [bool(wc[s].get("readers_agree")) for s in ("A", "C")],
                     "total_s": (r.get("timing_s") or {}).get("total"), "load0": r.get("loadavg_1m_start"),
                     "batch_scale": (r.get("memguard") or {}).get("batch_scale")})
        for p, x in r["policies"].items():
            pols.setdefault(p, {})[i] = _pol_fields(x)
    return {"scen": scen, "pols": pols}


def difftest_counts(sc: list[dict], pr: dict, idx: list[int]) -> dict:
    """Counts of one policy over scenario indices idx (definitions match difftest.policy_metrics)."""
    c = Counter()
    lost_c = extra_c = 0
    lat, lat_m = [], []
    rules = Counter()
    for i in idx:
        x = pr.get(i)
        tc = sc[i]["tc"]
        c["n"] += 1
        c["true_conflicts"] += tc
        if x is None:
            c["absent"] += 1
            continue
        if x["error"]:
            c["errors"] += 1
            c["errors_on_true_conflicts"] += tc
        elif x["conflict"]:
            c["TP" if tc else "FP"] += 1
            if x["rule"]:
                rules[("true conflict: " if tc else "commuting: ") + str(x["rule"])] += 1
        elif x["merged"]:
            c["FN" if tc else "TN"] += 1
            c["merged"] += 1
            c["oracle_checks"] += x["checked"]
            c["oracle_check_serial_equivalent"] += x["checked"] and x["se"]
            if not x["se"]:
                c["soundness_violations"] += 1
                c["unsound_merges_on_true_conflicts" if tc else "unsound_merges_on_commuting"] += 1
            elif not tc:
                c["sound_merges_on_commuting"] += 1
            c["readers_disagree"] += x["readers_agree"] is False
            ac, ca = (x["lost_ac"], x["extra_ac"]), (x["lost_ca"], x["extra_ca"])
            best = min(ac, ca, key=lambda t: t[0] + t[1])
            lost_c += best[0]
            extra_c += best[1]
            c["merges_with_row_loss"] += min(x["lost_ac"], x["lost_ca_raw"]) > 0
            c["rows_lost_vs_ac"] += x["lost_ac"]
            c["rows_extra_vs_ac"] += x["extra_ac"]
            if x["latency"] is not None:
                lat_m.append(x["latency"])
        if x["latency"] is not None:
            lat.append(x["latency"])
    c["commuting"] = c["n"] - c["true_conflicts"]
    out = {k: int(c.get(k, 0)) for k in ("n", "true_conflicts", "commuting", "TP", "FP", "FN", "TN", "errors",
                                         "errors_on_true_conflicts", "merged", "oracle_checks",
                                         "oracle_check_serial_equivalent", "soundness_violations",
                                         "unsound_merges_on_commuting", "unsound_merges_on_true_conflicts",
                                         "sound_merges_on_commuting", "readers_disagree", "merges_with_row_loss",
                                         "rows_lost_vs_ac", "rows_extra_vs_ac", "absent")}
    out["rows_lost_vs_closest"] = int(lost_c)
    out["rows_extra_vs_closest"] = int(extra_c)
    out["median_latency_ms"] = rnd(float(np.median(lat)), 3) if lat else None
    out["median_latency_ms_merged"] = rnd(float(np.median(lat_m)), 3) if lat_m else None
    out["mean_latency_ms"] = rnd(float(np.mean(lat)), 3) if lat else None
    out["conflict_rules"] = dict(rules.most_common(12))
    return out


def analyse_difftest(name: str, files: list[Path], B: int, seed: int) -> dict:
    data = load_difftest(files)
    sc, pols = data["scen"], data["pols"]
    n = len(sc)
    mix = np.array([s["mix"] for s in sc])
    tc = np.array([s["tc"] for s in sc], float)
    comm = 1.0 - tc
    groups = ["all"] + [m for m in MIXES if (mix == m).any()]
    gmask = {g: (np.ones(n) if g == "all" else (mix == g).astype(float)) for g in groups}
    policies = [p for p in POLICY_ORDER if p in pols] + sorted(p for p in pols if p not in POLICY_ORDER)

    def arr(p, key, default=0.0):
        pr = pols[p]
        return np.array([float(pr[i][key]) if i in pr and pr[i][key] is not None else default for i in range(n)])

    A = {}
    for p in policies:
        pres = np.array([1.0 if i in pols[p] else 0.0 for i in range(n)])
        merged, conflict = arr(p, "merged"), arr(p, "conflict")
        se = arr(p, "se")
        A[p] = {"pres": pres, "merged": merged, "TP": conflict * tc, "FP": conflict * comm,
                "sound_comm": merged * se * comm, "unsound": merged * (1 - se),
                "lat": arr(p, "latency", np.nan), "br": arr(p, "bytes_read"), "bw": arr(p, "bytes_written"),
                "mw": arr(p, "meta_written"), "io": arr(p, "has_io")}
    snap = "snapmerge" if "snapmerge" in A else None
    cols, NUMc, DENc = [], [], []

    def add(p, g, m, num, den):
        cols.append((p, g, m))
        NUMc.append(num * gmask[g])
        DENc.append(den * gmask[g])

    for p in policies:
        a = A[p]
        for g in groups:
            add(p, g, "conflict_recall", a["TP"], tc)  # absent or erroring policies count as misses
            add(p, g, "conflict_precision", a["TP"], a["TP"] + a["FP"])
            add(p, g, "sound_auto_merge_rate_of_commuting", a["sound_comm"], comm)
            add(p, g, "auto_merge_rate", a["merged"], np.ones(n))
            add(p, g, "false_conflict_rate_of_commuting", a["FP"], comm)
            add(p, g, "unsound_share_of_merges", a["unsound"], a["merged"])
            add(p, g, "mean_bytes_read", a["br"] * a["io"], a["io"])
            add(p, g, "mean_bytes_written", a["bw"] * a["io"], a["io"])
            add(p, g, "mean_metadata_bytes_written_approx", a["mw"] * a["io"], a["io"])
            if snap and p != snap:
                s = A[snap]
                ok_all = (~np.isnan(a["lat"])) & (~np.isnan(s["lat"])) & (a["lat"] > 0) & (s["lat"] > 0)
                lr = np.where(ok_all, np.log(np.where(ok_all, a["lat"], 1) / np.where(ok_all, s["lat"], 1)), 0.0)
                both = ok_all & (a["sound_comm"] > 0) & (s["sound_comm"] > 0)
                add(p, g, "lograt_latency_vs_snapmerge_both_sound", lr * both, both.astype(float))
                add(p, g, "lograt_latency_vs_snapmerge_all", lr * ok_all, ok_all.astype(float))
    NUM = np.column_stack(NUMc)
    DEN = np.column_stack(DENc)
    med_specs, med_keys = [], []
    for p in policies:
        lat = A[p]["lat"]
        for g in groups:
            for sub in ("all", "merged"):
                m = (gmask[g] > 0) & ~np.isnan(lat) & ((A[p]["merged"] > 0) if sub == "merged" else True)
                idx = np.flatnonzero(m)
                med_specs.append((idx, lat[idx]))
                med_keys.append((p, g, sub))
    cseed = seed ^ int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)
    R, M = bootstrap(mix, NUM, DEN, med_specs, B, cseed)
    with np.errstate(divide="ignore", invalid="ignore"):
        point = NUM.sum(0) / DEN.sum(0)
    colidx = {c: j for j, c in enumerate(cols)}

    out_pols = {}
    for p in policies:
        out_pols[p] = {"label": POLICY_LABEL.get(p, p), "groups": {}}
        for g in groups:
            idx = [i for i in range(n) if g == "all" or sc[i]["mix"] == g]
            cnt = difftest_counts(sc, pols[p], idx)
            met = {}
            for m in RATIO_METRICS:
                j = colidx.get((p, g, m))
                if j is None:
                    continue
                val, ci = point[j], qci(R[:, j].astype(float))
                if m.startswith("lograt"):
                    e = {"geomean_ratio": rnd(math.exp(val), 4) if not np.isnan(val) else None,
                         "ci95": [rnd(math.exp(ci[0]), 4), rnd(math.exp(ci[1]), 4)] if ci else None,
                         "pairs": int(DEN[:, j].sum())}
                else:
                    k = 6 if m.startswith("mean_") else 4
                    e = {"value": rnd(val, k), "ci95": [rnd(ci[0], k), rnd(ci[1], k)] if ci else None}
                    if m in COUNT_METRICS:
                        kk, nn = COUNT_METRICS[m]
                        e["k"], e["n"] = cnt[kk], cnt[nn]
                        if cnt[kk] in (0, cnt[nn]):
                            e["ci95_exact"] = clopper_pearson(cnt[kk], cnt[nn])
                    if m == "conflict_recall":
                        e["k"], e["n"] = cnt["TP"], cnt["true_conflicts"]
                        if cnt["TP"] in (0, cnt["true_conflicts"]):
                            e["ci95_exact"] = clopper_pearson(cnt["TP"], cnt["true_conflicts"])
                    if m == "conflict_precision":
                        e["k"], e["n"] = cnt["TP"], cnt["TP"] + cnt["FP"]
                met[m] = e
            for sub in ("all", "merged"):
                j = med_keys.index((p, g, sub))
                idxs, vals = med_specs[j]
                met[f"median_latency_ms_{sub}"] = {
                    "value": rnd(float(np.median(vals)), 3) if len(vals) else None,
                    "ci95": [rnd(c, 3) for c in qci(M[:, j].astype(float))] if len(vals) else None,
                    "n": int(len(vals))}
            out_pols[p]["groups"][g] = {"counts": cnt, "metrics": met}

    comparisons = {}
    if snap:
        for p in policies:
            if p == snap:
                continue
            comparisons[p] = {}
            for g in groups:
                e = {}
                for m in ("sound_auto_merge_rate_of_commuting", "conflict_recall", "conflict_precision",
                          "false_conflict_rate_of_commuting"):
                    js, jp = colidx[(snap, g, m)], colidx[(p, g, m)]
                    d = (R[:, js].astype(float) - R[:, jp].astype(float))
                    with np.errstate(invalid="ignore"):
                        pt = point[js] - point[jp]
                    ci = qci(d)
                    e[f"snapmerge_minus_policy_{m}"] = {
                        "value": rnd(pt), "ci95": [rnd(ci[0]), rnd(ci[1])] if ci else None}
                for m in ("lograt_latency_vs_snapmerge_both_sound", "lograt_latency_vs_snapmerge_all"):
                    e[m.replace("lograt_", "geomean_ratio_policy_over_snapmerge_")] = out_pols[p]["groups"][g]["metrics"][m]
                comparisons[p][g] = e

    # writer checks, oracle checks, load
    wc_total = 2 * n
    wc_fail = sum(1 for s in sc for ok in s["writer_ok"] if not ok)
    wc_disagree = sum(1 for s in sc for ok in s["writer_readers_agree"] if not ok)
    merges_checked = sum(out_pols[p]["groups"]["all"]["counts"]["oracle_checks"] for p in policies)
    merges_total = sum(out_pols[p]["groups"]["all"]["counts"]["merged"] for p in policies)
    loads = [x["load"] for p in policies for x in pols[p].values() if x["load"] is not None]
    meta = json.loads((RESULTS / name / "campaign.json").read_text())
    tc_by_mix = {g: int(tc[mix == g].sum()) for g in groups if g != "all"}
    return {
        "role": ROLES.get(name), "runner": "difftest", "config": meta["config"],
        "code_fingerprint": meta["code_fingerprint"], "scenarios": n, "true_conflicts": int(tc.sum()),
        "true_conflicts_by_mix": tc_by_mix, "scenarios_by_mix": {g: int((mix == g).sum()) for g in groups if g != "all"},
        "oracle_checks": {
            "writer_checks": wc_total, "writer_check_failures": wc_fail, "writer_check_reader_disagreements": wc_disagree,
            "emitted_merges_all_policies": merges_total,
            "emitted_merges_compared_with_both_orders_in_both_readers": merges_checked,
            "oracle_definition": "DuckDB applies the logical operations to the base rows in both orders; a merge is "
                                 "serial-equivalent iff DuckDB iceberg_scan AND PyIceberg rows equal (A;C) or (C;A) "
                                 "under EXCEPT ALL in both directions"},
        "loadavg_1m": {"min": rnd(min(loads), 2), "median": rnd(float(np.median(loads)), 2), "max": rnd(max(loads), 2)}
        if loads else None,
        "scenario_seconds_sum": rnd(sum(s["total_s"] or 0 for s in sc), 1),
        "memguard_batch_scales": sorted({str(s["batch_scale"]) for s in sc}),
        "policies": out_pols, "comparisons_vs_snapmerge": comparisons, "groups": groups,
        "bootstrap": {"B": B, "seed": cseed, "strata": "mix", "resample_unit": "scenario (one seed each)"},
        "_cross_check_inputs": (sc, pols, policies),
    }


def cross_check_with_runner(name: str, sc, pols, policies, out_pols) -> dict:
    """Recompute counts with the runner's own difftest.policy_metrics on the same verified records."""
    try:
        sys.path.insert(0, str(ROOT))
        sys.path.insert(0, str(ROOT / "src"))
        import difftest as D
    except Exception as e:  # pragma: no cover
        return {"status": "skipped", "reason": repr(e)}
    keys = ["n", "true_conflicts", "TP", "FP", "FN", "TN", "errors", "merged", "oracle_checks",
            "soundness_violations", "unsound_merges_on_commuting", "unsound_merges_on_true_conflicts",
            "readers_disagree", "rows_lost_vs_closest", "rows_extra_vs_closest", "merges_with_row_loss",
            "median_latency_ms"]
    mism = []
    for p in policies:
        prs = []
        for i, s in enumerate(sc):
            x = pols[p].get(i)
            if x is None:
                continue
            rec = {"verdict": x["verdict"]}
            if x["latency"] is not None:
                rec["latency_ms"] = x["latency"]
            if x["verdict"] == "merged":
                rec.update({"serial_equivalent": x["se"], "readers_agree": x["readers_agree"],
                            "rows_lost_vs_ac": x["lost_ac"], "rows_extra_vs_ac": x["extra_ac"],
                            "rows_lost_vs_ca": x["lost_ca_raw"], "rows_extra_vs_ca": x["extra_ca"],
                            "outcome": "FN" if s["tc"] else "TN"})
                if x["checked"]:
                    rec["check"] = True
            elif x["verdict"] == "conflict":
                rec["outcome"] = "TP" if s["tc"] else "FP"
            prs.append(({"oracle": {"true_conflict": s["tc"]}}, rec))
        ref = D.policy_metrics(prs)
        mine = out_pols[p]["groups"]["all"]["counts"]
        for k in keys:
            if ref.get(k) != mine.get(k):
                mism.append({"policy": p, "key": k, "runner": ref.get(k), "analyze": mine.get(k)})
        ours = out_pols[p]["groups"]["all"]["metrics"]["sound_auto_merge_rate_of_commuting"]["value"]
        if ref.get("sound_auto_merge_rate_of_commuting") != (round(ours, 4) if ours is not None else None):
            mism.append({"policy": p, "key": "sound_auto_merge_rate_of_commuting",
                         "runner": ref.get("sound_auto_merge_rate_of_commuting"), "analyze": ours})
    return {"status": "ok" if not mism else "MISMATCH", "compared_keys": keys + ["sound_auto_merge_rate_of_commuting"],
            "policies": len(policies), "mismatches": mism}


# ============================================================================ scale campaign

SF_ROWS = {}


def _size_key(size: str) -> tuple:
    kind, val = size.split(":")
    return (float(val), kind)


def _kind_key(kind: str) -> tuple:
    if "-" in kind:
        base, v = kind.split("-", 1)
        return (base, 10**9 if v == "all" else int(v))
    return (kind, 0)


def analyse_scale(name: str, files: list[Path], B: int, seed: int) -> dict:
    """Merge cost per (size, kind, policy).  Revision runners (2026-10-05) add isolated per-process
    measurements (--isolate: baseline/peak RSS, policy RSS increment, peak DuckDB temp bytes), the
    preflight composition estimate, per-phase timings, and per-policy repetition counts."""
    recs = [json.loads(f.read_text()) for f in files]
    meta = json.loads((RESULTS / name / "campaign.json").read_text())
    groups: dict = {}
    for r in recs:
        groups.setdefault((r["size"], r["kind"], r["policy"]), []).append(r)
    rng_seed = seed ^ int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)
    rows = []

    def stats(xs):
        xs = np.asarray([x for x in xs if x is not None], float)
        if not len(xs):
            return None
        ci_t = t_interval(xs)
        return {"n": int(len(xs)), "mean": rnd(xs.mean(), 3), "ci95_t": [rnd(ci_t[0], 3), rnd(ci_t[1], 3)] if ci_t else None,
                "sd": rnd(xs.std(ddof=1), 3) if len(xs) > 1 else None, "median": rnd(float(np.median(xs)), 3),
                "min": rnd(xs.min(), 3), "max": rnd(xs.max(), 3)}

    def iso(r, k):
        return (r.get("isolated") or {}).get(k)

    for (size, kind, pol), rs in sorted(groups.items(), key=lambda kv: (_size_key(kv[0][0]), _kind_key(kv[0][1]), kv[0][2])):
        rs = sorted(rs, key=lambda r: r["rep"])
        io = [r.get("io") or {} for r in rs]
        chk = [r["check"] for r in rs if "check" in r]
        mem = [max((r.get(k) or {}).get("peak_rss_gb", 0) for k in ("mem_after_oracle", "mem_after_policy", "mem_after_check"))
               for r in rs]
        est = [((r.get("details") or {}).get("estimate") or {}) for r in rs]
        tim = [((r.get("details") or {}).get("timing_ms") or {}) for r in rs]
        rows.append({
            "size": size, "sf": float(size.split(":")[1]), "partitioned": not size.startswith("tpch-unpart"),
            "kind": kind, "policy": pol, "reps": len(rs),
            "verdicts": dict(Counter(r.get("verdict") for r in rs)),
            "categories": dict(Counter(r.get("category") for r in rs)),
            "true_conflict": sorted({bool(r.get("true_conflict")) for r in rs}),
            "oracle_checks": len(chk), "oracle_checks_serial_equivalent": sum(1 for c in chk if c["serial_equivalent"]),
            "oracle_checks_both_orders": sum(1 for c in chk if c["eq_ac"] and c["eq_ca"]),
            "oracle_checks_readers_agree": sum(1 for c in chk if c["readers_agree"]),
            "oracle_comparisons_derived": sum(len(c.get("derived", [])) for c in chk),
            "checks_skipped": sum(1 for r in rs if r.get("check_skipped")),
            "errors": sum(1 for r in rs if r.get("verdict") == "error"),
            "latency_ms": stats([r.get("latency_ms") for r in rs]),
            "latency_ms_by_rep": [r.get("latency_ms") for r in rs],
            "order_by_rep": [r.get("order") for r in rs],
            "bytes_read_data_delete": stats([i.get("data_bytes_read", 0) + i.get("delete_bytes_read", 0) for i in io]),
            "bytes_read_metadata": stats([i.get("metadata_bytes_read", 0) for i in io]),
            "bytes_read_total": stats([i.get("data_bytes_read", 0) + i.get("delete_bytes_read", 0)
                                       + i.get("metadata_bytes_read", 0) for i in io]),
            "bytes_written_data_delete": stats([i.get("data_bytes_written", 0) + i.get("delete_bytes_written", 0)
                                                for i in io]),
            "bytes_written_metadata_approx": stats([i.get("metadata_bytes_written", 0) for i in io]),
            "manifests_written": stats([i.get("manifests_written", 0) for i in io]),
            "check_s": stats([r.get("check_s") for r in rs]),
            "loadavg_1m": stats([r.get("loadavg_1m") for r in rs]),
            "base_rows": rs[0]["base"]["rows"], "base_data_files": rs[0]["base"]["data_files"],
            "process_peak_rss_gb_max": rnd(max(mem), 3) if mem else None,
            "isolated_peak_rss_gb": stats([iso(r, "peak_rss_gb") for r in rs]),
            "isolated_baseline_rss_gb": stats([iso(r, "baseline_rss_gb") for r in rs]),
            "isolated_policy_rss_increment_gb": stats([iso(r, "policy_rss_increment_gb") for r in rs]),
            "isolated_peak_temp_bytes": stats([iso(r, "peak_temp_bytes") for r in rs]),
            "child_wall_s": stats([r.get("child_wall_s") for r in rs]),
            "estimate_read_bytes": stats([e.get("read_bytes") for e in est]),
            "estimate_read_rows": stats([e.get("read_rows") for e in est]),
            "estimate_contended_partitions": stats([e.get("contended_partitions") for e in est]),
            "timing_ms": {k: stats([t.get(k) for t in tim]) for k in ("lineage", "logical", "compose", "reconcile", "emit")
                          if any(t.get(k) is not None for t in tim)},
            # 2026-10-08: latency outside the timed phases.  For SnapMerge this is essentially the preflight read
            # estimate (rules.read_estimate runs in decide() before the logical timer starts), which re-evaluates
            # the bounds of every insert-role file against the other side's predicates.
            "untimed_ms": stats([r["latency_ms"] - sum(x for x in t.values() if isinstance(x, (int, float)))
                                 for r, t in zip(rs, tim) if t and r.get("latency_ms") is not None]),
            "replay_rule": sorted({str(r.get("rule")) for r in rs if r.get("verdict") == "conflict"}) or None,
        })
    # paired comparisons by rep (same seeded scenario for every policy of a (size, kind, rep))
    paired = []
    by = {(r["size"], r["kind"], r["policy"], r["rep"]): r for r in recs}
    sizes = sorted({r["size"] for r in recs}, key=_size_key)
    kinds = sorted({r["kind"] for r in recs}, key=_kind_key)
    others = sorted({r["policy"] for r in recs} - {"snapmerge"})
    rng = np.random.default_rng(rng_seed)
    for size in sizes:
        for kind in kinds:
            for other in others:
                reps = sorted(rp for (s, k, p, rp) in by if s == size and k == kind and p == "snapmerge"
                              and (s, k, other, rp) in by)
                if not reps:
                    continue
                pairs = [(by[(size, kind, "snapmerge", rp)], by[(size, kind, other, rp)]) for rp in reps]
                pairs = [(a, b) for a, b in pairs if a.get("latency_ms") and b.get("latency_ms")]
                both_merged = [(a, b) for a, b in pairs if a.get("verdict") == "merged" and b.get("verdict") == "merged"]
                entry = {"size": size, "sf": float(size.split(":")[1]), "kind": kind, "other": other,
                         "pairs": len(pairs), "pairs_both_merged": len(both_merged),
                         "other_verdicts": dict(Counter(b.get("verdict") for _, b in pairs))}
                use = both_merged if both_merged else pairs
                entry["basis"] = "both merged" if both_merged else "time to decision (other policy reported a conflict)"
                lr = np.log(np.array([b["latency_ms"] / a["latency_ms"] for a, b in use]))
                if len(lr):
                    W = rng.multinomial(len(lr), np.full(len(lr), 1 / len(lr)), size=B)
                    bs = np.exp((W @ lr) / len(lr))
                    tci = t_interval(lr)
                    entry.update({
                        "geomean_ratio_other_over_snapmerge": rnd(math.exp(lr.mean()), 4),
                        "ci95_paired_bootstrap": [rnd(float(np.quantile(bs, 0.025)), 4), rnd(float(np.quantile(bs, 0.975)), 4)],
                        "ci95_t_on_log_ratio": [rnd(math.exp(tci[0]), 4), rnd(math.exp(tci[1]), 4)] if tci else None,
                        "ratios_by_rep": [rnd(math.exp(v), 4) for v in lr],
                        "pairs_other_slower": int((lr > 0).sum()),
                        "distinct_bootstrap_resamples_possible": int(math.comb(2 * len(lr) - 1, len(lr)))})
                paired.append(entry)
    checks = [r["check"] for r in recs if "check" in r]
    rev = (meta["config"].get("revision") or {})
    return {
        "role": ROLES.get(name), "runner": "scale", "config": meta["config"], "code_fingerprint": meta["code_fingerprint"],
        "records": len(recs), "merges": sum(1 for r in recs if r.get("verdict") == "merged"),
        "oracle_checks": {"checked_merges": len(checks),
                          "serial_equivalent": sum(1 for c in checks if c["serial_equivalent"]),
                          "both_orders": sum(1 for c in checks if c["eq_ac"] and c["eq_ca"]),
                          "readers_agree": sum(1 for c in checks if c["readers_agree"]),
                          "skipped_by_design": sum(1 for r in recs if r.get("check_skipped")),
                          "comparisons_per_check": 5,
                          "comparisons_derived_by_multiset_equality": sum(len(c.get("derived", [])) for c in checks),
                          "definition": "DuckDB and PyIceberg rows of the merge snapshot (streamed into on-disk DuckDB) "
                                        "vs (A;C) and (C;A) by EXCEPT ALL both ways, plus reader-vs-reader"},
        "errors": sum(1 for r in recs if r.get("verdict") == "error"),
        "groups": rows, "paired": paired,
        "protocol": {"isolated_processes": bool(rev.get("isolate")), "warm_page_cache": bool(rev.get("warm")),
                     "policy_order": rev.get("order", "fixed"), "reps": meta["config"]["reps"],
                     "reps_policy": rev.get("reps_policy") or {}, "check_reps": rev.get("check_reps")},
        "bootstrap": {"B": B, "seed": rng_seed, "resample_unit": "repetition (seeded scenario)"},
    }


# ============================================================================ counterexamples

def _row_str(cells) -> str:
    return "rid " + str(cells[0])


def _rows_compact(rows) -> str:
    if not rows:
        return "{}"
    return "{" + ", ".join(f"{_row_str(c)}" + (f" x{m}" if m != 1 else "") for c, m in rows) + "}"


def _op_compact(o: dict) -> str:
    k = o["kind"]
    mode = o.get("mode")
    s = k + (f"[{mode}]" if mode and k in ("DELETE", "UPSERT", "OVERWRITE") else "")
    if o.get("filter"):
        s += f" where {o['filter']}"
    if o.get("keys") is not None:
        s += " keys {" + ", ".join(str(x) for x in o["keys"]) + "}"
    if o.get("rows"):
        s += " rows {" + ", ".join(f"rid {r['rid']}" for r in o["rows"]) + "}"
    return s


def analyse_counterexamples(name: str, files: list[Path]) -> dict:
    recs = [json.loads(f.read_text()) for f in files]
    cases = []
    for r in recs:
        c = r["case"]
        o, m = r.get("original", {}), r.get("minimised", {})
        ev = m.get("evaluation", {})
        pol = ev.get("policies", {})
        cases.append({
            "name": c["name"], "kind": c["kind"], "policy": c["policy"], "replay_rule": c.get("replay_rule"),
            "seed": c.get("seed"), "error": r.get("error"),
            "original_property_holds": o.get("property_holds"), "minimised_property_holds": m.get("property_holds"),
            "original_size": o.get("size"), "minimised_size": m.get("size"), "evaluations": m.get("evaluations"),
            "true_conflict": ev.get("true_conflict"), "writer_check": ev.get("writer_check"),
            "verdicts": {p: v.get("verdict") for p, v in pol.items()},
            "serial_equivalent": {p: v.get("serial_equivalent") for p, v in pol.items() if v.get("verdict") == "merged"},
            "readers_agree": {p: v.get("readers_agree") for p, v in pol.items() if v.get("verdict") == "merged"},
            "snapmerge_sound": (pol.get("snapmerge", {}).get("verdict") == "conflict" and ev.get("true_conflict"))
            or (pol.get("snapmerge", {}).get("verdict") == "merged" and pol["snapmerge"].get("eq_ac")
                and pol["snapmerge"].get("eq_ca")),
            "text": {
                "layout": m.get("scenario", {}).get("layout"),
                "base": _rows_compact([[[b["rid"]], 1] for b in m.get("scenario", {}).get("base", [])]),
                "A": "; ".join(_op_compact(x) for x in m.get("scenario", {}).get("ops", {}).get("A", [])),
                "C": "; ".join(_op_compact(x) for x in m.get("scenario", {}).get("ops", {}).get("C", [])),
                "oracle_ac": _rows_compact(ev.get("oracle_rows_ac")), "oracle_ca": _rows_compact(ev.get("oracle_rows_ca")),
                "policy_result": (_rows_compact(pol.get(c["policy"], {}).get("merged_rows"))
                                  if pol.get(c["policy"], {}).get("verdict") == "merged"
                                  else f"conflict ({pol.get(c['policy'], {}).get('rule')})"),
                "snapmerge_result": (_rows_compact(pol.get("snapmerge", {}).get("merged_rows"))
                                     if pol.get("snapmerge", {}).get("verdict") == "merged" else "conflict"),
            },
        })
    cases.sort(key=lambda x: x["name"])
    kinds = Counter((c["kind"], c["policy"]) for c in cases)
    box = []
    for want in ("replay_unsound_fuzz439", "replay_unsound_identical_new_key_upserts"):
        box += [c["name"] for c in cases if c["name"] == want]
    for pol in ("cherry_pick", "rowlevel"):
        cand = sorted((c for c in cases if c["kind"] == "unsound" and c["policy"] == pol and c["minimised_property_holds"]),
                      key=lambda c: (c["minimised_size"] or [99]))
        box += [cand[0]["name"]] if cand else []
    for rule in sorted({c["replay_rule"] for c in cases if c["replay_rule"]}):
        cand = sorted((c for c in cases if c["replay_rule"] == rule and c["minimised_property_holds"]),
                      key=lambda c: (c["minimised_size"] or [99]))
        box += [cand[0]["name"]] if cand else []
    return {
        "role": ROLES.get(name), "runner": "counterexamples", "cases": len(cases),
        "errors": sum(1 for c in cases if c["error"]),
        "original_property_holds": sum(1 for c in cases if c["original_property_holds"]),
        "minimised_property_holds": sum(1 for c in cases if c["minimised_property_holds"]),
        "by_kind_policy": {f"{k}:{p}": v for (k, p), v in sorted(kinds.items())},
        "snapmerge_sound_on_all_minimised": all(c["snapmerge_sound"] for c in cases if c["minimised_property_holds"]),
        "snapmerge_merged_minimised": sum(1 for c in cases if c["verdicts"].get("snapmerge") == "merged"),
        "snapmerge_conflict_minimised": sum(1 for c in cases if c["verdicts"].get("snapmerge") == "conflict"),
        "true_conflicts_minimised": sum(1 for c in cases if c["true_conflict"]),
        "minimised_ops_range": [min(c["minimised_size"][0] for c in cases if c["minimised_size"]),
                                max(c["minimised_size"][0] for c in cases if c["minimised_size"])],
        "minimised_base_rows_range": [min(c["minimised_size"][1] for c in cases if c["minimised_size"]),
                                      max(c["minimised_size"][1] for c in cases if c["minimised_size"])],
        "size_fields": ["ops", "base_rows", "inserted_rows", "predicate_atoms", "base_chunks"],
        "box": box, "case_list": cases,
    }


# ============================================================================ telemetry (not fingerprinted)

def guard_telemetry() -> list[dict]:
    out = []
    for p in sorted(list(RUNS.glob(f"{PROJECT}_experiment_*.json")) + list(RUNS.glob(f"{PROJECT}_revision_*.json"))):
        try:
            s = json.loads(p.read_text())
        except Exception:
            continue
        att = s.get("attempts") or []
        out.append({"state": p.name, "status": s.get("status"), "returncode": s.get("returncode"),
                    "started_utc": s.get("started_utc"), "finished_utc": s.get("finished_utc"),
                    "attempts": len(att),
                    "batch_scales": [a.get("batch_scale") for a in att],
                    "peak_tree_rss_gb": rnd(max((a.get("peak_tree_rss_bytes", 0) for a in att), default=0) / 1e9, 2)})
    return out


# ============================================================================ environment, workload and mechanism facts (Write stage)

def _count_lines(paths: list[Path]) -> int:
    return sum(len(p.read_text(errors="replace").splitlines()) for p in paths)


def _workload_and_mechanisms(files, label: str = "main campaign") -> tuple[dict, dict]:
    """Workload composition and SnapMerge mechanisms of one difftest campaign (Write stage; generalised 2026-10-04)."""
    kinds, per_side = Counter(), []
    templates = Counter()
    mech, plan_c = Counter(), Counter()
    merged = 0
    for f in files:
        d = json.loads(Path(f).read_text())
        sc = d["scenario"]
        for side in ("A", "C"):
            ops = sc["ops"][side]
            per_side.append(len(ops))
            for o in ops:
                kinds[f"{o['kind']}[{o.get('mode', 'cow')}]"] += 1
        if sc.get("meta", {}).get("template"):
            templates[sc["meta"]["template"]] += 1
        p = d["policies"]["snapmerge"]
        if p["verdict"] == "merged":
            merged += 1
            for k, v in (p.get("details") or {}).get("hits", {}).items():
                if v:
                    mech[k] += 1
            pl = (p.get("details") or {}).get("plan") or {}
            if pl.get("contended_partitions"):
                plan_c["merges_with_contended_partitions"] += 1
            if pl.get("new_delete_files"):
                plan_c["merges_writing_new_position_delete_files"] += 1
            if pl.get("carried"):
                plan_c["merges_carrying_source_files"] += 1
    ps = np.array(per_side)
    w = {"scenarios": len(files), "ops_per_side_min": int(ps.min()),
         "ops_per_side_median": float(np.median(ps)), "ops_per_side_max": int(ps.max()),
         "ops_total": int(ps.sum()), "op_kinds": dict(sorted(kinds.items())),
         "adversarial_templates": dict(sorted(templates.items()))}
    m = {"merges": merged, "merges_with_hit": dict(mech.most_common()), **dict(plan_c),
         "note": f"number of SnapMerge merges ({label}) in which each rule/mechanism "
                 "counter was non-zero (rules.Decision.hits)"}
    return w, m


def environment_facts(S: dict, gate: dict) -> dict:
    """Facts the manuscript quotes that are not result metrics: host and software versions as recorded
    in each accepted campaign's environment.json, data provenance (data/*/provenance.json), memory-guard
    configuration (runs/ state files; not fingerprinted), code size (static line counts of the CURRENT
    code), the main campaign's workload composition and the SnapMerge mechanisms exercised by its merges
    (both from the accepted raw records only)."""
    out: dict = {}
    hosts, pys, pkgs, duck = {}, {}, {}, {}
    for name in S["campaigns"]:
        ep = RESULTS / name / "environment.json"
        if not ep.exists():
            continue
        env = json.loads(ep.read_text())
        for inv in env.get("invocations", []):
            h = inv.get("host") or {}
            key = json.dumps({k: h.get(k) for k in ("platform", "machine", "cpu_count", "memory_bytes")}, sort_keys=True)
            hosts.setdefault(key, []).append(name)
        py = (env.get("python") or {}).get("version", "").split(" ")[0]
        pys.setdefault(py, set()).add(name)
        pkgs.setdefault(json.dumps(env.get("packages", {}), sort_keys=True), set()).add(name)
        duck.setdefault(json.dumps(env.get("duckdb", {}), sort_keys=True), set()).add(name)
    out["hosts"] = [dict(json.loads(k), memory_gib=round((json.loads(k)["memory_bytes"] or 0) / 2**30, 1),
                         campaigns=sorted(set(v))) for k, v in hosts.items()]
    out["python"] = [{"version": k, "campaigns": sorted(v)} for k, v in pys.items()]
    out["packages"] = [dict(json.loads(k), campaigns=sorted(v)) for k, v in pkgs.items()]
    out["duckdb"] = [dict(json.loads(k), campaigns=sorted(v)) for k, v in duck.items()]
    data = []
    for pp in sorted((ROOT / "data").glob("*/*/provenance.json")):
        d = json.loads(pp.read_text())
        files = {k: {kk: v.get(kk) for kk in ("rows", "bytes", "sha256")} for k, v in (d.get("files") or {}).items()}
        data.append({"path": _relpath(pp.parent), "source": d.get("source"), "scale_factor": d.get("scale_factor"),
                     "files": files, "license": d.get("license"), "caveat": d.get("caveat"), "url": d.get("url"),
                     "retrieved": d.get("retrieved"), "generator": d.get("generator"),
                     "duckdb_version": d.get("duckdb_version")})
    for pp in sorted((ROOT / "data").glob("*/provenance.json")):  # one-level layout (data/nyc/; added 2026-10-04)
        d = json.loads(pp.read_text())
        files = {Path(v.get("path", k)).name: {"rows": v.get("rows"), "bytes": None, "sha256": v.get("sha256"),
                                               "raw_rows": v.get("raw_rows"), "dropped_rows": v.get("dropped_rows")}
                 for k, v in (d.get("clean") or {}).items()}
        raw = d.get("raw") or {}
        data.append({"path": _relpath(pp.parent), "source": d.get("source"), "scale_factor": None,
                     "files": files, "license": d.get("license"), "caveat": None,
                     "url": d.get("landing_page"), "retrieved": ", ".join(sorted({v.get("retrieved") for v in raw.values() if v.get("retrieved")})) or None,
                     "raw_files": {Path(v.get("url", k)).name: {kk: v.get(kk) for kk in ("bytes", "sha256", "url", "retrieved")}
                                   for k, v in raw.items()},
                     "cleaning": d.get("cleaning")})
    out["data"] = data
    guard = []
    for p in sorted(list(RUNS.glob(f"{PROJECT}_experiment_*.json")) + list(RUNS.glob(f"{PROJECT}_revision_*.json"))):
        try:
            s = json.loads(p.read_text())
        except Exception:
            continue
        dur = None
        try:
            t0 = time.mktime(time.strptime(s["started_utc"], "%Y-%m-%dT%H:%M:%SZ"))
            t1 = time.mktime(time.strptime(s["finished_utc"], "%Y-%m-%dT%H:%M:%SZ"))
            dur = round((t1 - t0) / 60.0, 1)
        except Exception:
            pass
        guard.append({"state": p.name.replace(f"{PROJECT}_", ""), "limit_gb": rnd((s.get("limit_bytes") or 0) / 1e9, 1),
                      "reserve_gb": rnd((s.get("reserve_bytes") or 0) / 1e9, 1),
                      "physical_gib": rnd((s.get("physical_bytes") or 0) / 2**30, 1), "duration_min": dur})
    out["guard"] = guard
    src = sorted((ROOT / "src" / "snapmerge").glob("*.py"))
    tests = sorted((ROOT / "tests").glob("*.py"))
    runners = [ROOT / "difftest.py", ROOT / "scale.py"]
    scripts = sorted((ROOT / "scripts").glob("*.py"))
    out["code"] = {"library_files": len(src), "library_lines": _count_lines(src),
                   "library_lines_by_module": {p.name: _count_lines([p]) for p in src},
                   "runner_lines": _count_lines(runners), "script_lines": _count_lines(scripts),
                   "analysis_lines": _count_lines([ROOT / "analyze.py"]),
                   "test_files": len(tests), "test_lines": _count_lines(tests),
                   "test_methods_static": sum(p.read_text().count("    def test") for p in tests),
                   "note": "static line counts of the current code (blank and comment lines included)"}
    # workload composition and mechanisms (main campaign and, since 2026-10-04, the NYC second source; accepted raw records only)
    for camp, suffix in ((MAIN, "main"), (NYC, "nyc")):
        g = gate.get(camp)
        if g and g.get("accepted"):
            w, mech = _workload_and_mechanisms(g["accepted"], "main campaign" if suffix == "main" else camp)
            out[f"workload_{suffix}"] = w
            out[f"snapmerge_mechanisms_{suffix}"] = mech
    return out


# ============================================================================ audit-derived facts (Finalize stage, additive)

def clopper_pearson_one_sided(k: int, n: int, level: float = 0.95) -> dict | None:
    """Exact one-sided Clopper-Pearson bounds at `level`: the upper bound on the rate when k = 0 and
    the lower bound when k = n (other k: both one-sided bounds)."""
    from scipy.stats import beta

    if not n:
        return None
    a = 1.0 - level
    upper = 1.0 if k == n else float(beta.ppf(level, k + 1, n - k))
    lower = 0.0 if k == 0 else float(beta.ppf(a, k, n - k + 1))
    return {"k": k, "n": n, "level": level, "upper_one_sided": round(upper, 5), "lower_one_sided": round(lower, 5),
            "two_sided_95": clopper_pearson(k, n)}


def _pred_eval(f: dict, row: dict) -> bool:
    """Pure-Python evaluation of a serialized filter AST (expr.py format) on one non-null row."""
    op = f["op"]
    if op == "true":
        return True
    if op == "false":
        return False
    if op == "cmp":
        a, b, c = row[f["col"]], f["val"], f["cmp"]
        return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b, "=": a == b, "!=": a != b}[c]
    if op == "in":
        return row[f["col"]] in f["vals"]
    if op == "and":
        return all(_pred_eval(x, row) for x in f["args"])
    if op == "or":
        return any(_pred_eval(x, row) for x in f["args"])
    if op == "not":
        return not _pred_eval(f["arg"], row)
    raise ValueError(f"unknown filter op {op!r}")


def _op_pred(o: dict) -> dict:
    if o["kind"] in ("DELETE", "OVERWRITE"):
        return o.get("filter_ast") or {"op": "false"}
    if o["kind"] == "UPSERT":
        return {"op": "in", "col": o.get("key_col", "rid"), "vals": list(o.get("keys") or [])}
    return {"op": "false"}


def _append_only_cow_ops(scenario: dict, side: str) -> list[dict]:
    """Copy-on-write UPSERT/OVERWRITE operations of `side` that insert rows but whose deletion predicate
    matches no live row of that side's state just before the operation.  The project writer commits
    such an operation through PyIceberg 0.12.0 as delete-then-append (writer.apply_op), and a delete
    that matches nothing commits no snapshot, so the operation is visible only as an 'append' snapshot.
    The side's state is simulated in pure Python from the stored base rows and the side's own ops."""
    state = [dict(r) for r in scenario.get("base", [])]
    out = []
    for i, o in enumerate(scenario.get("ops", {}).get(side, [])):
        pred = _op_pred(o)
        matched = sum(1 for r in state if _pred_eval(pred, r))
        rows = o.get("rows") or []
        if o.get("mode") == "cow" and o["kind"] in ("UPSERT", "OVERWRITE") and rows and matched == 0:
            out.append({"side": side, "index": i, "kind": o["kind"], "inserted_rows": len(rows)})
        if o["kind"] != "REPLACE":
            state = [r for r in state if not _pred_eval(pred, r)] + [dict(r) for r in rows]
    return out


def _outcome_facts(files) -> dict:
    """Per-campaign outcome facts (Finalize stage; generalised 2026-10-04): missed conflicts by serial order,
    time to a decision by outcome, conflict share per policy, SnapMerge latency with and without reads."""
    missed, lat, sm_io = {}, {}, {"no_reads": [], "reads": [], "bytes": []}
    early = Counter()
    n = 0
    for f in files:
        d = json.loads(Path(f).read_text())
        n += 1
        tc = bool(d["oracle"]["true_conflict"])
        for p, x in d["policies"].items():
            v = x.get("verdict")
            if v == "conflict":
                oc = "TP" if tc else "FP"
            elif v == "merged":
                oc = "FN" if tc else "TN"
            else:
                oc = "error"
            if x.get("latency_ms") is not None:
                lat.setdefault(p, {}).setdefault(oc, []).append(float(x["latency_ms"]))
            if v == "conflict":
                early[p] += 1
            if v == "merged" and tc:
                m = missed.setdefault(p, Counter())
                ac, ca = bool(x.get("eq_ac")), bool(x.get("eq_ca"))
                m["merged_true_conflicts"] += 1
                m["equal_A_then_C_only" if ac and not ca else "equal_C_then_A_only" if ca and not ac
                  else "equal_both" if ac and ca else "equal_neither"] += 1
            if p == "snapmerge" and x.get("latency_ms") is not None:
                io = x.get("io") or {}
                b = io.get("data_bytes_read", 0) + io.get("delete_bytes_read", 0)
                (sm_io["reads"] if b > 0 else sm_io["no_reads"]).append(float(x["latency_ms"]))
                if b > 0:
                    sm_io["bytes"].append(b)

    def q(xs):
        if not xs:
            return None
        a = np.asarray(xs, float)
        return {"n": int(len(a)), "median_ms": rnd(float(np.median(a)), 2),
                "p25_ms": rnd(float(np.quantile(a, 0.25)), 2), "p75_ms": rnd(float(np.quantile(a, 0.75)), 2)}

    return {
        "missed_conflicts_by_order": {p: dict(sorted(c.items())) for p, c in sorted(missed.items())},
        "latency_by_outcome": {p: {oc: q(v) for oc, v in sorted(d.items())} for p, d in sorted(lat.items())},
        "conflict_share": {p: {"conflicts": int(c), "n": n, "share": rnd(c / n, 4)} for p, c in sorted(early.items())},
        "snapmerge_latency_by_reads": {
            "without_data_reads": q(sm_io["no_reads"]), "with_data_reads": q(sm_io["reads"]),
            "median_bytes_read_when_reading": int(np.median(sm_io["bytes"])) if sm_io["bytes"] else None,
            "note": "data plus delete-file bytes read by SnapMerge; latency = time to a decision (merge or conflict)"},
    }


def audit_facts(S: dict, gate: dict) -> dict:
    """Facts raised in the Audit stage and quoted by the manuscript, recomputed here from the accepted
    raw records only (additive; no other summary.json section depends on them):
      - main campaign: merged true conflicts (missed conflicts) of every policy split by the serial order
        they equal ((A;C) = target then source, the rebase order; (C;A) = source then target);
      - main campaign: median time to a decision by outcome (TP, FP, FN, TN) per policy, and SnapMerge's
        latency split by whether it read any data or delete-file bytes;
      - exact one-sided 95% Clopper-Pearson bounds for the boundary counts quoted in the paper;
      - counterexamples: unsound replay / cherry-pick cases classified by whether the source side holds a
        copy-on-write upsert or overwrite that deleted nothing on its own branch (committed append-only)."""
    out: dict = {"note": "computed from accepted raw records only; added in the Finalize stage (2026-10-03); "
                         "nyc_* keys added 2026-10-04 for the second data source"}
    for camp, prefix in ((MAIN, "main"), (NYC, "nyc")):
        g = gate.get(camp)
        if g and g.get("accepted"):
            out.update({f"{prefix}_{k}": v for k, v in _outcome_facts(g["accepted"]).items()})
    # exact one-sided bounds for the boundary counts the paper quotes
    cps = {}
    c = S["campaigns"].get(MAIN)
    if c:
        smc = _g(c, "snapmerge")["counts"]
        rpc = _g(c, "replay")["counts"]
        cps["snapmerge_unsound_per_merge_main"] = clopper_pearson_one_sided(smc["soundness_violations"], smc["merged"])
        cps["snapmerge_recall_main"] = clopper_pearson_one_sided(smc["TP"], smc["true_conflicts"])
        cps["replay_neither_order_per_merge_main"] = clopper_pearson_one_sided(rpc["soundness_violations"], rpc["merged"])
    c = S["campaigns"].get(NYC)
    if c:
        smc = _g(c, "snapmerge")["counts"]
        rpc = _g(c, "replay")["counts"]
        cps["snapmerge_unsound_per_merge_nyc"] = clopper_pearson_one_sided(smc["soundness_violations"], smc["merged"])
        cps["snapmerge_recall_nyc"] = clopper_pearson_one_sided(smc["TP"], smc["true_conflicts"])
        cps["replay_neither_order_per_merge_nyc"] = clopper_pearson_one_sided(rpc["soundness_violations"], rpc["merged"])
    tot_m = tot_v = rep_m = rep_v = 0
    for name, cc in S["campaigns"].items():
        if cc.get("runner") != "difftest":
            continue
        for p, pc in (cc.get("policies") or {}).items():
            if p.startswith("snapmerge"):
                k = pc["groups"]["all"]["counts"]
                if p in REPRO_VARIANTS:
                    rep_m += k["merged"]
                    rep_v += k["soundness_violations"]
                    continue
                tot_m += k["merged"]
                tot_v += k["soundness_violations"]
    if tot_m:  # pooled merges are not independent trials (several configurations merge the same scenario)
        cps["snapmerge_all_variants_all_difftest_campaigns_unsound_per_merge"] = clopper_pearson_one_sided(tot_v, tot_m)
    if rep_m:
        cps["append_rule_reproduction_all_difftest_campaigns_neither_order_per_merge"] = clopper_pearson_one_sided(rep_v, rep_m)
    out["clopper_pearson_one_sided_95"] = cps
    # counterexample classification
    g = gate.get(CEX)
    if g and g.get("accepted"):
        cls = []
        for f in g["accepted"]:
            r = json.loads(Path(f).read_text())
            case = r["case"]
            if case.get("kind") != "unsound" or case.get("policy") not in ("replay", "cherry_pick"):
                continue
            row = {"name": case["name"], "policy": case["policy"]}
            for part in ("original", "minimised"):
                sc = (r.get(part) or {}).get("scenario")
                if not sc:
                    row[part] = None
                    continue
                try:
                    hits = _append_only_cow_ops(sc, "C")
                    row[part] = {"source_append_only_cow_ops": hits, "encoding_dependent": bool(hits),
                                 "true_conflict": ((r.get(part) or {}).get("evaluation") or {}).get("true_conflict"),
                                 "layout": sc.get("layout")}
                except Exception as e:  # unknown predicate form: leave unclassified
                    row[part] = {"error": repr(e)}
            cls.append(row)
        cls.sort(key=lambda x: (x["policy"], x["name"]))
        summ = {}
        for pol in ("replay", "cherry_pick"):
            rows = [x for x in cls if x["policy"] == pol]
            summ[pol] = {
                "cases": len(rows),
                "minimised_encoding_dependent": sum(1 for x in rows if (x.get("minimised") or {}).get("encoding_dependent")),
                "original_encoding_dependent": sum(1 for x in rows if (x.get("original") or {}).get("encoding_dependent")),
                "minimised_not_encoding_dependent": sorted(x["name"] for x in rows
                                                           if x.get("minimised") and "error" not in x["minimised"]
                                                           and not x["minimised"]["encoding_dependent"]),
                "unclassified": sorted(x["name"] for x in rows if any(isinstance(x.get(p), dict) and "error" in x[p]
                                                                      for p in ("original", "minimised")))}
        out["counterexample_encoding"] = {
            "summary": summ, "cases": cls,
            "definition": "encoding_dependent = the SOURCE side holds a copy-on-write UPSERT or OVERWRITE with inserted rows "
                          "whose deletion predicate matched no live row of the source branch just before it; the project "
                          "writer (PyIceberg 0.12.0 delete-then-append) then commits only an 'append' snapshot, which replay "
                          "and iterated cherry-pick re-add without validation. This classifies the cases; it does not test "
                          "replay under another encoding."}
    return out


# ============================================================================ hypotheses

def _g(c, p, g="all"):
    return c["policies"][p]["groups"][g]


def _m(c, p, m, g="all"):
    return _g(c, p, g)["metrics"][m]


def _fmt_ci(e, k=3, pct=False):
    if e is None or e.get("value") is None:
        return "n/a"
    f = (lambda x: f"{100 * x:.1f}") if pct else (lambda x: f"{x:.{k}f}")
    s = f(e["value"])
    if e.get("ci95"):
        s += f" [{f(e['ci95'][0])}, {f(e['ci95'][1])}]"
    return s


def hypotheses(S: dict) -> list[dict]:
    C = S["campaigns"]
    main, abl, fb, pil = (C.get(n) for n in (MAIN, ABLATION, FALLBACK, PILOT))
    scale, cx = C.get(SCALE), C.get(CEX)
    nyc = C.get(NYC)
    H = []
    dcs = [c for c in (pil, main, nyc, abl, fb) if c]

    # H1 soundness (pooled over every accepted difftest campaign; the append-rule reproduction is excluded)
    tot_m = tot_v = 0
    parts = []
    all_dc = [c for c in C.values() if c.get("runner") == "difftest"]
    for c in all_dc:
        for p in c["policies"]:
            if not p.startswith("snapmerge") or p in REPRO_VARIANTS:
                continue
            cnt = _g(c, p)["counts"]
            tot_m += cnt["merged"]
            tot_v += cnt["soundness_violations"]
    for c in dcs:
        cnt = _g(c, "snapmerge")["counts"]
        parts.append(f"{c['name']}: {cnt['soundness_violations']} of {cnt['merged']} SnapMerge merges")
    sc_m = sum(g["oracle_checks"] for g in scale["groups"] if g["policy"] == "snapmerge") if scale else 0
    sc_v = sum(g["oracle_checks"] - g["oracle_checks_serial_equivalent"] for g in scale["groups"] if g["policy"] == "snapmerge") if scale else 0
    m_main = _g(main, "snapmerge")["counts"]
    H.append({"id": "H1", "claim": "C2/C3: every merge SnapMerge emits equals a serial order of the two lineages (0 soundness violations)",
              "verdict": "supported" if tot_v == 0 and sc_v == 0 and (cx is None or cx["snapmerge_sound_on_all_minimised"]) else "not supported",
              "numbers": "; ".join(parts) + f"; all SnapMerge configurations in all {len(all_dc)} difftest campaigns together: {tot_v} of {tot_m} merges "
                         f"(not independent trials; the append-rule reproduction of the submitted version is excluded); "
                         f"scale_v5_tpch: {sc_v} of {sc_m}; counterexamples_v3: SnapMerge sound on "
                         f"{'all' if cx and cx['snapmerge_sound_on_all_minimised'] else 'NOT all'} {cx['minimised_property_holds'] if cx else 0} minimised cases. "
                         f"Exact 95% upper bound on the per-merge violation rate in the main campaign: "
                         f"{clopper_pearson(m_main['soundness_violations'], m_main['merged'])[1]:.5f} (0 of {m_main['merged']})",
              "caveat": "Empirical check under the frozen scenario generator; the theorem itself is a paper proof, not shown by the data."})
    # H2 recall
    parts, ok = [], True
    for c in dcs:
        e = _m(c, "snapmerge", "conflict_recall")
        ok &= e["value"] == 1.0
        parts.append(f"{c['name']}: {e['k']}/{e['n']} (exact 95% CI {e.get('ci95_exact')})")
    H.append({"id": "H2", "claim": "C3: SnapMerge's conflict recall is 100% (every scenario whose serial orders differ is reported)",
              "verdict": "supported" if ok else "not supported", "numbers": "; ".join(parts)})
    # H3 primary advantage over replay
    cmp_ = main["comparisons_vs_snapmerge"]["replay"]
    d = cmp_["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
    per = []
    n_pos = 0
    for g in [x for x in MIXES if x in cmp_]:
        e = cmp_[g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        n_pos += bool(e["ci95"] and e["ci95"][0] is not None and e["ci95"][0] > 0)
        per.append(f"{g} {_d(e)}")
    H.append({"id": "H3", "claim": "C4 (primary): SnapMerge auto-merges a larger share of commuting scenarios soundly than partition-level replay",
              "verdict": "supported" if d["ci95"][0] > 0 else ("not supported" if d["ci95"][1] < 0 else "inconclusive"),
              "numbers": f"main campaign, all mixes: SnapMerge {_fmt_ci(_m(main, 'snapmerge', 'sound_auto_merge_rate_of_commuting'))} vs replay "
                         f"{_fmt_ci(_m(main, 'replay', 'sound_auto_merge_rate_of_commuting'))}; paired difference {_d(d)}. "
                         f"Per mix (difference [95% CI]): " + "; ".join(per) + f". CI excludes 0 on {n_pos} of 7 mixes",
              "caveat": "On partition_disjoint_ingest both reach 1.0 (difference 0): replay is already complete there."})
    # H4 day-10 gate
    ge20 = [g for g in MIXES if g in cmp_
            and (cmp_[g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]["value"] or 0) >= 0.20]
    ge20_ci = [g for g in MIXES if g in cmp_
               and ((cmp_[g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]["ci95"] or [0])[0] or 0) >= 0.20]
    comp = cmp_.get("compaction_on_main", {}).get("snapmerge_minus_policy_sound_auto_merge_rate_of_commuting",
                                                   {"value": None, "ci95": None})
    H.append({"id": "H4", "claim": "Day-10 gate: SnapMerge's auto-merge advantage over replay is at least 20 points on at least one mix including compaction (otherwise reframe as E&A)",
              "verdict": "supported" if ge20 and (comp["value"] or 0) >= 0.2 else "not supported",
              "numbers": f"advantage >= 20 points (point estimate) on {len(ge20)} of 7 mixes ({', '.join(ge20)}); with the 95% CI lower bound >= 20 points on "
                         f"{len(ge20_ci)} of 7 ({', '.join(ge20_ci)}); compaction_on_main {_d(comp)}. Gate not triggered; no E&A reframing",
              "caveat": "Points are absolute percentage points of the sound auto-merge rate of commuting scenarios."})
    # H5 replay over-conservative and missing conflicts
    rc = _g(main, "replay")["counts"]
    H.append({"id": "H5", "claim": "C4: partition-level replay is over-conservative (false conflicts) and also misses true conflicts",
              "verdict": "supported" if rc["FP"] > 0 and rc["FN"] > 0 else "not supported",
              "numbers": f"main campaign replay: TP {rc['TP']}, FP {rc['FP']}, FN {rc['FN']}, TN {rc['TN']}; recall "
                         f"{_fmt_ci(_m(main, 'replay', 'conflict_recall'))}, precision {_fmt_ci(_m(main, 'replay', 'conflict_precision'))}; "
                         f"false-conflict rate on commuting scenarios {_fmt_ci(_m(main, 'replay', 'false_conflict_rate_of_commuting'))}"})
    cxr = sum(1 for c in cx["case_list"] if c["kind"] == "unsound" and c["policy"] == "replay" and c["minimised_property_holds"]) if cx else 0
    cxr_comm = sum(1 for c in cx["case_list"] if c["kind"] == "unsound" and c["policy"] == "replay" and c["minimised_property_holds"]
                   and not c["true_conflict"]) if cx else 0
    H.append({"id": "H6", "claim": "C4: replay can emit merges equal to NEITHER serial order (unsound)",
              "verdict": "supported" if cxr > 0 else "inconclusive",
              "numbers": f"{cxr} minimised counterexamples (counterexamples_v3; {cxr_comm} of them on commuting scenarios); "
                         f"but 0 such merges in the main campaign ({rc['soundness_violations']} of {rc['merged']} replay merges; exact 95% upper bound "
                         f"{clopper_pearson(rc['soundness_violations'], rc['merged'])[1]:.4f})",
              "caveat": "Existence only: the unsound replay cases come from the review fuzzer's tiny value domains, not from the main workload."})
    nf = _g(main, "nessie_force")["counts"]
    H.append({"id": "H7", "claim": "C4: FORCE / source-wins merges silently lose or add rows",
              "verdict": "supported" if nf["soundness_violations"] > 0 else "not supported",
              "numbers": f"Nessie FORCE (main): {nf['soundness_violations']} of {nf['merged']} merges equal neither serial order; "
                         f"{nf['rows_lost_vs_closest']} rows lost and {nf['rows_extra_vs_closest']} extra against the closer order; "
                         f"{nf['merges_with_row_loss']} merges lose rows against both orders"})
    # H8 cost vs row-level; H9 vs replay disjoint
    if scale:
        rl = [e for e in scale["paired"] if e["other"] == "rowlevel"]
        ok = all(e["ci95_paired_bootstrap"][0] > 1 for e in rl)
        H.append({"id": "H8", "claim": "C4: SnapMerge's merge latency is far below row-level three-way merge (which reads all data)",
                  "verdict": "supported" if ok else "inconclusive",
                  "numbers": "; ".join(f"SF {e['sf']:g} {e['kind']}: row-level/SnapMerge {_r(e, 'geomean_ratio_other_over_snapmerge', 'ci95_paired_bootstrap', 1)}x "
                                       f"({e['pairs_other_slower']}/{e['pairs']} reps slower)" for e in rl),
                  "caveat": f"{'/'.join(sorted({str(e['pairs']) for e in rl}))} paired reps per point; prototype implementations on a shared host (load average recorded per record)."})
        rp = [e for e in scale["paired"] if e["other"] == "replay" and e["kind"] == "disjoint"]
        verdicts = ["faster" if e["ci95_paired_bootstrap"][0] > 1 else ("slower" if e["ci95_paired_bootstrap"][1] < 1 else "unresolved")
                    for e in rp]
        H.append({"id": "H9", "claim": "C4: on disjoint merges SnapMerge costs no more than replay (both metadata-only)",
                  "verdict": "supported" if all(v == "faster" for v in verdicts) else
                  ("not supported" if any(v == "slower" for v in verdicts) else "inconclusive"),
                  "numbers": "; ".join(f"SF {e['sf']:g}: replay/SnapMerge {_r(e, 'geomean_ratio_other_over_snapmerge', 'ci95_paired_bootstrap', 3)} "
                                       f"({e['pairs_other_slower']}/{e['pairs']} reps replay slower) -> {v}"
                                       for e, v in zip(rp, verdicts)),
                  "caveat": f"n = {'/'.join(sorted({str(e['pairs']) for e in rp}))} paired reps per size; prototype on a shared host."})
        ov = {g["sf"]: g for g in scale["groups"] if g["kind"] == "overlap" and g["policy"] == "snapmerge"}
        ovr = {g["sf"]: g for g in scale["groups"] if g["kind"] == "overlap" and g["policy"] == "rowlevel"}
        frac = [ov[sf]["bytes_read_data_delete"]["max"] / ovr[sf]["bytes_read_data_delete"]["min"] for sf in sorted(ov)]
        H.append({"id": "H10", "claim": "C1: bounded reads touch only the contended partition, a small fraction (criterion: < 2% in every rep) "
                                        "of the data a full-data row-level merge reads",
                  "verdict": "supported" if all(f < 0.02 for f in frac) else "not supported",
                  "numbers": "; ".join(f"SF {sf:g}: SnapMerge reads {ov[sf]['bytes_read_data_delete']['median'] / 1e6:.2f} MB "
                                       f"vs row-level {ovr[sf]['bytes_read_data_delete']['median'] / 1e6:.0f} MB "
                                       f"({100 * ov[sf]['bytes_read_data_delete']['median'] / ovr[sf]['bytes_read_data_delete']['median']:.2f}%)"
                                       for sf in sorted(ov)),
                  "caveat": "SnapMerge's bytes grow with the contended partition (one month of lineitem), i.e. about 10x per 10x scale factor; "
                            "they are bounded by the partition, not constant."})
    # H11 ablation
    if abl:
        parts, ok = [], True
        for v in ABLATIONS:
            e = abl["comparisons_vs_snapmerge"][v]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
            ok &= e["ci95"][0] > 0
            parts.append(f"{v}: full minus variant {_d(e)}")
        H.append({"id": "H11", "claim": "C1: each rule family (net-effect test, compaction rebase, delete union, bounded reads) contributes auto-merges",
                  "verdict": "supported" if ok else "inconclusive", "numbers": "ablation_v4: " + "; ".join(parts)})
    # H12 fallback
    if fb:
        e = fb["comparisons_vs_snapmerge"]["replay"]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        H.append({"id": "H12", "claim": "Without recorded logical metadata (file-level rules only) SnapMerge still auto-merges at least as many commuting scenarios as replay",
                  "verdict": "not supported" if e["ci95"][1] < 0 else ("supported" if e["ci95"][0] >= 0 else "inconclusive"),
                  "numbers": f"fallback_filelevel_v4: SnapMerge {_fmt_ci(_m(fb, 'snapmerge', 'sound_auto_merge_rate_of_commuting'))} vs replay "
                             f"{_fmt_ci(_m(fb, 'replay', 'sound_auto_merge_rate_of_commuting'))}; difference {_d(e)}; "
                             f"replay misses {_g(fb, 'replay')['counts']['FN']} true conflicts, SnapMerge 0",
                  "caveat": "Negative result: SnapMerge's advantage depends on the recorded filters/keys."})
    # H13 row-level as sound upper bound
    rlc = _g(main, "rowlevel")["counts"]
    H.append({"id": "H13", "claim": "Plan Section 7: row-level three-way merge is a (sound) completeness upper bound",
              "verdict": "not supported" if rlc["soundness_violations"] > 0 else "supported",
              "numbers": f"main campaign row-level: {rlc['soundness_violations']} merges equal neither order "
                         f"({rlc['unsound_merges_on_commuting']} on commuting, {rlc['unsound_merges_on_true_conflicts']} on true conflicts); "
                         f"sound auto-merge rate of commuting {_fmt_ci(_m(main, 'rowlevel', 'sound_auto_merge_rate_of_commuting'))} vs SnapMerge "
                         f"{_fmt_ci(_m(main, 'snapmerge', 'sound_auto_merge_rate_of_commuting'))}; recall {_fmt_ci(_m(main, 'rowlevel', 'conflict_recall'))}"})
    # H14 readers
    rd = sum(_g(c, p)["counts"]["readers_disagree"] for c in dcs for p in c["policies"])
    mm = sum(_g(c, p)["counts"]["oracle_checks"] for c in dcs for p in c["policies"])
    wd = sum(c["oracle_checks"]["writer_check_reader_disagreements"] for c in dcs)
    wt = sum(c["oracle_checks"]["writer_checks"] for c in dcs)
    sd = sum(g["oracle_checks"] - g["oracle_checks_readers_agree"] for g in scale["groups"]) if scale else 0
    st_ = sum(g["oracle_checks"] for g in scale["groups"]) if scale else 0
    H.append({"id": "H14", "claim": "Plan Section 7: every emitted snapshot is read identically by PyIceberg and DuckDB",
              "verdict": "supported" if rd == 0 and wd == 0 and sd == 0 else "not supported",
              "numbers": f"difftest campaigns: {rd} disagreements over {mm} checked merges (all policies) and {wd} over {wt} writer checks; "
                         f"scale_v5_tpch: {sd} over {st_} checked merges",
              "caveat": "Spark and Trino readability is untested."})
    if scale:
        peak = max(g["process_peak_rss_gb_max"] for g in scale["groups"])
        H.append({"id": "H15", "claim": "Plan Section 6: the SF 10 oracle plus scans stay below about 16 GB",
                  "verdict": "not supported" if peak > 16 else "supported",
                  "numbers": f"scale_v5_tpch process peak RSS {peak:.1f} GB (max over records; per size: " +
                             ", ".join(f"SF {sf:g} {max(g['process_peak_rss_gb_max'] for g in scale['groups'] if g['sf'] == sf):.1f} GB"
                                       for sf in sorted({g['sf'] for g in scale['groups']})) + "); within the memory guard's limit "
                             "(48 GB; 80 GB for the cost steps re-run on 2026-10-08)"})
    H.append(h16(main, nyc))
    return H


def h16(main: dict, nyc: dict | None) -> dict:
    """H16 (second data source), data-driven since 2026-10-04: the differential-test results replicate on NYC TLC.
    Criteria (all must hold on difftest_main_nyc_v4): SnapMerge 0 soundness violations, conflict recall n/n,
    sound auto-merge rate of commuting = 1; paired SnapMerge-minus-replay 95% CI above 0 (pooled); replay has
    both false and missed conflicts. The NYC scale points were not run, so H8-H10 stay TPC-H-only."""
    claim = ("Plan Section 6: the differential-test results (soundness, recall, advantage over replay) hold on a second data source "
             "(NYC TLC yellow taxi 2023-01, day partitions; same generator, mixes, seed base and policies as the main campaign)")
    if not nyc:
        return {"id": "H16", "claim": claim, "verdict": "inconclusive",
                "numbers": "not run: no accepted difftest_main_nyc_v4 records"}
    s = _g(nyc, "snapmerge")["counts"]
    r = _g(nyc, "replay")["counts"]
    sam = _m(nyc, "snapmerge", "sound_auto_merge_rate_of_commuting")
    d = nyc["comparisons_vs_snapmerge"]["replay"]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
    dm = main["comparisons_vs_snapmerge"]["replay"]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
    ok = (s["soundness_violations"] == 0 and s["TP"] == s["true_conflicts"] and sam["value"] == 1.0
          and d["ci95"][0] > 0 and r["FP"] > 0 and r["FN"] > 0)
    per, n_pos, n_pos_m = [], 0, 0
    for g in MIXES:
        e = nyc["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        em = main["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        n_pos += bool(e["ci95"] and e["ci95"][0] > 0)
        n_pos_m += bool(em["ci95"] and em["ci95"][0] > 0)
        per.append(f"{g} {_d(e)} (TPC-H {em['value']:+.3f})")
    rc = _m(nyc, "snapmerge", "conflict_recall")
    return {"id": "H16", "claim": claim, "verdict": "supported" if ok else "not supported",
            "numbers": f"difftest_main_nyc_v4 ({nyc['scenarios']} scenarios, {nyc['true_conflicts']} true conflicts): SnapMerge TP {s['TP']}, "
                       f"FP {s['FP']}, FN {s['FN']}, TN {s['TN']}, {s['soundness_violations']} of {s['merged']} merges unsound "
                       f"(exact 95% upper bound {clopper_pearson(s['soundness_violations'], s['merged'])[1]:.5f}), recall {rc['k']}/{rc['n']} "
                       f"(exact 95% CI {rc.get('ci95_exact')}); sound auto-merge of commuting SnapMerge {_fmt_ci(sam)} vs replay "
                       f"{_fmt_ci(_m(nyc, 'replay', 'sound_auto_merge_rate_of_commuting'))}; paired difference {_d(d)} "
                       f"(TPC-H main {_d(dm)}). Replay TP {r['TP']}, FP {r['FP']}, FN {r['FN']}, TN {r['TN']}, "
                       f"{r['soundness_violations']} of {r['merged']} merges equal neither order. Per mix: " + "; ".join(per) +
                       f". CI excludes 0 on {n_pos} of 7 mixes (TPC-H: {n_pos_m} of 7)",
            "caveat": "Same scenario generator and mixes on a different source table (timestamps, day partitions); the NYC scale points "
                      "(1/3/12 months) were not run, because only the 2023-01 download was approved, so the cost results (H8-H10) remain "
                      "TPC-H-only."}


# ============================================================================ markdown

def md_table(header: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join("" if c is None else str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def _ci(e, k=3, pct=False):
    return _fmt_ci(e, k, pct)


def _d(e) -> str:
    """Signed difference with its interval."""
    if not e or e.get("value") is None:
        return "n/a"
    s = f"{e['value']:+.3f}"
    return s + (f" [{e['ci95'][0]:+.3f}, {e['ci95'][1]:+.3f}]" if e.get("ci95") else "")


def _r(e, key="geomean_ratio", ci="ci95", k=2) -> str:
    """Ratio with its interval."""
    if not e or e.get(key) is None:
        return "n/a"
    s = f"{e[key]:.{k}f}"
    return s + (f" [{e[ci][0]:.{k}f}, {e[ci][1]:.{k}f}]" if e.get(ci) else "")


def _exact(e):
    return f" (exact {e['ci95_exact']})" if e.get("ci95_exact") else ""


def write_markdown(S: dict) -> str:
    C = S["campaigns"]
    L = []
    L.append("# SnapMerge: Analysis-stage results\n")
    L.append(f"Generated {S['generated']} by `analyze.py` from fingerprint-verified raw results only. "
             f"Bootstrap B = {S['params']['B']}, seed {S['params']['seed']}. All numbers below are recomputed from "
             "`results/<campaign>/raw/*.json`. Intervals are 95% percentile bootstraps over scenarios (one seed each), stratified "
             "by mix, unless marked otherwise. Comparisons with SnapMerge are paired: both policies use the same resamples. "
             "Where a count is 0 of n or n of n, the exact Clopper-Pearson interval is also given.\n")
    L.append("Rates: *recall* = TP / true conflicts (errors and merges on true conflicts count as misses). *Precision* = TP / reported conflicts. "
             "*Sound auto-merge rate of commuting* = merges equal to a serial order / commuting scenarios. A *soundness violation* is a merge "
             "equal to neither serial order in DuckDB and PyIceberg.\n")
    # gate
    L.append("## 1. Inputs and fingerprint gate\n")
    rows = []
    for n, g in S["gate"].items():
        code = g.get("code", {}).get("status", "-")
        src = ("ok" if g.get("sources", {}).get("ok") else "MISMATCH") if "sources" in g else "-"
        rows.append([n, g.get("runner") or "-", g.get("recorded_code_fingerprint") or "-", code, src,
                     g["accepted_records"], g["rejected_records"], g.get("missing_planned"), g["status"]])
    L.append(md_table(["Directory", "Runner", "Recorded code fp", "Code vs current source", "Source data sha256",
                       "Records read", "Records rejected", "Planned but missing", "Status"], rows))
    eq = [n for n, g in S["gate"].items() if g.get("code", {}).get("status") == "equivalent"]
    L.append(f"\n*equivalent* ({', '.join(eq)}): the current code differs from the campaign code only by the top-level function "
             "`catalog.pyiceberg_reader_groups`, added in the Experiment stage for scale checks. Removing it from the current `catalog.py` "
             "gives a file whose sha256 equals the one recorded in each campaign's environment.json, and that reproduces the recorded "
             "code fingerprint. No code file of these runners references the function. Rejected directories were produced by earlier code "
             "and are not analysed. The submitted version's campaigns (" + ", ".join(
                 f"`{k}`" for k in SUPERSEDED if k not in REREVIEW_SUPERSEDED | MINOR_SUPERSEDED)
             + ") are superseded by the revision campaigns (code changed 2026-10-05), the 2026-10-05 revision "
             "campaigns (" + ", ".join(f"`{k}`" for k in sorted(REREVIEW_SUPERSEDED))
             + ") by the re-review campaigns (code changed 2026-10-07), and the re-review campaigns ("
             + ", ".join(f"`{k}`" for k in sorted(MINOR_SUPERSEDED))
             + ") by the minor-revision campaigns (code changed 2026-10-08; same seeds and settings throughout). "
             "The review and smoke runs predate the Experiment-stage fixes.\n")
    # oracle checks
    L.append("## 2. Oracle-check counts\n")
    rows = []
    for n, c in C.items():
        if c["runner"] == "difftest":
            o = c["oracle_checks"]
            rows.append([n, c["scenarios"], c["true_conflicts"], f"{o['writer_checks'] - o['writer_check_failures']}/{o['writer_checks']}",
                         o["emitted_merges_compared_with_both_orders_in_both_readers"], o["emitted_merges_all_policies"],
                         _g(c, "snapmerge")["counts"]["oracle_check_serial_equivalent"], _g(c, "snapmerge")["counts"]["merged"]])
    L.append(md_table(["Campaign", "Scenarios", "True conflicts (oracle)", "Writer checks pass", "Merges oracle-checked (all policies)",
                       "Merges emitted (all policies)", "SnapMerge merges = serial order", "SnapMerge merges"], rows))
    sc = C.get(SCALE)
    if sc:
        o = sc["oracle_checks"]
        L.append(f"\nscale_v5_tpch: {sc['records']} records, {sc['merges']} merges, {o['checked_merges']} oracle-checked, "
                 f"{o['serial_equivalent']} serial-equivalent, {o['readers_agree']} with readers agreeing. Each check makes 5 comparisons; "
                 f"{o['comparisons_derived_by_multiset_equality']} of them are derived exactly from multiset equality. Errors: {sc['errors']}.")
    cx = C.get(CEX)
    if cx:
        L.append(f"counterexamples_v3: {cx['cases']} cases, original property holds on {cx['original_property_holds']} and the minimised "
                 f"property on {cx['minimised_property_holds']}, {cx['errors']} errors. Every candidate is checked with the pure-Python "
                 "multiset oracle, both readers and the writer check.\n")
    # Table 1
    m = C[MAIN]
    L.append("## 3. Table 1: differential test per mix (main campaign `difftest_main_tpch_v4`)\n")
    L.append(f"TPC-H SF 0.1 lineitem, base 2,000 rows, 429 scenarios per mix, seed base {m['config']['seed_base']}. "
             "Each cell gives recall / precision / unsound merges.\n")
    pols = ["snapmerge", "replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "nessie_force", "fast_forward"]
    rows = []
    for g in m["groups"]:
        r = [MIX_LABEL[g], _g(m, "snapmerge", g)["counts"]["n"], _g(m, "snapmerge", g)["counts"]["true_conflicts"]]
        for p in pols:
            cnt = _g(m, p, g)["counts"]
            rec = _m(m, p, "conflict_recall", g)["value"]
            pre = _m(m, p, "conflict_precision", g)["value"]
            r.append(f"{rec:.3f} / {'-' if pre is None else f'{pre:.3f}'} / {cnt['soundness_violations']}")
        rows.append(r)
    L.append(md_table(["Mix", "n", "True conf."] + [POLICY_LABEL[p] for p in pols], rows))
    # Figure 1 data
    L.append("\n### Figure 1 data: sound auto-merge rate of commuting scenarios, with 95% CI\n")
    rows = []
    for g in m["groups"]:
        r = [MIX_LABEL[g], _g(m, "snapmerge", g)["counts"]["commuting"]]
        for p in ("snapmerge", "replay", "cherry_pick", "rowlevel"):
            r.append(_ci(_m(m, p, "sound_auto_merge_rate_of_commuting", g)))
        e = m["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        r.append(_d(e))
        rows.append(r)
    L.append(md_table(["Mix", "Commuting", "SnapMerge", "Replay", "Cherry-pick", "Row-level (sound only)", "SnapMerge - replay (paired)"], rows))
    # per-configuration tables
    L.append("\n## 4. Per-configuration tables (all mixes pooled)\n")
    for n, c in C.items():
        if c["runner"] != "difftest":
            continue
        cfg = c["config"]
        src_desc = (f"months {','.join(cfg.get('months') or [])}, partition {'/'.join(cfg.get('partition') or [])}"
                    if str(cfg["source"]).startswith("nyc") else f"SF {cfg['sf']}")
        L.append(f"### {n} ({c['role']}): source {cfg['source']} {src_desc}, base {cfg['base_rows']} rows, {cfg['per_mix']} per mix, "
                 f"seed base {cfg['seed_base']}, writers record logical metadata: {cfg['record']}\n")
        rows = []
        for p in c["policies"]:
            cnt = _g(c, p)["counts"]
            met = _g(c, p)["metrics"]
            rows.append([POLICY_LABEL.get(p, p), f"{cnt['TP']}/{cnt['FP']}/{cnt['FN']}/{cnt['TN']}", cnt["errors"],
                         f"{cnt['oracle_check_serial_equivalent']}/{cnt['merged']}", cnt["soundness_violations"], cnt["readers_disagree"],
                         _ci(met["conflict_recall"]) + _exact(met["conflict_recall"]), _ci(met["conflict_precision"]),
                         _ci(met["sound_auto_merge_rate_of_commuting"]) + _exact(met["sound_auto_merge_rate_of_commuting"]),
                         _ci(met["median_latency_ms_all"], 1), f"{met['mean_bytes_read']['value'] / 1e3:.1f}"])
        L.append(md_table(["Policy", "TP/FP/FN/TN", "Errors", "Merges = serial order / merges", "Unsound merges", "Reader disagreements",
                           "Recall [95% CI]", "Precision [95% CI]", "Sound auto-merge of commuting [95% CI]",
                           "Median latency ms [95% CI]", "Mean data+delete KB read"], rows))
        L.append(f"\nLoad average (1 min) during policies: {c['loadavg_1m']}. MEMGUARD batch scales seen: {c['memguard_batch_scales']}. "
                 f"Cross-check against `difftest.policy_metrics` on the same records: {c['cross_check']['status']} "
                 f"({len(c['cross_check'].get('mismatches', []))} mismatches over {c['cross_check'].get('policies')} policies).\n")
    # paired comparisons main
    L.append("## 5. Paired comparisons against SnapMerge (main campaign, all mixes)\n")
    rows = []
    for p, cm in m["comparisons_vs_snapmerge"].items():
        e = cm["all"]
        a = e["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        rr = e["snapmerge_minus_policy_conflict_recall"]
        l1 = e["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_both_sound"]
        l2 = e["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_all"]
        rows.append([POLICY_LABEL.get(p, p), _d(a), _d(rr),
                     f"{_r(l1)} (n={l1['pairs']})" if l1.get("geomean_ratio") else "n/a",
                     f"{_r(l2)} (n={l2['pairs']})" if l2.get("geomean_ratio") else "n/a"])
    L.append(md_table(["Policy", "SnapMerge - policy: sound auto-merge of commuting", "SnapMerge - policy: recall",
                       "Latency policy/SnapMerge, both merged soundly (geomean)", "Latency policy/SnapMerge, all scenarios (time to decision)"], rows))
    # Table 2 FORCE
    L.append("\n## 6. Table 2: silent loss of FORCE / source-wins merges per mix (main campaign)\n")
    rows = []
    for g in m["groups"]:
        cnt = _g(m, "nessie_force", g)["counts"]
        rows.append([MIX_LABEL[g], cnt["merged"], cnt["soundness_violations"], cnt["merges_with_row_loss"],
                     cnt["rows_lost_vs_closest"], cnt["rows_extra_vs_closest"]])
    L.append(md_table(["Mix", "FORCE merges", "Equal neither order", "Lose rows vs both orders", "Rows lost (vs closer order)",
                       "Rows extra (vs closer order)"], rows))
    # Scale
    if sc:
        L.append("\n## 7. Merge cost versus table size (`scale_v5_tpch`, TPC-H lineitem; isolated processes, seeded random order)\n")
        L.append("Latency in ms: mean [Student-t 95% CI], median, min-max. Bytes are medians. Metadata bytes written are approximate (F12). "
                 "Replay's overlap rows give the time to report a (false) partition conflict, not a merge.\n")
        rows = []
        for gr in sc["groups"]:
            lt = gr["latency_ms"]
            rows.append([f"{gr['sf']:g}", gr["kind"], POLICY_LABEL[gr["policy"]], ",".join(f"{k} {v}" for k, v in gr["verdicts"].items()),
                         f"{gr['oracle_checks_serial_equivalent']}/{gr['oracle_checks']}",
                         f"{lt['mean']:.1f} [{lt['ci95_t'][0]:.1f}, {lt['ci95_t'][1]:.1f}]", f"{lt['median']:.1f}", f"{lt['min']:.1f}-{lt['max']:.1f}",
                         f"{gr['bytes_read_data_delete']['median'] / 1e6:.3f} ({gr['bytes_read_data_delete']['min'] / 1e6:.3f}-{gr['bytes_read_data_delete']['max'] / 1e6:.3f})",
                         f"{gr['bytes_read_metadata']['median'] / 1e6:.3f}",
                         f"{gr['bytes_written_data_delete']['median'] / 1e6:.3f}", f"{gr['bytes_written_metadata_approx']['median'] / 1e3:.1f}",
                         f"{gr['loadavg_1m']['median']:.1f}"])
        L.append(md_table(["SF", "Kind", "Policy", "Verdicts", "Oracle pass", "Latency mean [t-CI]", "Median", "Min-max",
                           "Data+delete MB read, median (min-max)", "Metadata MB read", "Data+delete MB written", "Metadata KB written (approx)", "Load avg"], rows))
        L.append("\nPaired latency ratios by repetition (other / SnapMerge; geometric mean, 95% paired bootstrap over the "
                 "paired reps, t-interval on log ratios; repetitions are paired by index):\n")
        rows = [[f"{e['sf']:g}", e["kind"], POLICY_LABEL[e["other"]], e["basis"],
                 _r(e, "geomean_ratio_other_over_snapmerge", "ci95_paired_bootstrap", 3),
                 _r(e, "geomean_ratio_other_over_snapmerge", "ci95_t_on_log_ratio", 3), f"{e.get('pairs_other_slower')}/{e['pairs']}"]
                for e in sc["paired"]]
        L.append(md_table(["SF", "Kind", "Other", "Basis", "Geomean ratio [paired bootstrap 95% CI]", "Geomean ratio [t 95% CI on logs]",
                           "Reps other slower"], rows))
        _dr = sorted({(e["pairs"], e.get("distinct_bootstrap_resamples_possible")) for e in sc["paired"]})
        L.append("\nDistinct paired-bootstrap resamples: " + ", ".join(f"{n} pairs -> {d}" for n, d in _dr)
                 + "; with 3 pairs the intervals are coarse.")
        bases = {g["sf"]: (g["base_rows"], g["base_data_files"]) for g in sc["groups"]}
        L.append(f"\nBase tables: " + ", ".join(f"SF {sf:g}: {r:,} rows in {f:,} files" for sf, (r, f) in sorted(bases.items()))
                 + ". Process peak RSS (max over records): " + ", ".join(
            f"SF {sf:g} {max(g['process_peak_rss_gb_max'] for g in sc['groups'] if g['sf'] == sf):.1f} GB" for sf in sorted({g['sf'] for g in sc['groups']})) + ".\n")
    # ablation
    a = C.get(ABLATION)
    if a:
        L.append("## 8. Ablation of rule families (`ablation_v4`, 1,505 scenarios; main campaign as replication)\n")
        rows = []
        full_a = _m(a, "snapmerge", "sound_auto_merge_rate_of_commuting")["value"]
        rep_a = _m(a, "replay", "sound_auto_merge_rate_of_commuting")["value"]
        for v in ["snapmerge"] + ABLATIONS + ["snapmerge[filelevel]", "replay"]:
            ea = _m(a, v, "sound_auto_merge_rate_of_commuting") if v in a["policies"] else None
            em = _m(m, v, "sound_auto_merge_rate_of_commuting") if v in m["policies"] else None
            da = a["comparisons_vs_snapmerge"].get(v, {}).get("all", {}).get("snapmerge_minus_policy_sound_auto_merge_rate_of_commuting")
            share = (full_a - ea["value"]) / (full_a - rep_a) if ea and v not in ("snapmerge", "replay") else None
            rows.append([POLICY_LABEL[v], _ci(ea) if ea else "-", _d(da) if da else "-",
                         f"{100 * share:.0f}%" if share is not None else "-",
                         _g(a, v)["counts"]["soundness_violations"] if v in a["policies"] else "-",
                         _ci(em) if em else "-"])
        L.append(md_table(["Variant", "ablation_v4 sound auto-merge of commuting", "Full - variant (paired)",
                           "Share of SnapMerge's advantage over replay lost", "Unsound merges", "Main campaign (replication)"], rows))
        L.append("\nThe shares do not add up to 100%: the rule families interact, because one scenario can need several of them.\n")
        rows = []
        for g in MIXES:
            rows.append([MIX_LABEL[g]] + [f"{_m(a, v, 'sound_auto_merge_rate_of_commuting', g)['value']:.3f}" for v in ["snapmerge"] + ABLATIONS + ["replay"]])
        L.append(md_table(["Mix"] + [POLICY_LABEL[v] for v in ["snapmerge"] + ABLATIONS + ["replay"]], rows))
    fb = C.get(FALLBACK)
    if fb:
        L.append("\n## 9. Fallback without recorded metadata (`fallback_filelevel_v4`)\n")
        rows = []
        for g in fb["groups"]:
            e = fb["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
            rows.append([MIX_LABEL[g], _g(fb, "snapmerge", g)["counts"]["commuting"], _ci(_m(fb, "snapmerge", "sound_auto_merge_rate_of_commuting", g)),
                         _ci(_m(fb, "replay", "sound_auto_merge_rate_of_commuting", g)), _d(e),
                         _g(fb, "replay", g)["counts"]["FN"], _g(fb, "snapmerge", g)["counts"]["FN"]])
        L.append(md_table(["Mix", "Commuting", "SnapMerge (file-level)", "Replay", "SnapMerge - replay (paired)", "Replay missed conflicts",
                           "SnapMerge missed conflicts"], rows))
    if cx:
        L.append("\n## 10. Counterexamples (`counterexamples_v3`)\n")
        rows = [[c["name"], c["kind"], POLICY_LABEL[c["policy"]], c["original_size"], c["minimised_size"], c["true_conflict"],
                 c["verdicts"].get("snapmerge"), c["minimised_property_holds"]] for c in cx["case_list"]]
        L.append(md_table(["Case", "Kind", "Policy", "Original size", "Minimised size", "True conflict", "SnapMerge", "Property holds"], rows))
        L.append("\nSize = (operations, base rows, inserted rows, predicate atoms, base chunks). The minimiser never drops the last base row.\n")
        L.append("### Counterexample box (minimised)\n")
        by = {c["name"]: c for c in cx["case_list"]}
        for nm in cx["box"]:
            c = by[nm]
            t = c["text"]
            L.append(f"- **{nm}** ({c['kind']}, {POLICY_LABEL[c['policy']]}; layout {t['layout']}). Base {t['base']}. "
                     f"A: {t['A']}. C: {t['C']}. Oracle (A;C) = {t['oracle_ac']}, (C;A) = {t['oracle_ca']}. "
                     f"{POLICY_LABEL[c['policy']]}: {t['policy_result']}. SnapMerge: {t['snapmerge_result']}.")
    # hypotheses
    L.append("\n## 11. Hypotheses\n")
    L.append("Verdict rules: *supported* means the point estimate meets the criterion and the 95% interval lies on the claimed side. "
             "For zero-event claims this means 0 events were observed, and the exact upper bound is reported. *Not supported* means the "
             "estimate contradicts the criterion and the interval excludes the claimed side. *Inconclusive* means the interval straddles "
             "the criterion or the data were not collected.\n")
    for h in S["hypotheses"]:
        L.append(f"- **{h['id']} ({h['verdict'].upper()})**: {h['claim']}. {h['numbers'].rstrip('.')}." + (f" *Caveat:* {h['caveat']}" if h.get("caveat") else ""))
    L.append("\n## 12. Negative findings (kept)\n")
    L += [f"- {x}" for x in S["negative_findings"]]
    L.append("\n## 13. What the data does NOT show\n")
    L += [f"- {x}" for x in S["does_not_show"]]
    L.append("\n## 14. Resource telemetry (guard state files; not fingerprinted, for context only)\n")
    rows = [[t["state"].replace(f"{PROJECT}_", ""), t["status"], t["returncode"], t["attempts"], t["batch_scales"], t["peak_tree_rss_gb"],
             t["started_utc"], t["finished_utc"]] for t in S["telemetry"]["guard"]]
    L.append(md_table(["Guard state", "Status", "rc", "Attempts", "Batch scales", "Peak tree RSS GB", "Started (UTC)", "Finished (UTC)"], rows))
    L.append(f"\nAnalysis run: {S['telemetry']['analysis_seconds']} s, peak RSS {S['telemetry']['analysis_peak_rss_gb']} GB.\n")
    L.append("## 15. Figures (drawn from results/summary.json only)\n")
    L += [f"- `{f['file']}`: {f['caption']}" for f in S.get("figures", [])]
    E = S.get("environment")
    if E:
        L.append("\n## 16. Environment, data provenance, code size, workload and mechanisms\n")
        L.append("Recorded in each accepted campaign's environment.json (host, versions), data/*/provenance.json (data), the guard "
                 "state files in runs/ (guard; not fingerprinted), static line counts of the current code, and the accepted raw "
                 "records of the main campaign (workload, mechanisms).\n")
        for h in E["hosts"]:
            L.append(f"- Host: {h['platform']}, {h['machine']}, {h['cpu_count']} logical CPUs, {h['memory_gib']} GiB memory "
                     f"(campaigns: {', '.join(h['campaigns'])}).")
        for py in E["python"]:
            L.append(f"- Python {py['version']} (campaigns: {', '.join(py['campaigns'])}).")
        for pk in E["packages"]:
            L.append("- Packages: " + ", ".join(f"{k} {v}" for k, v in pk.items() if k != "campaigns") + ".")
        for dk in E["duckdb"]:
            ext = ", ".join(f"{k} {v.get('version')}" for k, v in (dk.get("extensions") or {}).items())
            L.append(f"- DuckDB {dk.get('duckdb_version')}; extensions: {ext}.")
        rows = []
        for d in E["data"]:
            for fn, fv in d["files"].items():
                rows.append([d["path"], d["source"], d["scale_factor"], fn, fv["rows"], fv["bytes"], fv["sha256"], d["license"], d["retrieved"]])
        L.append("")
        L.append(md_table(["Data dir", "Source", "SF", "File", "Rows", "Bytes", "sha256", "License", "Retrieved"], rows))
        if E["data"]:
            L.append(f"\nCaveat: {E['data'][0]['caveat']} URL: {E['data'][0]['url']}\n")
        rows = [[x["state"], x["limit_gb"], x["reserve_gb"], x["physical_gib"], x["duration_min"]] for x in E["guard"]]
        L.append(md_table(["Guard state", "Limit GB", "Reserve GB", "Host physical GiB", "Duration min"], rows))
        c = E["code"]
        L.append(f"\nCode (static line counts, current code): library {c['library_lines']} lines in {c['library_files']} modules "
                 f"({', '.join(f'{k} {v}' for k, v in c['library_lines_by_module'].items())}); runners {c['runner_lines']}; "
                 f"scripts {c['script_lines']}; analysis {c['analysis_lines']}; tests {c['test_lines']} lines in {c['test_files']} files, "
                 f"{c['test_methods_static']} test methods.\n")
        w = E.get("workload_main")
        if w:
            L.append(f"Main-campaign workload: {w['scenarios']} scenarios, {w['ops_total']} operations; operations per side "
                     f"min {w['ops_per_side_min']}, median {w['ops_per_side_median']}, max {w['ops_per_side_max']}. Operation kinds: "
                     + ", ".join(f"{k} {v}" for k, v in w["op_kinds"].items()) + ". Adversarial templates: "
                     + ", ".join(f"{k} {v}" for k, v in w["adversarial_templates"].items()) + ".\n")
        m = E.get("snapmerge_mechanisms_main")
        if m:
            extra = {k: v for k, v in m.items() if k.startswith("merges_") and k != "merges_with_hit"}
            L.append(f"SnapMerge mechanisms in the main campaign's {m['merges']} merges (merges with the counter non-zero): "
                     + ", ".join(f"{k} {v}" for k, v in m["merges_with_hit"].items()) + "; "
                     + ", ".join(f"{k.replace('merges_', '')} {v}" for k, v in extra.items()) + ".\n")
    AF = S.get("audit_facts")
    if AF:
        L.append("\n## 17. Audit-derived facts (added in the Finalize stage; accepted raw records only)\n")
        mo = AF.get("main_missed_conflicts_by_order") or {}
        if mo:
            L.append("Missed conflicts (merges of true conflicts) in the main campaign by the serial order they equal "
                     "((A;C) = target then source, the rebase order of replay and cherry-pick; (C;A) = source then target):\n")
            keys = ["merged_true_conflicts", "equal_A_then_C_only", "equal_C_then_A_only", "equal_neither", "equal_both"]
            L.append(md_table(["Policy"] + keys, [[POLICY_LABEL.get(p, p)] + [c.get(k, 0) for k in keys]
                                                  for p, c in mo.items()]))
        lo = AF.get("main_latency_by_outcome") or {}
        if lo:
            L.append("\nMedian time to a decision by outcome in the main campaign (ms; n in parentheses; p25-p75):\n")
            ocs = ["TP", "FP", "FN", "TN", "error"]
            rows = []
            for p, d in lo.items():
                rows.append([POLICY_LABEL.get(p, p)] + [
                    (f"{d[o]['median_ms']} (n={d[o]['n']}; {d[o]['p25_ms']}-{d[o]['p75_ms']})" if d.get(o) else "")
                    for o in ocs])
            L.append(md_table(["Policy"] + ocs, rows))
        cs = AF.get("main_conflict_share") or {}
        if cs.get("replay"):
            r = cs["replay"]
            L.append(f"\nReplay reports a conflict (stops before replaying) in {r['conflicts']} of {r['n']} main scenarios "
                     f"(share {r['share']}).")
        sr = AF.get("main_snapmerge_latency_by_reads")
        if sr and sr.get("with_data_reads"):
            a, b = sr["without_data_reads"], sr["with_data_reads"]
            L.append(f"SnapMerge time to a decision without data/delete-file reads: median {a['median_ms']} ms (n={a['n']}); "
                     f"with reads: median {b['median_ms']} ms (n={b['n']}; median {sr['median_bytes_read_when_reading']} bytes read).\n")
        cp = AF.get("clopper_pearson_one_sided_95") or {}
        if cp:
            L.append("Exact Clopper-Pearson bounds (one-sided 95% versus the two-sided 95% interval):\n")
            L.append(md_table(["Quantity", "k", "n", "one-sided 95% upper", "one-sided 95% lower", "two-sided 95%"],
                              [[k, v["k"], v["n"], v["upper_one_sided"], v["lower_one_sided"], v["two_sided_95"]]
                               for k, v in cp.items() if v]))
        ce = AF.get("counterexample_encoding")
        if ce:
            L.append("\nUnsound replay / cherry-pick counterexamples by writer encoding. " + ce["definition"] + "\n")
            for pol, s in ce["summary"].items():
                L.append(f"- {POLICY_LABEL.get(pol, pol)}: {s['cases']} cases; encoding-dependent: minimised "
                         f"{s['minimised_encoding_dependent']}, original {s['original_encoding_dependent']}; minimised cases "
                         f"without such an op: {', '.join(s['minimised_not_encoding_dependent']) or 'none'}; unclassified: "
                         f"{', '.join(s['unclassified']) or 'none'}.")
            rows = []
            for x in ce["cases"]:
                m = x.get("minimised") or {}
                rows.append([x["name"], POLICY_LABEL.get(x["policy"], x["policy"]), m.get("true_conflict"), m.get("layout"),
                             m.get("encoding_dependent"),
                             "; ".join(f"{h['kind']}#{h['index']} ({h['inserted_rows']} rows)"
                                       for h in m.get("source_append_only_cow_ops", [])) or "-",
                             (x.get("original") or {}).get("encoding_dependent")])
            L.append("")
            L.append(md_table(["Case", "Policy", "True conflict", "Layout", "Encoding-dependent (minimised)",
                               "Source append-only COW ops (minimised)", "Encoding-dependent (original)"], rows))
    L += second_source_markdown(S)
    L.append("\nReproduce:`cd <WORKSPACE> && .venv/bin/python 05_snapmerge_branch_merge/analyze.py`\n")
    return "\n".join(L) + "\n"


def second_source_markdown(S: dict) -> list[str]:
    """Section 18 (added 2026-10-04): the NYC TLC second-source differential test next to the TPC-H main campaign."""
    C = S["campaigns"]
    n, m = C.get(NYC), C.get(MAIN)
    if not n or not m:
        return []
    L = ["\n## 18. Second data source: NYC TLC yellow taxi 2023-01 (`difftest_main_nyc_v4`)\n"]
    cfg = n["config"]
    L.append(f"Source `{cfg['source']}`, months {', '.join(cfg['months'])}, partitioned by {cfg['partition'][1]}({cfg['partition'][0]}); "
             f"base {cfg['base_rows']} rows, {cfg['per_mix']} scenarios per mix, seed base {cfg['seed_base']}, the same 13 policies, "
             "mixes and generator as the TPC-H main campaign (scenarios pick four consecutive day partitions instead of month partitions). "
             f"{n['scenarios']} scenarios, {n['true_conflicts']} true conflicts "
             f"({100 * n['true_conflicts'] / n['scenarios']:.1f}%; TPC-H main {100 * m['true_conflicts'] / m['scenarios']:.1f}%).\n")
    o = n["oracle_checks"]
    L.append(f"Writer checks {o['writer_checks'] - o['writer_check_failures']}/{o['writer_checks']} pass "
             f"({o['writer_check_reader_disagreements']} reader disagreements); {o['emitted_merges_compared_with_both_orders_in_both_readers']} "
             f"of {o['emitted_merges_all_policies']} emitted merges (all policies) compared with both orders in both readers. "
             f"Load average (1 min): {n['loadavg_1m']}. Cross-check: {n['cross_check']['status']}.\n")
    # Table 1 (NYC)
    pols = ["snapmerge", "replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "nessie_force", "fast_forward"]
    rows = []
    for g in n["groups"]:
        r = [MIX_LABEL[g], _g(n, "snapmerge", g)["counts"]["n"], _g(n, "snapmerge", g)["counts"]["true_conflicts"],
             _g(n, "snapmerge", g)["counts"]["commuting"]]
        for p in pols:
            cnt = _g(n, p, g)["counts"]
            rec = _m(n, p, "conflict_recall", g)["value"]
            pre = _m(n, p, "conflict_precision", g)["value"]
            r.append(f"{rec:.3f} / {'-' if pre is None else f'{pre:.3f}'} / {cnt['soundness_violations']}")
        rows.append(r)
    L.append("### 18.1 Table 1 (NYC): recall / precision / unsound merges per mix\n")
    L.append(md_table(["Mix", "n", "True conf.", "Commuting"] + [POLICY_LABEL[p] for p in pols], rows))
    # Figure 1 data (NYC) next to TPC-H
    L.append("\n### 18.2 Sound auto-merge rate of commuting scenarios per mix, NYC vs TPC-H main (95% CI)\n")
    rows = []
    for g in n["groups"]:
        en = n["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        em = m["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        rows.append([MIX_LABEL[g], _g(n, "snapmerge", g)["counts"]["commuting"],
                     _ci(_m(n, "snapmerge", "sound_auto_merge_rate_of_commuting", g)),
                     _ci(_m(n, "replay", "sound_auto_merge_rate_of_commuting", g)),
                     _ci(_m(n, "cherry_pick", "sound_auto_merge_rate_of_commuting", g)),
                     _ci(_m(n, "rowlevel", "sound_auto_merge_rate_of_commuting", g)), _d(en),
                     _g(m, "snapmerge", g)["counts"]["commuting"], _ci(_m(m, "replay", "sound_auto_merge_rate_of_commuting", g)), _d(em)])
    L.append(md_table(["Mix", "NYC commuting", "NYC SnapMerge", "NYC replay", "NYC cherry-pick", "NYC row-level (sound only)",
                       "NYC SnapMerge - replay (paired)", "TPC-H commuting", "TPC-H replay", "TPC-H SnapMerge - replay (paired)"], rows))
    # pooled side by side
    L.append("\n### 18.3 All mixes pooled: NYC vs TPC-H main\n")
    rows = []
    for p in POLICY_ORDER:
        if p not in n["policies"]:
            continue
        cn, cm = _g(n, p)["counts"], _g(m, p)["counts"]
        rows.append([POLICY_LABEL[p], f"{cn['TP']}/{cn['FP']}/{cn['FN']}/{cn['TN']}", cn["soundness_violations"],
                     _ci(_m(n, p, "conflict_recall")), _ci(_m(n, p, "conflict_precision")),
                     _ci(_m(n, p, "sound_auto_merge_rate_of_commuting")), _ci(_m(n, p, "median_latency_ms_all"), 1),
                     f"{_m(n, p, 'mean_bytes_read')['value'] / 1e3:.1f}",
                     f"{cm['TP']}/{cm['FP']}/{cm['FN']}/{cm['TN']}", cm["soundness_violations"],
                     _ci(_m(m, p, "sound_auto_merge_rate_of_commuting"))])
    L.append(md_table(["Policy", "NYC TP/FP/FN/TN", "NYC unsound", "NYC recall", "NYC precision", "NYC sound auto-merge of commuting",
                       "NYC median latency ms", "NYC mean KB read", "TPC-H TP/FP/FN/TN", "TPC-H unsound",
                       "TPC-H sound auto-merge of commuting"], rows))
    # paired comparisons NYC
    L.append("\n### 18.4 Paired comparisons against SnapMerge (NYC, all mixes)\n")
    rows = []
    for p, cm_ in n["comparisons_vs_snapmerge"].items():
        e = cm_["all"]
        l1 = e["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_both_sound"]
        l2 = e["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_all"]
        rows.append([POLICY_LABEL.get(p, p), _d(e["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]),
                     _d(e["snapmerge_minus_policy_conflict_recall"]),
                     f"{_r(l1)} (n={l1['pairs']})" if l1.get("geomean_ratio") else "n/a",
                     f"{_r(l2)} (n={l2['pairs']})" if l2.get("geomean_ratio") else "n/a"])
    L.append(md_table(["Policy", "SnapMerge - policy: sound auto-merge of commuting", "SnapMerge - policy: recall",
                       "Latency policy/SnapMerge, both merged soundly (geomean)", "Latency policy/SnapMerge, all scenarios"], rows))
    nf = _g(n, "nessie_force")["counts"]
    L.append(f"\nNessie FORCE (NYC): {nf['soundness_violations']} of {nf['merged']} merges equal neither order; {nf['merges_with_row_loss']} lose rows "
             f"against both orders; {nf['rows_lost_vs_closest']} rows lost and {nf['rows_extra_vs_closest']} extra against the closer order.")
    rl = _g(n, "rowlevel")["counts"]
    L.append(f"Row-level (NYC): {rl['soundness_violations']} of {rl['merged']} merges equal neither order ({rl['unsound_merges_on_commuting']} on "
             f"commuting, {rl['unsound_merges_on_true_conflicts']} on true conflicts).\n")
    # ablation variants in NYC
    L.append("### 18.5 SnapMerge variants (NYC vs TPC-H main): full minus variant, paired\n")
    rows = []
    for v in ABLATIONS + ["snapmerge[filelevel]"]:
        en = n["comparisons_vs_snapmerge"][v]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        em = m["comparisons_vs_snapmerge"][v]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        rows.append([POLICY_LABEL[v], _ci(_m(n, v, "sound_auto_merge_rate_of_commuting")), _d(en), _g(n, v)["counts"]["soundness_violations"],
                     _ci(_m(m, v, "sound_auto_merge_rate_of_commuting")), _d(em)])
    L.append(md_table(["Variant", "NYC sound auto-merge of commuting", "NYC full - variant", "NYC unsound",
                       "TPC-H main sound auto-merge of commuting", "TPC-H main full - variant"], rows))
    E = S.get("environment") or {}
    w, mc = E.get("workload_nyc"), E.get("snapmerge_mechanisms_nyc")
    if w:
        L.append(f"\nNYC workload: {w['scenarios']} scenarios, {w['ops_total']} operations; operations per side min {w['ops_per_side_min']}, "
                 f"median {w['ops_per_side_median']}, max {w['ops_per_side_max']}. Operation kinds: "
                 + ", ".join(f"{k} {v}" for k, v in w["op_kinds"].items()) + ".")
    if mc:
        extra = {k: v for k, v in mc.items() if k.startswith("merges_") and k != "merges_with_hit"}
        L.append(f"SnapMerge mechanisms in the NYC campaign's {mc['merges']} merges: "
                 + ", ".join(f"{k} {v}" for k, v in mc["merges_with_hit"].items()) + "; "
                 + ", ".join(f"{k.replace('merges_', '')} {v}" for k, v in extra.items()) + ".\n")
    AF = S.get("audit_facts") or {}
    mo = AF.get("nyc_missed_conflicts_by_order") or {}
    if mo:
        keys = ["merged_true_conflicts", "equal_A_then_C_only", "equal_C_then_A_only", "equal_neither", "equal_both"]
        L.append("Missed conflicts (merges of true conflicts) in the NYC campaign by the serial order they equal:\n")
        L.append(md_table(["Policy"] + keys, [[POLICY_LABEL.get(p, p)] + [c.get(k, 0) for k in keys] for p, c in mo.items()]))
    lo = AF.get("nyc_latency_by_outcome") or {}
    if lo:
        L.append("\nMedian time to a decision by outcome in the NYC campaign (ms; n):\n")
        ocs = ["TP", "FP", "FN", "TN", "error"]
        L.append(md_table(["Policy"] + ocs, [[POLICY_LABEL.get(p, p)] + [(f"{d[o]['median_ms']} (n={d[o]['n']})" if d.get(o) else "")
                                                                         for o in ocs] for p, d in lo.items()]))
    cs = (AF.get("nyc_conflict_share") or {}).get("replay")
    if cs:
        L.append(f"\nReplay reports a conflict in {cs['conflicts']} of {cs['n']} NYC scenarios (share {cs['share']}).")
    sr = AF.get("nyc_snapmerge_latency_by_reads")
    if sr and sr.get("with_data_reads"):
        a, b = sr["without_data_reads"], sr["with_data_reads"]
        L.append(f"SnapMerge (NYC) without reads: median {a['median_ms']} ms (n={a['n']}); with reads: median {b['median_ms']} ms (n={b['n']}).")
    nd = [d for d in E.get("data", []) if str(d.get("path", "")).endswith("data/nyc") or d.get("path") == "data/nyc"]
    for d in nd:
        for fn, fv in d["files"].items():
            L.append(f"\nNYC data: {fn}: {fv['rows']:,} rows after cleaning ({fv.get('raw_rows'):,} raw, {fv.get('dropped_rows')} dropped), "
                     f"sha256 {fv['sha256']}.")
        for fn, fv in (d.get("raw_files") or {}).items():
            L.append(f"Raw {fn}: {fv['bytes']:,} bytes, sha256 {fv['sha256']}, retrieved {fv['retrieved']} from {fv['url']}. "
                     f"License: {d['license']}.")
    hh = next((h for h in S["hypotheses"] if h["id"] == "H16"), None)
    if hh:
        L.append(f"\nH16 ({hh['verdict'].upper()}): {hh['numbers']}.")
    return L


# ============================================================================ findings lists

def negative_findings(S: dict) -> list[str]:
    C = S["campaigns"]
    m, fb, sc = C[MAIN], C.get(FALLBACK), C.get(SCALE)
    out = []
    if fb:
        e = fb["comparisons_vs_snapmerge"]["replay"]["all"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        per = {g: fb["comparisons_vs_snapmerge"]["replay"][g]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"] for g in MIXES}
        lose = [MIX_LABEL[g] for g, x in per.items() if x["ci95"] and x["ci95"][1] < 0]
        win = [MIX_LABEL[g] for g, x in per.items() if x["ci95"] and x["ci95"][0] > 0]
        out.append(f"Without recorded filters and keys (file-level rules only), SnapMerge soundly auto-merges fewer commuting scenarios than replay: "
                   f"{_fmt_ci(_m(fb, 'snapmerge', 'sound_auto_merge_rate_of_commuting'))} vs {_fmt_ci(_m(fb, 'replay', 'sound_auto_merge_rate_of_commuting'))} "
                   f"(paired difference {_d(e)}). Replay pays for this with {_g(fb, 'replay')['counts']['FN']} missed true conflicts. "
                   f"By mix (paired 95% CI excluding 0), the file-level rules lose on {', '.join(lose) or 'none'} and win on "
                   f"{', '.join(win) or 'none'} (Section 9).")
    rl = _g(m, "rowlevel")["counts"]
    out.append(f"Row-level three-way merge is not a sound upper bound: {rl['soundness_violations']} of {rl['merged']} main-campaign merges equal neither "
               f"serial order ({rl['unsound_merges_on_commuting']} on commuting scenarios), and its recall is "
               f"{_m(m, 'rowlevel', 'conflict_recall')['value']:.3f}.")
    pd_ = m["comparisons_vs_snapmerge"]["replay"]["partition_disjoint_ingest"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
    out.append(f"No advantage on partition-disjoint ingest: SnapMerge and replay both reach "
               f"{_m(m, 'snapmerge', 'sound_auto_merge_rate_of_commuting', 'partition_disjoint_ingest')['value']:.3f} (difference {pd_['value']:+.3f}). "
               f"On filter corrections the advantage is only {m['comparisons_vs_snapmerge']['replay']['filter_corrections']['snapmerge_minus_policy_sound_auto_merge_rate_of_commuting']['value']:+.3f}.")
    l2 = m["comparisons_vs_snapmerge"]["replay"]["all"]["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_all"]
    l1 = m["comparisons_vs_snapmerge"]["replay"]["all"]["geomean_ratio_policy_over_snapmerge_latency_vs_snapmerge_both_sound"]
    out.append(f"In the main campaign SnapMerge's time to decision is higher than replay's: median "
               f"{_m(m, 'snapmerge', 'median_latency_ms_all')['value']:.1f} ms vs {_m(m, 'replay', 'median_latency_ms_all')['value']:.1f} ms; "
               f"replay/SnapMerge geometric-mean ratio {_r(l2)} over all scenarios and {_r(l1)} "
               f"on the {l1['pairs']} scenarios both merged soundly. Replay often stops early with a (false) conflict. SnapMerge also reads "
               f"{_m(m, 'snapmerge', 'mean_bytes_read')['value'] / 1e3:.1f} KB of data per scenario on average, and replay reads none.")
    if sc:
        g10 = [g for g in sc["groups"] if g["sf"] == 10 and g["kind"] == "overlap"]
        s10 = next(g for g in g10 if g["policy"] == "snapmerge")
        r10 = next(g for g in g10 if g["policy"] == "replay")
        out.append(f"At SF 10, a SnapMerge overlap merge (compaction vs delete+append in one partition) takes {s10['latency_ms']['median']:.0f} ms "
                   f"(median; range {s10['latency_ms']['min']:.0f}-{s10['latency_ms']['max']:.0f}) and reads "
                   f"{s10['bytes_read_data_delete']['median'] / 1e6:.1f} MB (range {s10['bytes_read_data_delete']['min'] / 1e6:.1f}-"
                   f"{s10['bytes_read_data_delete']['max'] / 1e6:.1f} MB, depending on the seeded partition). Replay answers in "
                   f"{r10['latency_ms']['median']:.0f} ms, but only with a false conflict. The bounded-read cost grows with partition size.")
        rp = [e for e in sc["paired"] if e["other"] == "replay" and e["kind"] == "disjoint"]
        unres = [f"SF {e['sf']:g}" for e in rp if e["ci95_paired_bootstrap"][0] <= 1 <= e["ci95_paired_bootstrap"][1]]
        if unres:
            out.append("SnapMerge's disjoint-merge latency advantage over replay is not resolved at " + ", ".join(unres) +
                       f" (paired 95% interval of the replay/SnapMerge ratio includes 1 with {rp[0]['pairs']} reps).")
        peak = max(g["process_peak_rss_gb_max"] for g in sc["groups"])
        out.append(f"The plan's memory estimate was too low: the scale runner (oracle checks included) peaks at {peak:.1f} GB process RSS "
                   "at SF 10, against a plan estimate of at most 16 GB.")
        iso = {}
        for g in sc["groups"]:
            if g["sf"] == 10 and g.get("isolated_peak_rss_gb"):
                cur = iso.setdefault(g["policy"], [0.0, 0.0])
                cur[0] = max(cur[0], g["isolated_peak_rss_gb"]["max"])
                cur[1] = max(cur[1], (g.get("isolated_peak_temp_bytes") or {}).get("max", 0.0) / 1e9)
        if iso:
            out.append("Isolated merge processes at SF 10 (max over reps and both shapes): " + "; ".join(
                f"{POLICY_LABEL[k]} {v[0]:.1f} GB peak RSS, {v[1]:.1f} GB DuckDB temporary files" for k, v in sorted(iso.items()))
                + ". The row-level baseline's spill exceeds the plan's 60 GB disk estimate.")
    nb = C.get(ABLATION)
    if nb:
        e = _m(nb, "snapmerge[no_bounded_reads]", "sound_auto_merge_rate_of_commuting")
        out.append(f"Without bounded reads, SnapMerge stays close to replay ({_fmt_ci(e)} vs {_fmt_ci(_m(nb, 'replay', 'sound_auto_merge_rate_of_commuting'))} "
                   "in ablation_v4): most of the advantage needs data reads, so the merge is not metadata-only in those cases.")
    out.append(f"Replay silently merges {_g(m, 'replay')['counts']['FN']} true conflicts in the main campaign (it picks one serial order without "
               f"reporting the conflict). Iterated cherry-pick misses {_g(m, 'cherry_pick')['counts']['FN']}.")
    out.append("Known SnapMerge limitation (fails closed): a second merge between the same branches is unsupported (F9). Since the "
               "revision, a commit on another branch (including the source) during a merge is re-sequenced and retried, and a moved "
               "target returns 'retryable' (F10; workflow_v2 races).")
    out.append("The difftest runner's RSS grows over a campaign (8.1 GB guard peak over 3,003 scenarios in the submitted version's "
               "Experiment stage), probably from catalog engines that are never released.")
    ws = C.get("writer_single_v2")
    if ws and "snapmerge[append_rule]" in ws["policies"]:
        ar = _g(ws, "snapmerge[append_rule]")["counts"]
        out.append(f"The submitted version's insert rule (inserts only from append snapshots, variant snapmerge[append_rule]) is unsound "
                   f"for a writer that commits each copy-on-write operation as one snapshot: with writer_single_v2 it merged "
                   f"{ar['FN']} true conflicts, {ar['soundness_violations']} of them into a result equal to neither order "
                   f"(the revised SnapMerge: 0).")
    cu = C.get("contended_unpart_v2")
    if cu:
        cg = {g["policy"]: g for g in cu["groups"]}
        if "snapmerge" in cg and "recompute" in cg:
            a, b = cg["snapmerge"], cg["recompute"]
            out.append(f"When the whole table is contended (unpartitioned SF 1, contended_unpart_v2), SnapMerge's composition takes "
                       f"{a['latency_ms']['median'] / 1e3:.1f} s and {a['isolated_policy_rss_increment_gb']['median']:.1f} GB above its "
                       f"baseline, more than recomputing both orders from the base ({b['latency_ms']['median'] / 1e3:.1f} s, "
                       f"{b['isolated_policy_rss_increment_gb']['median']:.1f} GB).")
    if C.get(NYC):
        la = C[NYC]["loadavg_1m"]
        out.append(f"The NYC campaign ran under a heavier host load (1-min load average median {la['median']:.0f}, max {la['max']:.0f}, "
                   f"32 logical CPUs) than the TPC-H main campaign (median {m['loadavg_1m']['median']:.0f}); its latencies are not "
                   "comparable with TPC-H's.")
    n = C.get(NYC)
    if n:  # second data source (2026-10-04)
        pdn = n["comparisons_vs_snapmerge"]["replay"]["partition_disjoint_ingest"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        fcn = n["comparisons_vs_snapmerge"]["replay"]["filter_corrections"]["snapmerge_minus_policy_sound_auto_merge_rate_of_commuting"]
        rln = _g(n, "rowlevel")["counts"]
        out.append(f"The second source (NYC TLC 2023-01) repeats the weak spots: on partition-disjoint ingest the advantage over replay is "
                   f"{_d(pdn)}, on filter corrections {_d(fcn)} (only {_g(n, 'snapmerge', 'filter_corrections')['counts']['commuting']} "
                   f"commuting scenarios); replay silently merges {_g(n, 'replay')['counts']['FN']} true conflicts and row-level merge "
                   f"emits {rln['soundness_violations']} merges equal to neither order. The NYC scale points were not run.")
    return out


def _load_range(S: dict) -> str:
    lo, hi = [], []
    for c in S["campaigns"].values():
        la = c.get("loadavg_1m")
        if isinstance(la, dict) and la.get("min") is not None:
            lo.append(la["min"]); hi.append(la["max"])
        for g in c.get("groups") or []:
            gl = g.get("loadavg_1m") if isinstance(g, dict) else None
            if isinstance(gl, dict) and gl.get("min") is not None:
                lo.append(gl["min"]); hi.append(gl["max"])
    return f"{min(lo):.0f}-{max(hi):.0f}" if lo else "not recorded"


def does_not_show(S: dict) -> list[str]:
    C = S["campaigns"]
    m = C[MAIN]
    n = _g(m, "snapmerge")["counts"]["merged"]
    ub = clopper_pearson(0, n)[1]
    ny = C.get(NYC)
    if ny:
        src_item = (f"It covers a second data source only for the differential test and only under the same generator. "
                    f"{NYC} ({ny['scenarios']} scenarios, NYC TLC yellow taxi 2023-01, four consecutive day partitions per scenario) "
                    "replicates the main campaign; the NYC scale points (1/3/12 months) were not run because only the 2023-01 download "
                    "was approved, so every cost and scale result and the ablation, fallback, sensitivity, single-snapshot-writer and "
                    "partial-metadata campaigns are on TPC-H lineitem only.")
        share = (f"The {100 * m['true_conflicts'] / m['scenarios']:.0f}% (TPC-H) and {100 * ny['true_conflicts'] / ny['scenarios']:.0f}% (NYC) "
                 "shares of true conflicts are properties of the frozen scenario generator ")
    else:
        src_item = ("It does not cover a second data source. The NYC TLC half of difftest_main and the NYC scale points were not run (the download "
                    "needs approval), so every difftest result is on TPC-H lineitem (SF 0.01 pilot, SF 0.1 otherwise) with month partitions.")
        share = "The 55% share of true conflicts is a property of the frozen scenario generator "
    return [
        f"It does not prove soundness. Zero violations in {n} main-campaign merges bound the per-merge violation rate only for this "
        f"generator (exact 95% upper bound {ub:.5f}). The proof of the theorem is separate.",
        src_item,
        "It does not measure real-world conflict rates. " + share +
        "(4 partitions per scenario, deletes mixes dominated by APPEND x DELETE; F8). GitLake reports about 10 conflicts per 100k attempts "
        "at table granularity. The nine one-factor sensitivity campaigns (section 21) vary partition span, history length, "
        "selectivity and compaction frequency under the same generator; real branch histories are not covered.",
        "It does not show production performance. Prototype Python/PyIceberg code ran on a shared host (1-min load averages "
        f"{_load_range(S)} across the accepted campaigns, 32 logical CPUs). Difftest latencies are single-process wall clock with a "
        "seeded random policy order per scenario and in-process caches cleared; scale latencies come from a fresh process per timed "
        "run with a warm page cache, in random order.",
        "It does not compare against upstream implementations. Replay implements the Iceberg #13959 proposal as we read it (the "
        "proposal was never merged). lakeFS and Nessie are emulations of their documented merge semantics, and row-level merge is our "
        "own DuckDB implementation in the Decibel/Dolt style.",
        "It does not show readability by Spark or Trino. Only DuckDB 1.5.6 (iceberg extension) and PyIceberg 0.12.0 read the snapshots.",
        "It does not cover equality deletes, deletion vectors, schema or partition-spec evolution, repeated merges between the same "
        "branches (F9); they are unsupported and fail closed. Concurrent commits are covered only by the injected races of workflow_v2.",
        "It does not show scale behaviour beyond TPC-H SF 10 (60M rows, 2,520 files), with many branches, or with workloads other "
        "than the fixed scale shapes (disjoint, overlap, contended-K, history-L). Scale points have 10 reps (3 for row-level and "
        "recompute), so the heavy policies' intervals are wide.",
        "Its metadata byte counts are approximate (F12: about 0.05% run-to-run variation). Per-policy memory comes from isolated child "
        "processes (peak minus baseline RSS, sampled), not from a kernel-enforced measurement.",
        "Minimised counterexamples are minimal only down to one base row, because the minimiser never removes the last one. The unsound "
        "replay cases come from the review fuzzer's tiny value domains: the main campaign has 0 unsound replay merges.",
        "The per-mix 95% intervals are bootstrap intervals over 429 (main, NYC), 215 (ablation), 143 (fallback, single-snapshot writer, "
        "partial metadata) or 60 (sensitivity) scenarios per mix. Intervals at a rate of "
        "exactly 0 or 1 are degenerate in the bootstrap; use the exact intervals given beside them.",
    ]


# ============================================================================ figures (from summary.json only)

PAL = {"snapmerge": "#2a78d6", "replay": "#eb6834", "rowlevel": "#1baf7a", "cherry_pick": "#4a3aa7",
       "recompute": "#4a3aa7"}  # recompute and cherry-pick never share a figure
GREY = "#8a8984"
INK = "#1f1f1e"
INK2 = "#52514e"
MARK = {"snapmerge": "o", "replay": "s", "cherry_pick": "^", "rowlevel": "D", "recompute": "v"}
COL_W = 3.33


def _relpath(p: Path) -> str:
    return str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else str(p)


def _mpl():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({
        "pdf.fonttype": 42, "ps.fonttype": 42, "font.family": "sans-serif",
        "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"], "font.size": 7, "axes.labelsize": 7,
        "axes.titlesize": 7, "xtick.labelsize": 6.5, "ytick.labelsize": 6.5, "legend.fontsize": 6.5,
        "axes.linewidth": 0.6, "xtick.major.width": 0.6, "ytick.major.width": 0.6, "xtick.major.size": 2.5,
        "ytick.major.size": 2.5, "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2,
        "ytick.color": INK2, "text.color": INK, "axes.spines.top": False, "axes.spines.right": False,
        "grid.color": "#e4e3df", "grid.linewidth": 0.5, "legend.frameon": False, "savefig.bbox": None,
        "figure.dpi": 150,
    })
    return plt


def fig_automerge(S: dict, out: Path) -> dict:
    plt = _mpl()
    m = S["campaigns"][MAIN]
    groups = list(reversed(m["groups"]))  # "all" at the bottom
    pols = ["snapmerge", "replay", "cherry_pick", "rowlevel"]
    nyc = S["campaigns"].get(NYC)
    off = {"snapmerge": 0.3, "replay": 0.1, "cherry_pick": -0.1, "rowlevel": -0.3}  # Finalize: wider dodge
    if nyc:  # 2026-10-04: replay on the NYC second source as hollow squares between replay and cherry-pick
        off = {"snapmerge": 0.32, "replay": 0.16, "replay_nyc": 0.0, "cherry_pick": -0.16, "rowlevel": -0.32}
    fig, ax = plt.subplots(figsize=(COL_W, 2.85))
    fig.subplots_adjust(left=0.335, right=0.975, bottom=0.135, top=0.865)
    for gi, g in enumerate(groups):
        if g == "all":
            ax.axhspan(gi - 0.45, gi + 0.45, color="#f0efec", zorder=0, lw=0)
    for p in pols:
        ys, xs, lo, hi = [], [], [], []
        for gi, g in enumerate(groups):
            e = m["policies"][p]["groups"][g]["metrics"]["sound_auto_merge_rate_of_commuting"]
            ys.append(gi + off[p])
            xs.append(100 * e["value"])
            lo.append(100 * (e["value"] - e["ci95"][0]))
            hi.append(100 * (e["ci95"][1] - e["value"]))
        ax.errorbar(xs, ys, xerr=[lo, hi], fmt=MARK[p], ms=3.6, color=PAL[p], mec="white", mew=0.4,
                    elinewidth=0.8, capsize=0, label=POLICY_LABEL[p] + (" (sound merges)" if p == "rowlevel" else "")
                    + (", TPC-H" if nyc and p == "replay" else ""),
                    zorder=4 if p == "snapmerge" else 3)
        if nyc and p == "replay":
            ys, xs, lo, hi = [], [], [], []
            for gi, g in enumerate(groups):
                e = nyc["policies"][p]["groups"][g]["metrics"]["sound_auto_merge_rate_of_commuting"]
                ys.append(gi + off["replay_nyc"])
                xs.append(100 * e["value"])
                lo.append(100 * (e["value"] - e["ci95"][0]))
                hi.append(100 * (e["ci95"][1] - e["value"]))
            ax.errorbar(xs, ys, xerr=[lo, hi], fmt=MARK[p], ms=3.4, color=PAL[p], mfc="white", mec=PAL[p], mew=0.8,
                        elinewidth=0.8, capsize=0, label=POLICY_LABEL[p] + ", NYC taxi", zorder=3)
    ax.set_yticks(range(len(groups)))
    short = {"partition_disjoint_ingest": "Disjoint ingest", "filter_corrections": "Filter corrections",
             "upsert_keys": "Upsert by key", "deletes_mor": "Deletes (MOR)", "deletes_cow": "Deletes (COW)",
             "compaction_on_main": "Compaction", "adversarial_overlap": "Adversarial", "all": "All mixes"}
    ax.set_yticklabels([f"{short[g]} (n={m['policies']['snapmerge']['groups'][g]['counts']['commuting']})" for g in groups])
    ax.set_ylim(-0.55, len(groups) - 0.45)
    ax.set_xlim(-3, 103)
    ax.set_xticks([0, 20, 40, 60, 80, 100])
    ax.set_xlabel("Sound auto-merge rate of commuting scenarios (%)")
    ax.grid(axis="x", zorder=0)
    ax.tick_params(axis="y", length=0)
    hh, ll = ax.get_legend_handles_labels()
    if nyc:  # legend order: SnapMerge, replay TPC-H, replay NYC, cherry-pick, row-level (two columns, column-major)
        fig.subplots_adjust(top=0.835)
    fig.legend(hh, ll, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=2, handletextpad=0.3,
               columnspacing=0.8, borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    cap = ("Figure 1. Share of commuting scenarios each policy merges soundly, per mix "
           "(difftest_main_tpch_v4, TPC-H SF 0.1, 3,003 scenarios). Whiskers: 95% bootstrap intervals over scenarios. n = commuting scenarios. "
           "Row-level counts only its serial-equivalent merges.")
    if nyc:
        cap += (f" Hollow squares: replay on the NYC taxi source ({NYC}, {nyc['scenarios']} scenarios); SnapMerge merges 100% of the NYC "
                "commuting scenarios as well. n counts the TPC-H commuting scenarios.")
    return {"file": _relpath(out), "caption": cap}


def fig_scale(S: dict, out: Path, metric: str) -> dict:
    plt = _mpl()
    sc = S["campaigns"][SCALE]
    fig, axes = plt.subplots(1, 2, figsize=(COL_W, 2.25), sharey=True)
    # 2026-10-08: more headroom so the legend's last row does not touch the panel titles
    fig.subplots_adjust(left=0.155, right=0.985, bottom=0.167, top=0.738, wspace=0.08)
    for ax, kind, title in zip(axes, ("disjoint", "overlap"), ("Disjoint partitions", "Overlapping partition")):
        for p in ("rowlevel", "recompute", "replay", "snapmerge"):
            rows = sorted((g for g in sc["groups"] if g["kind"] == kind and g["policy"] == p and g.get("partitioned", True)),
                          key=lambda g: g["sf"])
            if not rows:
                continue
            xs = [g["sf"] for g in rows]
            conflict = all(set(g["verdicts"]) == {"conflict"} for g in rows)
            if metric == "latency":
                ys = [g["latency_ms"]["mean"] for g in rows]  # ms
                lo = [max(1e-9, g["latency_ms"]["mean"] - max(g["latency_ms"]["ci95_t"][0], g["latency_ms"]["min"])) for g in rows]
                hi = [g["latency_ms"]["ci95_t"][1] - g["latency_ms"]["mean"] for g in rows]
                yerr = [lo, hi]
            else:  # MB
                ys = [g["bytes_read_total"]["mean"] / 1e6 for g in rows]
                yerr = [[(g["bytes_read_total"]["mean"] - max(g["bytes_read_total"]["ci95_t"][0], g["bytes_read_total"]["min"])) / 1e6
                         for g in rows],
                        [(g["bytes_read_total"]["ci95_t"][1] - g["bytes_read_total"]["mean"]) / 1e6 for g in rows]]
            lab = {"snapmerge": "SnapMerge", "replay": "Replay (#13959)", "rowlevel": "Row-level 3-way",
                   "recompute": "Recompute (reference)"}[p] + \
                (": false conflict" if conflict else "")
            ax.errorbar(xs, ys, yerr=yerr, fmt=MARK[p] + ("--" if conflict else "-"), color=PAL[p], ms=3.4,
                        mfc="white" if conflict else PAL[p], mec=PAL[p], mew=0.8, lw=1.0, elinewidth=0.8, capsize=1.5, label=lab, zorder=3)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xticks([0.1, 1, 10])
        ax.set_xticklabels(["0.1", "1", "10"])
        ax.minorticks_off()
        ax.set_title(title, pad=3)
        ax.grid(axis="y", zorder=0)
        ax.set_xlim(0.07, 14)
    fig.supxlabel("TPC-H scale factor (lineitem: 0.6M, 6M, 60M rows)", fontsize=7, y=0.0, va="bottom")
    axes[0].set_ylabel("Merge latency (ms)" if metric == "latency" else "Data + metadata read (MB)")
    h, l = [], []
    for ax in axes:
        for hh, ll in zip(*ax.get_legend_handles_labels()):
            if ll not in l:
                h.append(hh)
                l.append(ll)
    keys = ["SnapMerge", "Replay", "Row", "Recompute"]
    order = sorted(range(len(l)), key=lambda i: keys.index(next(k for k in keys if l[i].startswith(k))) * 2 + ("conflict" in l[i]))
    fig.legend([h[i] for i in order], [l[i] for i in order], loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=2,
               handletextpad=0.3, columnspacing=0.8, borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    if metric == "latency":
        reps = sorted({g["reps"] for g in sc["groups"]})
        cap = (f"Figure 2. Merge latency versus table size ({SCALE}; TPC-H lineitem SF 0.1/1/10 = 0.6M/6M/60M rows). Markers: mean "
               f"of seeded repetitions ({'/'.join(map(str, reps))} per point); whiskers: Student-t 95% intervals (clipped at the observed "
               "minimum). Every timed run is a fresh process with a warm page cache, in seeded random order. Hollow markers, dashed: "
               "replay reports a (false) partition conflict on the overlap scenario, so its time is the time to the conflict.")
    else:
        cap = ("Figure 4. Bytes read per merge (data, delete and metadata files) versus table size (scale_v5_tpch). Mean over repetitions with "
               "t 95% intervals. SnapMerge and replay read only metadata on disjoint merges, and SnapMerge reads only the contended "
               "partition on overlap. Row-level merge reads the whole table.")
    return {"file": _relpath(out), "caption": cap}


def fig_ablation(S: dict, out: Path) -> dict:
    plt = _mpl()
    a = S["campaigns"][ABLATION]
    m = S["campaigns"][MAIN]
    vs = ["snapmerge"] + ABLATIONS + ["snapmerge[filelevel]", "replay"]
    lab = {"snapmerge": "SnapMerge (all rules)", "snapmerge[no_net_effect]": "w/o net-effect test",
           "snapmerge[no_compaction_rebase]": "w/o compaction rebase", "snapmerge[no_delete_union]": "w/o delete-file union",
           "snapmerge[no_bounded_reads]": "w/o bounded reads", "snapmerge[filelevel]": "file-level rules only",
           "replay": "Replay (#13959)"}
    vs = list(reversed(vs))
    fig, ax = plt.subplots(figsize=(COL_W, 2.1))
    fig.subplots_adjust(left=0.335, right=0.975, bottom=0.18, top=0.88)
    nyc = S["campaigns"].get(NYC)
    camps = [(a, "a", "filled", 0.13, "ablation_v4 (1,505 scenarios)"), (m, "m", "hollow", -0.13, "difftest_main_tpch_v4 (3,003)")]
    if nyc:  # 2026-10-04: the same variants on the NYC second source (diamonds)
        camps = [(a, "a", "filled", 0.2, ABLATION), (m, "m", "hollow", 0.0, "main TPC-H"), (nyc, "n", "diamond", -0.2, "NYC taxi")]
    for camp, c, style, dy, name in camps:
        for i, v in enumerate(vs):
            if v not in camp["policies"]:
                continue
            e = camp["policies"][v]["groups"]["all"]["metrics"]["sound_auto_merge_rate_of_commuting"]
            col = PAL["replay"] if v == "replay" else PAL["snapmerge"]
            ax.errorbar([100 * e["value"]], [i + dy], xerr=[[100 * (e["value"] - e["ci95"][0])], [100 * (e["ci95"][1] - e["value"])]],
                        fmt="D" if style == "diamond" else "o", ms=3.1 if style == "diamond" else 3.6, color=col,
                        mfc=col if style == "filled" else "white", mec=col, mew=0.8,
                        elinewidth=0.8, capsize=0, zorder=3, label=name if i == len(vs) - 1 else None)
    ax.set_yticks(range(len(vs)))
    ax.set_yticklabels([lab[v] for v in vs])
    ax.set_xlim(30, 103)
    ax.set_ylim(-0.55, len(vs) - 0.45)
    ax.set_xlabel("Sound auto-merge rate of commuting scenarios (%)")
    ax.grid(axis="x", zorder=0)
    ax.tick_params(axis="y", length=0)
    from matplotlib.lines import Line2D

    if nyc:
        hd = [Line2D([], [], marker="o", ls="", color=INK2, mfc=INK2, ms=3.6, label="ablation (1,505)"),
              Line2D([], [], marker="o", ls="", color=INK2, mfc="white", mew=0.8, ms=3.6, label="main TPC-H (3,003)"),
              Line2D([], [], marker="D", ls="", color=INK2, mfc="white", mew=0.8, ms=3.1, label=f"NYC taxi ({nyc['scenarios']:,})")]
        ncol = 3
    else:
        hd = [Line2D([], [], marker="o", ls="", color=INK2, mfc=INK2, ms=3.6, label="ablation_v4 (1,505 scenarios)"),
              Line2D([], [], marker="o", ls="", color=INK2, mfc="white", mew=0.8, ms=3.6, label="main campaign (3,003)")]
        ncol = 2
    fig.legend(handles=hd, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=ncol, handletextpad=0.2, columnspacing=0.8,
               borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    cap = ("Figure 3. Ablation of SnapMerge's rule families: sound auto-merge rate of "
           "commuting scenarios with one family disabled (blue) versus replay (orange). Filled: ablation_v4 (seed base 20261003); hollow: "
           "the same variants in the main campaign (seed base 20261002)")
    cap += (f"; diamonds: the same variants on the NYC taxi source ({NYC}, seed base 20261002). " if nyc else ". ")
    cap += "Whiskers: 95% bootstrap intervals. Every variant stays sound with recall 1.0."
    return {"file": _relpath(out), "caption": cap}


def fig_detection(S: dict, out: Path) -> dict:
    plt = _mpl()
    m = S["campaigns"][MAIN]
    pols = ["snapmerge", "replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "fast_forward"]
    short = {"snapmerge": "SnapMerge", "replay": "Replay (#13959)", "cherry_pick": "Cherry-pick", "rowlevel": "Row-level 3-way",
             "lakefs": "lakeFS", "nessie_normal": "Nessie NORMAL, fast-forward", "fast_forward": None}
    fig, ax = plt.subplots(figsize=(COL_W, 2.3))
    fig.subplots_adjust(left=0.135, right=0.975, bottom=0.165, top=0.975)
    for p in pols:
        r = m["policies"][p]["groups"]["all"]["metrics"]["conflict_recall"]
        q = m["policies"][p]["groups"]["all"]["metrics"]["conflict_precision"]
        col = PAL.get(p, GREY)
        ax.errorbar([100 * r["value"]], [100 * q["value"]],
                    xerr=[[100 * (r["value"] - r["ci95"][0])], [100 * (r["ci95"][1] - r["value"])]],
                    yerr=[[100 * (q["value"] - q["ci95"][0])], [100 * (q["ci95"][1] - q["value"])]],
                    fmt=MARK.get(p, "o"), color=col, ms=3.8 if p in PAL else 3.0, mec="white", mew=0.4, elinewidth=0.7, capsize=0, zorder=3)
        if short[p]:
            dx, dy, ha, va = {"snapmerge": (-2.0, 0, "right", "center"), "replay": (-2.0, 0, "right", "center"),
                              "cherry_pick": (-1.0, -2.4, "right", "top"), "rowlevel": (2.2, 0, "left", "center"),
                              "lakefs": (-2.5, 13.0, "right", "center"), "nessie_normal": (-2.5, -8.0, "right", "center")}[p]
            lead = dict(arrowstyle="-", lw=0.5, color=GREY, shrinkA=1.5, shrinkB=2.5) if p in ("lakefs", "nessie_normal") else None
            ax.annotate(short[p], (100 * r["value"], 100 * q["value"]), xytext=(100 * r["value"] + dx, 100 * q["value"] + dy),
                        ha=ha, va=va, fontsize=6.3, color=INK, arrowprops=lead)
    ax.set_xlabel("Conflict recall (% of true conflicts reported)")
    ax.set_ylabel("Conflict precision (%)")
    ax.set_xlim(30, 103)
    ax.set_ylim(40, 103)
    ax.grid(zorder=0)
    fig.savefig(out)
    plt.close(fig)
    nf = m["policies"]["nessie_force"]["groups"]["all"]["counts"]
    return {"file": _relpath(out), "caption": "Figure 5. Conflict detection quality per policy in the main campaign "
            "(1,662 true conflicts among 3,003 scenarios). Whiskers: 95% bootstrap intervals. Nessie NORMAL and fast-forward overlap "
            "at recall 100%, because they report a conflict whenever both sides changed the table. Nessie FORCE never reports a conflict "
            f"(recall 0) and is omitted; {nf['soundness_violations']} of its {nf['merged']} merges equal neither serial order (Table 2)."}


def make_figures(summary_path: Path, figdir: Path) -> list[dict]:
    S = json.loads(summary_path.read_text())  # figures use the aggregated data only
    figdir.mkdir(parents=True, exist_ok=True)
    figs = [fig_automerge(S, figdir / "fig1_automerge_by_mix.pdf")]
    if SCALE in S["campaigns"]:
        figs.append(fig_scale(S, figdir / "fig2_scale_latency.pdf", "latency"))
    if ABLATION in S["campaigns"]:
        figs.append(fig_ablation(S, figdir / "fig3_ablation.pdf"))
    if SCALE in S["campaigns"]:
        figs.append(fig_scale(S, figdir / "fig4_scale_bytes_read.pdf", "bytes"))
    figs.append(fig_detection(S, figdir / "fig5_conflict_detection.pdf"))
    import analysis_revision
    figs += analysis_revision.figures(S, figdir)
    return figs


# ============================================================================ main

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--boot", type=int, default=B_DEFAULT)
    ap.add_argument("--seed", type=int, default=SEED_DEFAULT)
    ap.add_argument("--out-dir", default=str(RESULTS), help="where summary.json and analysis.md go (inputs are always results/)")
    ap.add_argument("--figdir", default=str(FIGDIR))
    ap.add_argument("--figures-only", action="store_true")
    a = ap.parse_args(argv)
    summary_path = Path(a.out_dir) / "summary.json"
    md_path = Path(a.out_dir) / "analysis.md"
    if a.figures_only:
        figs = make_figures(summary_path, Path(a.figdir))
        print(json.dumps(figs, indent=1))
        return 0
    t0 = time.time()
    gate = run_gate()
    campaigns = {}
    for name, g in gate.items():
        if g["status"] not in ("accepted", "partial") or not g["accepted"]:
            continue
        if g["runner"] == "difftest":
            c = analyse_difftest(name, g["accepted"], a.boot, a.seed)
            sc, pols, policies = c.pop("_cross_check_inputs")
            c["cross_check"] = cross_check_with_runner(name, sc, pols, policies, c["policies"])
        elif g["runner"] == "scale":
            c = analyse_scale(name, g["accepted"], a.boot, a.seed)
        elif g["runner"] in ("enumerator", "workflow"):  # analysed in analysis_revision from the raw records
            c = {"role": ROLES.get(name), "runner": g["runner"], "records": len(g["accepted"]),
                 "code_fingerprint": g["recorded_code_fingerprint"]}
        else:
            c = analyse_counterexamples(name, g["accepted"])
        c["name"] = name
        campaigns[name] = c
        print(f"analysed {name}: {len(g['accepted'])} records ({time.time() - t0:.1f} s, peak RSS {peak_rss_gb()} GB)", flush=True)
    order = [n for n in ROLES if n in campaigns] + [n for n in campaigns if n not in ROLES]
    S = {"generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "params": {"B": a.boot, "seed": a.seed},
         "gate": {n: gate_json(g) for n, g in gate.items()},
         "campaigns": {n: campaigns[n] for n in order}}
    lenient = os.environ.get("SNAPMERGE_ANALYSIS_LENIENT") == "1"  # test mode only (tiny smoke campaigns)

    def safe(fn, *args):
        try:
            return fn(*args)
        except Exception as e:
            if not lenient:
                raise
            print(f"LENIENT: {fn.__name__} failed: {e!r}", flush=True)
            return {"error": repr(e)}

    S["hypotheses"] = safe(hypotheses, S)
    S["negative_findings"] = safe(negative_findings, S)
    S["does_not_show"] = safe(does_not_show, S)
    S["telemetry"] = {"guard": guard_telemetry(), "analysis_seconds": None, "analysis_peak_rss_gb": None}
    S["environment"] = safe(environment_facts, S, gate)
    S["audit_facts"] = safe(audit_facts, S, gate)
    import analysis_revision
    S["revision"] = analysis_revision.run(S, gate, a.boot, a.seed)
    S["figures"] = []
    atomic_write(summary_path, json.dumps(S, indent=1, sort_keys=False, default=str) + "\n")
    figs = safe(make_figures, summary_path, Path(a.figdir))
    figs = figs if isinstance(figs, list) else []
    S["figures"] = figs
    S["telemetry"]["analysis_seconds"] = round(time.time() - t0, 1)
    S["telemetry"]["analysis_peak_rss_gb"] = peak_rss_gb()
    atomic_write(summary_path, json.dumps(S, indent=1, sort_keys=False, default=str) + "\n")
    import analysis_revision
    md = safe(write_markdown, S)
    atomic_write(md_path, (md if isinstance(md, str) else f"(legacy sections failed: {md})\n")
                 + "\n".join(analysis_revision.markdown(S)) + "\n")
    bad = [n for n, c in campaigns.items() if c.get("cross_check", {}).get("status") == "MISMATCH"]
    print(f"wrote {summary_path} and {md_path}; figures: {[f['file'] for f in figs]}; cross-check mismatches in: {bad or 'none'}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
