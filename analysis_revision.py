"""Revision analyses (2026-10-05, review package R1-R4 / G1-G5 / F1-F10).

Called by analyze.main() after the per-campaign analyses; reads only gate-accepted raw records (plus,
for the explicitly labelled v2-vs-v3 regression comparison, the superseded v2 main records).  Output:
summary.json["revision"] and analysis.md sections 19-30; figures fig6-fig8.

  contract      outcome categories per policy under both contracts: merges equal to both serial orders,
                to the designated rebase order (A;C) only, to (C;A) only, to neither, and rejections,
                split by commuting / true-conflict scenarios (R3 MC2)
  mechanisms    information-matched controls on identical scenarios: paired differences of the sound
                auto-merge rate of commuting scenarios (bootstrap stratified by mix): recorded intent
                (replay_intent - replay), the exact net-effect test under replay (replay_intent_exact -
                replay_intent), physical composition under pairwise and exact decisions (snapmerge[no_net_
                effect] - replay_intent, snapmerge - replay_intent_exact) (R1 MC1, R3 MC1, G2)
  sensitivity   rates and paired advantages per knob level (n_parts, hist, sel, compact_p) (R3 MC3, G3)
  writer_single the single-snapshot (foreign) writer: SnapMerge vs the submitted append-snapshot rule
                vs replay under that encoding (F1)
  provenance    provenance-aware writer checks (F7); witnesses (R4 Q5); latency on matched subsets,
                time to rejection vs time to merge, SnapMerge phase timings, order position (R3 MC5)
  enumerator    exhaustive physical-state enumeration (R2 MC5); workflow trace and races (R4 MC4)
  contended / history   isolated resource scaling with contended rows and history length (R3 MC4)
  equivalence   per-scenario decisions of the v3 main campaign vs the submitted version's v2 campaign
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

import analyze as A

CONTROL = ["replay", "replay_intent", "replay_intent_exact", "rebase_occ", "snapmerge[no_net_effect]", "snapmerge",
           "recompute"]
EFFECTS = [("recorded intent (replay_intent - replay)", "replay_intent", "replay"),
           ("exact net-effect test under replay (replay_intent_exact - replay_intent)", "replay_intent_exact", "replay_intent"),
           ("composition under pairwise decisions (snapmerge[no_net_effect] - replay_intent)", "snapmerge[no_net_effect]", "replay_intent"),
           ("composition under exact decisions (snapmerge - replay_intent_exact)", "snapmerge", "replay_intent_exact"),
           ("exact test under composition (snapmerge - snapmerge[no_net_effect])", "snapmerge", "snapmerge[no_net_effect]"),
           ("total (snapmerge - replay)", "snapmerge", "replay")]
LABEL = dict(A.POLICY_LABEL, **{
    "replay_intent": "Replay + recorded predicates (pairwise)", "replay_intent_exact": "Replay + exact net-effect test",
    "rebase_occ": "Rebase, one-direction validation (OCC)", "recompute": "Recompute both orders (reference)",
    "snapmerge[append_rule]": "SnapMerge, submitted insert rule"})


def _load(files) -> list[dict]:
    return [json.loads(Path(f).read_text()) for f in files]


def _accepted(gate: dict, name: str) -> list:
    g = gate.get(name) or {}
    return list(g.get("accepted") or []) if g.get("status") in ("accepted", "partial") else []


# ============================================================================ contract-normalized outcomes

def contract_table(recs: list[dict]) -> dict:
    out = {}
    pols = sorted({p for r in recs for p in r["policies"]})
    for p in pols:
        c = Counter()
        for r in recs:
            x = r["policies"].get(p)
            if x is None:
                continue
            tc = bool(r["oracle"]["true_conflict"])
            side = "true_conflict" if tc else "commuting"
            c[f"{side}:n"] += 1
            v = x.get("verdict")
            if v == "merged":
                ac, ca = bool(x.get("eq_ac")), bool(x.get("eq_ca"))
                k = "both" if ac and ca else "ac_only" if ac else "ca_only" if ca else "neither"
                c[f"{side}:merged_{k}"] += 1
            elif v == "conflict":
                c[f"{side}:rejected"] += 1
                c[f"{side}:rejected:{x.get('category') or 'conflict'}"] += 1
            else:
                c[f"{side}:error"] += 1
        n_tc, n_cm = c["true_conflict:n"], c["commuting:n"]
        out[p] = {
            "label": LABEL.get(p, p), "counts": dict(sorted(c.items())),
            "two_order": {"sound_merges": c["commuting:merged_both"], "missed_conflicts": c["true_conflict:merged_ac_only"]
                          + c["true_conflict:merged_ca_only"], "neither_order": c["commuting:merged_neither"]
                          + c["true_conflict:merged_neither"], "false_conflicts": c["commuting:rejected"],
                          "reported_conflicts": c["true_conflict:rejected"]},
            "rebase_order": {"merges_equal_ac": c["commuting:merged_both"] + c["true_conflict:merged_ac_only"],
                             "merges_not_ac": c["true_conflict:merged_ca_only"] + c["true_conflict:merged_neither"]
                             + c["commuting:merged_neither"]},
            "n_true_conflicts": n_tc, "n_commuting": n_cm}
    return out


# ============================================================================ bootstrap helpers

def _rate_arrays(recs: list[dict], pols: list[str]):
    mix = np.array([r["scenario"]["mix"] for r in recs])
    comm = np.array([0.0 if r["oracle"]["true_conflict"] else 1.0 for r in recs])
    num = {}
    for p in pols:
        num[p] = np.array([1.0 if (r["policies"].get(p, {}).get("verdict") == "merged"
                                   and r["policies"][p].get("eq_ac") and r["policies"][p].get("eq_ca")
                                   and not r["oracle"]["true_conflict"]) else 0.0 for r in recs])
    return mix, comm, num


def paired_rates(recs: list[dict], pols: list[str], diffs: list[tuple], B: int, seed: int, groups=True) -> dict:
    """Sound auto-merge rate of commuting scenarios per policy and paired differences, with percentile
    bootstrap intervals over scenarios stratified by mix (the same resamples for every policy)."""
    pols = [p for p in pols if any(p in r["policies"] for r in recs)]
    if not recs or not pols:
        return {}
    mix, comm, num = _rate_arrays(recs, pols)
    gnames = ["all"] + ([m for m in A.MIXES if (mix == m).any()] if groups else [])
    cols, NUM, DEN = [], [], []
    for g in gnames:
        gm = np.ones(len(recs)) if g == "all" else (mix == g).astype(float)
        for p in pols:
            cols.append((g, p))
            NUM.append(num[p] * gm)
            DEN.append(comm * gm)
    NUM, DEN = np.column_stack(NUM), np.column_stack(DEN)
    R, _ = A.bootstrap(mix, NUM, DEN, [], B, seed)
    with np.errstate(divide="ignore", invalid="ignore"):
        point = NUM.sum(0) / DEN.sum(0)
    j = {c: i for i, c in enumerate(cols)}
    out = {}
    for g in gnames:
        e = {"commuting": int(DEN[:, j[(g, pols[0])]].sum()), "rates": {}, "differences": {}}
        for p in pols:
            v, ci = point[j[(g, p)]], A.qci(R[:, j[(g, p)]].astype(float))
            e["rates"][p] = {"value": A.rnd(v), "ci95": [A.rnd(ci[0]), A.rnd(ci[1])] if ci else None,
                             "k": int(NUM[:, j[(g, p)]].sum())}
        for name, a, b in diffs:
            if a in pols and b in pols:
                d = R[:, j[(g, a)]].astype(float) - R[:, j[(g, b)]].astype(float)
                ci = A.qci(d)
                e["differences"][name] = {"a": a, "b": b, "value": A.rnd(point[j[(g, a)]] - point[j[(g, b)]]),
                                          "ci95": [A.rnd(ci[0]), A.rnd(ci[1])] if ci else None}
        out[g] = e
    return out


def mechanisms(recs: list[dict], B: int, seed: int) -> dict:
    out = paired_rates(recs, CONTROL, EFFECTS, B, seed)
    reasons = {}
    for p in ("replay_intent", "replay_intent_exact", "rebase_occ"):
        c = Counter()
        for r in recs:
            x = r["policies"].get(p)
            if x and x.get("verdict") == "conflict":
                tc = "true conflict" if r["oracle"]["true_conflict"] else "commuting"
                w = x.get("witness") or {}
                c[f"{tc}: {x.get('conflict_class')}: {w.get('reason') or x.get('rule')}"] += 1
        reasons[p] = dict(c.most_common())
    return {"by_group": out, "conflict_reasons": reasons}


# ============================================================================ sensitivity

def sensitivity(gate: dict, B: int, seed: int) -> dict:
    main = [r for r in _load(_accepted(gate, A.MAIN))
            if int(r["scenario"]["scenario_id"].rsplit("-", 1)[1]) < 60]
    levels = [("default", "all", None, main)]
    for name, (knob, val) in A.SENS_LEVELS.items():
        recs = _load(_accepted(gate, name))
        if recs:
            levels.append((name, knob, val, recs))
    pols = ["replay", "replay_intent", "replay_intent_exact", "rebase_occ", "snapmerge[no_net_effect]", "snapmerge"]
    diffs = [("snapmerge - replay", "snapmerge", "replay"),
             ("snapmerge - replay_intent_exact", "snapmerge", "replay_intent_exact"),
             ("replay_intent_exact - replay", "replay_intent_exact", "replay")]
    out = []
    for name, knob, val, recs in levels:
        tc = sum(1 for r in recs if r["oracle"]["true_conflict"])
        ops = [len(r["scenario"]["ops"][s]) for r in recs for s in ("A", "C")]
        sm_unsound = sum(1 for r in recs for p, x in r["policies"].items()
                         if p.startswith("snapmerge") and p != "snapmerge[append_rule]" and x.get("verdict") == "merged"
                         and not (x.get("eq_ac") and x.get("eq_ca")))
        sm_recall = sum(1 for r in recs if r["oracle"]["true_conflict"]
                        and r["policies"].get("snapmerge", {}).get("verdict") == "conflict")
        cs = seed ^ int(hashlib.sha256(name.encode()).hexdigest()[:8], 16)  # deterministic per level
        e = {"campaign": name if name != "default" else A.MAIN + " (first 60 per mix)", "knob": knob, "level": val,
             "scenarios": len(recs), "true_conflicts": tc, "commuting": len(recs) - tc,
             "true_conflict_share": A.rnd(tc / len(recs)) if recs else None,
             "ops_per_side_mean": A.rnd(float(np.mean(ops)), 2) if ops else None,
             "ops_per_side_max": int(max(ops)) if ops else None,
             "snapmerge_unsound_merges_all_variants": sm_unsound, "snapmerge_recall": f"{sm_recall}/{tc}",
             "rates": paired_rates(recs, pols, diffs, B, cs, groups=False).get("all")}
        out.append(e)
    return {"levels": out, "note": "default level = the first 60 scenarios per mix of the main campaign (same seeds "
                                   "and generator defaults as the sensitivity campaigns)"}


# ============================================================================ single-snapshot writer, provenance, witnesses

def writer_single(gate: dict, S: dict) -> dict | None:
    recs = _load(_accepted(gate, A.WRITER_SINGLE))
    if not recs:
        return None
    ct = contract_table(recs)
    roles = Counter(role for r in recs for s in (r.get("provenance_check") or {}).values()
                    for o in s["ops"] for role in o["roles"])
    unsound = {p: v["two_order"]["neither_order"] for p, v in ct.items()}
    out = {"scenarios": len(recs), "true_conflicts": sum(1 for r in recs if r["oracle"]["true_conflict"]),
           "snapshot_roles": dict(roles), "contract": ct, "neither_order_merges": unsound,
           "scenarios_with_mixed_snapshots": sum(1 for r in recs if any(
               "mixed" in o["roles"] for s in (r.get("provenance_check") or {}).values() for o in s["ops"]))}
    sm = ct.get("snapmerge")
    ar = ct.get("snapmerge[append_rule]")
    if sm:
        k = sm["two_order"]["neither_order"] + sm["two_order"]["missed_conflicts"]
        n = sm["two_order"]["sound_merges"] + k
        out["snapmerge_unsound_bound"] = A.clopper_pearson_one_sided(k, n)
    if ar:  # merges that violate the contract (not equal to both orders): one order only, or neither order
        out["append_rule_unsound_merges"] = ar["two_order"]["neither_order"] + ar["two_order"]["missed_conflicts"]
        out["append_rule_one_order_merges"] = ar["two_order"]["missed_conflicts"]
        out["append_rule_neither_order_merges"] = ar["two_order"]["neither_order"]
    return out


def provenance_stats(recs: list[dict]) -> dict | None:
    have = [r for r in recs if r.get("provenance_check")]
    if not have:
        return None
    ops = [o for r in have for s in r["provenance_check"].values() for o in s["ops"]]
    viol = Counter(v for o in ops for v in o["violations"])
    return {"scenarios": len(have), "operations_checked": len(ops), "snapshots_checked": sum(o["snapshots"] for o in ops),
            "operations_with_violations": sum(1 for o in ops if not o["ok"]), "violations": dict(viol.most_common(10)),
            "roles": dict(Counter(role for o in ops for role in o["roles"]))}


def witness_stats(recs: list[dict]) -> dict:
    kinds, diffs, rules, cats = Counter(), Counter(), Counter(), Counter()
    for r in recs:
        x = r["policies"].get("snapmerge") or {}
        if x.get("verdict") != "conflict":
            continue
        cats[x.get("category")] += 1
        w = x.get("witness") or {}
        if x.get("category") == "logical-conflict":
            kinds[w.get("kind", "pair")] += 1
            rules[x.get("rule")] += 1
            if "order_difference" in w:
                diffs[w["order_difference"]] += 1
    return {"categories": dict(cats), "logical_witness_kinds": dict(kinds), "rules": dict(rules.most_common()),
            "order_difference": {str(k): v for k, v in sorted(diffs.items())},
            "nonzero_order_difference": sum(v for k, v in diffs.items() if k != 0)}


def latency_facts(recs: list[dict]) -> dict:
    lat = defaultdict(lambda: defaultdict(list))
    phases = defaultdict(lambda: defaultdict(list))
    by_order = defaultdict(lambda: defaultdict(list))
    for r in recs:
        tc = bool(r["oracle"]["true_conflict"])
        for p, x in r["policies"].items():
            if x.get("latency_ms") is None:
                continue
            v = x.get("verdict")
            sound = v == "merged" and x.get("eq_ac") and x.get("eq_ca")
            lat[p]["merge" if v == "merged" else "rejection" if v == "conflict" else "error"].append(x["latency_ms"])
            if sound:
                lat[p]["sound_merge"].append(x["latency_ms"])
            by_order[p]["first" if x.get("order") == 0 else "later"].append(x["latency_ms"])
            if p == "snapmerge":
                for k, t in ((x.get("details") or {}).get("timing_ms") or {}).items():
                    phases["merge" if v == "merged" else "rejection"][k].append(t)

    def med(xs):
        return {"n": len(xs), "median_ms": A.rnd(float(np.median(xs)), 2)} if xs else None

    return {"by_outcome": {p: {k: med(v) for k, v in d.items()} for p, d in sorted(lat.items())},
            "snapmerge_phases": {o: {k: med(v) for k, v in d.items()} for o, d in phases.items()},
            "by_order_position": {p: {k: med(v) for k, v in d.items()} for p, d in sorted(by_order.items())},
            "note": "order position: policies run in a seeded random order per scenario; 'first' = ran first"}


# ============================================================================ enumerator, workflow

def enumerator(gate: dict) -> dict | None:
    recs = _load(_accepted(gate, A.ENUM))
    if not recs:
        return None
    c = Counter()
    for r in recs:
        if r.get("verdict") == "error":
            c["errors"] += 1
        elif r["verdict"] == "merged":
            c["merged"] += 1
            c["merged_sound"] += bool(r.get("sound"))
            c["positions_match_formula"] += bool(r.get("positions_match_formula"))
            c["bad_delete_files"] += int(r.get("bad_delete_files") or 0)
            c["merged_on_true_conflict"] += bool(r.get("true_conflict"))
        else:
            c["rejected_true_conflict" if r.get("true_conflict") else "rejected_commuting"] += 1
            c[f"category:{r.get('category')}"] += 1
    bases = sorted({r["case"]["base"] for r in recs})
    return {"cases": len(recs), "bases": len(bases), "menu": len({r["case"]["A"] for r in recs}),
            "true_conflicts": sum(1 for r in recs if r.get("true_conflict")), **dict(c),
            "unsound_bound": A.clopper_pearson_one_sided(c["merged"] - c["merged_sound"], c["merged"]) if c["merged"] else None}


def workflow(gate: dict) -> dict | None:
    recs = _load(_accepted(gate, A.WORKFLOW))
    if not recs:
        return None
    trace = next((r for r in recs if r.get("id") == "trace"), None)
    races = [r for r in recs if r.get("id") != "trace" and "error" not in r]
    seq = Counter((r["injected_on"], r["fired"], " -> ".join(a["category"] for a in r["attempts"])) for r in races)
    fired = [r for r in races if r["fired"]]
    checks = [r["check"] for r in races if r.get("check")]
    return {
        "trace": [{k: s.get(k) for k in ("step", "category", "rule", "resequenced", "latency_ms", "check",
                                          "target_unchanged")} for s in (trace or {}).get("steps", [])],
        "races": len(races), "errors": sum(1 for r in recs if "error" in r),
        "fired": len(fired), "by_target": {t: {"fired": sum(1 for r in fired if r["injected_on"] == t),
                                               "first_attempt": dict(Counter(r["attempts"][0]["category"] for r in fired
                                                                             if r["injected_on"] == t)),
                                               "final": dict(Counter(r["final_category"] for r in fired if r["injected_on"] == t)),
                                               "attempts_max": max((len(r["attempts"]) for r in fired if r["injected_on"] == t),
                                                                   default=0)}
                                           for t in ("other", "source", "target")},
        "sequences": {f"{t}|fired={f}|{s}": n for (t, f, s), n in sorted(seq.items())},
        "checked_merges": len(checks), "checked_both_orders": sum(1 for c in checks if c["eq_ac"] and c["eq_ca"]),
        "checked_readers_agree": sum(1 for c in checks if c["readers_agree"]),
        "resequenced": dict(Counter(a["resequenced"] for r in fired for a in r["attempts"] if a["category"] == "merged"))}


# ============================================================================ contended, history (scale-runner campaigns)

def scaling(S: dict, names: list[str]) -> list[dict]:
    rows = []
    for name in names:
        c = S["campaigns"].get(name)
        if not c:
            continue
        for g in c["groups"]:
            def v(key, stat="median"):
                e = g.get(key)
                return e.get(stat) if e else None
            rows.append({"campaign": name, "size": g["size"], "partitioned": g["partitioned"], "kind": g["kind"],
                         "policy": g["policy"], "reps": g["reps"], "verdicts": g["verdicts"],
                         "latency_ms_median": v("latency_ms"), "latency_ms_max": v("latency_ms", "max"),
                         "bytes_read_median": v("bytes_read_data_delete"),
                         "estimate_read_bytes_median": v("estimate_read_bytes"),
                         "estimate_read_rows_median": v("estimate_read_rows"),
                         "contended_partitions": v("estimate_contended_partitions"),
                         "peak_rss_gb_max": v("isolated_peak_rss_gb", "max"),
                         "rss_increment_gb_max": v("isolated_policy_rss_increment_gb", "max"),
                         "peak_temp_bytes_max": v("isolated_peak_temp_bytes", "max"),
                         "timing_ms": {k: e.get("median") for k, e in (g.get("timing_ms") or {}).items() if e},
                         "untimed_ms_median": v("untimed_ms"),
                         "checks": g["oracle_checks"], "checks_both_orders": g.get("oracle_checks_both_orders")})
    return rows


def estimate_accuracy(rows: list[dict]) -> dict | None:
    pts = [(r["estimate_read_bytes_median"], r["bytes_read_median"]) for r in rows
           if r["policy"] == "snapmerge" and r["estimate_read_bytes_median"] and r["bytes_read_median"]]
    if len(pts) < 2:
        return None
    est, act = np.array(pts, float).T
    ratio = act / est
    return {"points": len(pts), "actual_over_estimate_median": A.rnd(float(np.median(ratio)), 3),
            "actual_over_estimate_range": [A.rnd(float(ratio.min()), 3), A.rnd(float(ratio.max()), 3)],
            "log_log_correlation": A.rnd(float(np.corrcoef(np.log(est), np.log(act))[0, 1]), 4)}


# ============================================================================ regression against earlier versions

def equivalence(gate: dict) -> dict:
    """Same-seed regression of each current campaign against its superseded predecessors (A.PREVIOUS):
    per shared policy, identical verdict and serial equivalence.  Not used for any rate."""
    out = {}
    for cur, prevs in A.PREVIOUS.items():
        now = {Path(f).stem: f for f in _accepted(gate, cur)}
        for prev in prevs:
            pdir = A.RESULTS / prev / "raw"
            if not now or not pdir.exists():
                continue
            same, diff, compared = Counter(), [], 0
            for sid, f in sorted(now.items()):
                g = pdir / f"{sid}.json"
                if not g.exists():
                    continue
                a, b = json.loads(Path(g).read_text()), json.loads(Path(f).read_text())
                compared += 1
                if a["oracle"]["true_conflict"] != b["oracle"]["true_conflict"]:
                    diff.append({"scenario": sid, "policy": "oracle"})
                for pol in set(a["policies"]) & set(b["policies"]):
                    va = (a["policies"][pol].get("verdict"), a["policies"][pol].get("serial_equivalent"))
                    vb = (b["policies"][pol].get("verdict"), b["policies"][pol].get("serial_equivalent"))
                    if va == vb:
                        same[pol] += 1
                    else:
                        diff.append({"scenario": sid, "policy": pol, "before": va, "now": vb})
            out[f"{cur} vs {prev}"] = {"scenarios_compared": compared, "identical_by_policy": dict(sorted(same.items())),
                                       "differences": len(diff), "difference_examples": diff[:20]}
    return out


# ============================================================================ re-review 2026-10-07: coverage, phase I/O, estimate bounds, recording

def coverage(gate: dict) -> dict:
    """Cost campaigns: which timed runs emitted a merge and which merges were oracle-checked (both orders,
    both readers).  Repetitions use different seeded scenarios (e.g. a different contended partition);
    only the first --check-reps repetitions of a merging cell are checked, because a check at SF 10
    takes minutes.  The full manifest (one row per timed run) is written to results/coverage_cost_campaigns.json."""
    camps = [A.SCALE, A.CONTENDED, A.CONTENDED_UNPART, A.CONTENDED_SF10, A.HISTORY]
    summary, manifest = {}, []
    for c in camps:
        files = _accepted(gate, c)
        if not files:
            continue
        runs = merges = checked = checked_ok = 0
        by_policy: dict = {}
        for f in files:
            r = json.loads(Path(f).read_text())
            runs += 1
            merged = r.get("verdict") == "merged"
            chk = r.get("check") if isinstance(r.get("check"), dict) else None
            ok = bool(chk and chk.get("eq_ac") and chk.get("eq_ca") and chk.get("readers_agree", True))
            merges += merged
            checked += bool(merged and chk)
            checked_ok += bool(merged and ok)
            bp = by_policy.setdefault(r["policy"], {"runs": 0, "merges": 0, "checked": 0, "checked_both_orders": 0})
            bp["runs"] += 1
            bp["merges"] += merged
            bp["checked"] += bool(merged and chk)
            bp["checked_both_orders"] += bool(merged and ok)
            scen = hashlib.sha256(json.dumps(r.get("ops"), sort_keys=True, default=str).encode()).hexdigest()[:12]
            mg, iso = r.get("memguard") or {}, r.get("isolated") or {}
            # minor revision 2026-10-08 (M3): resource settings and memory quantities of every timed run, kept apart:
            # absolute resident set before the call and at its peak, the increment, and the peak spill (DuckDB temp)
            manifest.append({"campaign": c, "size": r["size"], "kind": r["kind"], "rep": int(r["rep"]),
                             "policy": r["policy"], "verdict": r.get("verdict"), "scenario_digest": scen,
                             "checked": bool(chk), "both_orders": ok if chk else None,
                             "readers_agree": chk.get("readers_agree") if chk else None,
                             "status": "completed",  # a run is in the manifest iff its record was written atomically
                             "order_position": r.get("order"), "latency_ms": r.get("latency_ms"),
                             "loadavg_1m": r.get("loadavg_1m"),
                             "guard_limit_gb": mg.get("guard_limit_gb"), "guard_invocation": mg.get("guard_invocation"),
                             "duckdb_memory_limit_mb": mg.get("duckdb_memory_limit_mb"),
                             "batch_scale": mg.get("batch_scale"), "restart": mg.get("restart"),
                             "rss_before_gb": iso.get("baseline_rss_gb"), "rss_peak_gb": iso.get("peak_rss_gb"),
                             "rss_increment_gb": iso.get("policy_rss_increment_gb"),
                             "spill_bytes_peak": iso.get("peak_temp_bytes")})
        rows_c = [m for m in manifest if m["campaign"] == c]
        summary[c] = {"timed_runs": runs, "merges": merges, "checked_merges": checked,
                      "checked_merges_equal_both_orders": checked_ok, "by_policy": dict(sorted(by_policy.items())),
                      "distinct_scenarios": len({(m["size"], m["kind"], m["rep"]) for m in rows_c}),
                      "guard_limits_gb": sorted({str(m["guard_limit_gb"]) for m in rows_c}),
                      "duckdb_memory_limits_mb": sorted({str(m["duckdb_memory_limit_mb"]) for m in rows_c}),
                      "batch_scales": sorted({str(m["batch_scale"]) for m in rows_c}),
                      "restarts": sorted({str(m["restart"]) for m in rows_c}),
                      "runs_by_invocation": dict(sorted(Counter(str(m["guard_invocation"]) for m in rows_c).items()))}
    try:
        (A.OUT_DIR if hasattr(A, "OUT_DIR") else A.RESULTS).joinpath("coverage_cost_campaigns.json").write_text(
            json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    except OSError:
        pass
    return summary


def _sm(r: dict) -> dict:
    return r["policies"].get("snapmerge") or {}


def estimate_bounds(gate: dict) -> dict:
    """Does the preflight estimate bound the bytes actually read, per phase, in every SnapMerge decision
    of every differential campaign?  (Re-review G2: an upper bound under the same bounds evaluation.)"""
    n = viol = 0
    examples = []
    phase_tot: Counter = Counter()
    est_tot: Counter = Counter()
    for c, role in A.ROLES.items():
        if role in ("scale", "contended", "history", "enumerator", "workflow", "counterexamples"):
            continue
        for f in _accepted(gate, c):
            r = json.loads(Path(f).read_text())
            for pol, x in r["policies"].items():
                if not pol.startswith("snapmerge") or x.get("verdict") not in ("merged", "conflict"):
                    continue
                est = (x.get("details") or {}).get("estimate")
                io = x.get("io") or {}
                if not est or "phases" not in est:
                    continue
                n += 1
                for ph, b in (io.get("read_bytes_by_phase") or {}).items():
                    phase_tot[ph] += b
                    e = (est["phases"].get(ph) or {}).get("bytes")
                    if e is None or b > e:
                        viol += 1
                        if len(examples) < 10:
                            examples.append({"campaign": c, "scenario": r["scenario"]["scenario_id"], "policy": pol,
                                             "phase": ph, "actual": b, "estimate": e})
                for ph, d in est["phases"].items():
                    est_tot[ph] += d.get("bytes", 0)
    return {"decisions": n, "phase_bound_violations": viol, "examples": examples,
            "actual_bytes_by_phase": dict(phase_tot), "estimated_bytes_by_phase": dict(est_tot)}


def validator_rejections(gate: dict) -> dict:
    """Unsupported outcomes of every SnapMerge configuration in the differential campaigns, by reason
    (re-review G1: how often did the fresh-identity guard and the phase-order checks fire on generated
    scenarios?).  Reasons come from the structural witness (rule R7) or the rule name."""
    n = 0
    per_campaign: dict = {}
    reasons: Counter = Counter()
    for c, role in A.ROLES.items():
        if role in ("scale", "contended", "history", "enumerator", "workflow", "counterexamples"):
            continue
        k = 0
        for f in _accepted(gate, c):
            r = json.loads(Path(f).read_text())
            for pol, x in r["policies"].items():
                if not pol.startswith("snapmerge") or "verdict" not in x:
                    continue
                n += 1
                if x.get("category") != "unsupported" and x.get("conflict_class") != "structural":
                    continue
                k += 1
                rs = ((x.get("witness") or {}).get("reasons") if isinstance(x.get("witness"), dict) else None) or [x.get("rule")]
                for why in rs:
                    why = str(why)
                    kind = ("identity" if "identity:" in why else "phase" if "contradictory" in why
                            else why.split(":", 1)[0] if ":" in why else why)
                    reasons[kind] += 1
        per_campaign[c] = k
    return {"decisions": n, "unsupported": sum(per_campaign.values()), "by_reason": dict(reasons),
            "identity": reasons.get("identity", 0), "phase": reasons.get("phase", 0), "per_campaign": per_campaign}


def split_vs_mixed(gate: dict) -> dict | None:
    """Matched histories: the first 143 scenarios per mix of the main campaign (split writer: delete part +
    insert part) and the single-snapshot campaign (one mixed snapshot per copy-on-write operation) share
    seeds, so the same logical histories are encoded two ways.  Per-phase bytes SnapMerge read and its
    phase timings, paired by scenario."""
    main = {Path(f).stem: f for f in _accepted(gate, A.MAIN)}
    ws = {Path(f).stem: f for f in _accepted(gate, A.WRITER_SINGLE)}
    pairs = sorted(set(main) & set(ws))
    if not pairs:
        return None
    rows = []
    for sid in pairs:
        a, b = json.loads(Path(main[sid]).read_text()), json.loads(Path(ws[sid]).read_text())
        sa, sb = _sm(a), _sm(b)
        if a["oracle"]["true_conflict"] != b["oracle"]["true_conflict"]:
            continue  # the encodings must describe the same logical histories
        ra = (sa.get("io") or {}).get("read_bytes_by_phase") or {}
        rb = (sb.get("io") or {}).get("read_bytes_by_phase") or {}
        rows.append({"split": ra, "mixed": rb, "lat_split": sa.get("latency_ms"), "lat_mixed": sb.get("latency_ms"),
                     "t_split": (sa.get("details") or {}).get("timing_ms") or {},
                     "t_mixed": (sb.get("details") or {}).get("timing_ms") or {},
                     "meta_split": (sa.get("io") or {}).get("metadata_bytes_read", 0),
                     "meta_mixed": (sb.get("io") or {}).get("metadata_bytes_read", 0),
                     "same_verdict": sa.get("verdict") == sb.get("verdict"),
                     "mixed_parts": sum(1 for s_ in (b.get("provenance_check") or {}).values() for o in s_["ops"]
                                        if "mixed" in o["roles"])})

    def stat(key, ph):
        xs = np.array([r[key].get(ph, 0) for r in rows], float)
        return {"mean_bytes": A.rnd(float(xs.mean()), 1), "share_nonzero": A.rnd(float((xs > 0).mean()), 4),
                "p90_bytes": A.rnd(float(np.percentile(xs, 90)), 1), "max_bytes": A.rnd(float(xs.max()), 1)}

    phases = sorted({ph for r in rows for k in ("split", "mixed") for ph in r[k]})
    with_mixed = [r for r in rows if r["mixed_parts"]]
    lat = [(r["lat_split"], r["lat_mixed"]) for r in rows if r["lat_split"] and r["lat_mixed"]]
    ratio = np.exp(np.mean([np.log(m / s_) for s_, m in lat])) if lat else None
    return {"pairs": len(rows), "pairs_with_mixed_snapshots": len(with_mixed),
            "same_verdict": sum(r["same_verdict"] for r in rows),
            "by_phase": {ph: {"split": stat("split", ph), "mixed": stat("mixed", ph)} for ph in phases},
            "total_mean_bytes": {"split": A.rnd(float(np.mean([sum(r["split"].values()) for r in rows])), 1),
                                 "mixed": A.rnd(float(np.mean([sum(r["mixed"].values()) for r in rows])), 1)},
            "latency_ms_median": {"split": A.rnd(float(np.median([s_ for s_, _ in lat])), 2) if lat else None,
                                  "mixed": A.rnd(float(np.median([m for _, m in lat])), 2) if lat else None},
            "latency_geomean_ratio_mixed_over_split": A.rnd(float(ratio), 3) if ratio else None,
            "metadata_bytes_mean": {"split": A.rnd(float(np.mean([r["meta_split"] for r in rows])), 1),
                                    "mixed": A.rnd(float(np.mean([r["meta_mixed"] for r in rows])), 1)},
            "timing_ms_median": {enc: {ph: A.rnd(float(np.median([r[f"t_{enc}"][ph] for r in rows if ph in r[f"t_{enc}"]])), 3)
                                       for ph in sorted({k for r in rows for k in r[f"t_{enc}"]})}
                                 for enc in ("split", "mixed")}}


def recording_overhead(gate: dict) -> dict | None:
    """Cost of recording (production) vs harness validation (main campaign, every scenario): bytes of
    snapmerge.* summary properties per committed snapshot, share of the table metadata file, writer time
    (apply) vs the harness's provenance check and extensional writer check.  The fallback campaign writes
    the same generator's operations without recording (different seeds) for a distribution-level contrast."""
    out = {}
    for label, camp in (("recorded (main)", A.MAIN), ("unrecorded (fallback)", A.FALLBACK)):
        recs = _load(_accepted(gate, camp))
        rec = [r for r in recs if r.get("recording") and r["recording"].get("snapshots")]
        if not rec:
            continue
        per_snap = [r["recording"]["snapmerge_bytes"] / r["recording"]["snapshots"] for r in rec]
        share = [r["recording"]["snapmerge_bytes"] / r["recording"]["metadata_file_bytes"]
                 for r in rec if r["recording"].get("metadata_file_bytes")]
        nops = [sum(len(r["scenario"]["ops"][s_]) for s_ in ("A", "C")) for r in rec]
        t = [r.get("timing_s") or {} for r in rec]
        apply_per_op = [x.get("apply", 0) * 1e3 / n for x, n in zip(t, nops) if n]
        prov_per_op = [x.get("provenance_check", 0) * 1e3 / n for x, n in zip(t, nops) if n and x.get("provenance_check")]
        wchk = [x.get("writer_check", 0) * 1e3 for x in t if x.get("writer_check") is not None]
        out[label] = {"campaign": camp, "scenarios": len(rec),
                      "snapmerge_bytes_per_snapshot_median": A.rnd(float(np.median(per_snap)), 1),
                      "snapmerge_bytes_per_snapshot_max": A.rnd(float(np.max(per_snap)), 1),
                      "snapmerge_share_of_metadata_file_median": A.rnd(float(np.median(share)), 4) if share else None,
                      "metadata_file_bytes_median": A.rnd(float(np.median([r["recording"]["metadata_file_bytes"] for r in rec
                                                                            if r["recording"].get("metadata_file_bytes")])), 1),
                      "snapshots_per_scenario_median": A.rnd(float(np.median([r["recording"]["snapshots"] for r in rec])), 1),
                      "writer_ms_per_op_median": A.rnd(float(np.median(apply_per_op)), 2) if apply_per_op else None,
                      "provenance_check_ms_per_op_median": A.rnd(float(np.median(prov_per_op)), 2) if prov_per_op else None,
                      "writer_check_ms_per_scenario_median": A.rnd(float(np.median(wchk)), 2) if wchk else None}
    return out or None


# ============================================================================ driver

def run(S: dict, gate: dict, B: int, seed: int) -> dict:
    rev: dict = {}
    main = _load(_accepted(gate, A.MAIN))
    nyc = _load(_accepted(gate, A.NYC))
    for key, recs in (("main", main), ("nyc", nyc)):
        if recs:
            rev[f"contract_{key}"] = contract_table(recs)
            rev[f"mechanisms_{key}"] = mechanisms(recs, B, seed ^ (1 if key == "main" else 2))
            rev[f"provenance_{key}"] = provenance_stats(recs)
            rev[f"witnesses_{key}"] = witness_stats(recs)
            rev[f"latency_{key}"] = latency_facts(recs)
    for name in ("ablation", "fallback", "mixed"):
        camp = {"ablation": A.ABLATION, "fallback": A.FALLBACK, "mixed": A.MIXED}[name]
        recs = _load(_accepted(gate, camp))
        if recs:
            rev[f"contract_{name}"] = contract_table(recs)
            rev[f"mechanisms_{name}"] = mechanisms(recs, B, seed ^ 3)
    rev["sensitivity"] = sensitivity(gate, B, seed ^ 4)
    rev["writer_single"] = writer_single(gate, S)
    ws = _load(_accepted(gate, A.WRITER_SINGLE))
    if ws:
        rev["provenance_writer_single"] = provenance_stats(ws)
    rev["enumerator"] = enumerator(gate)
    rev["workflow"] = workflow(gate)
    rev["contended"] = scaling(S, [A.CONTENDED, A.CONTENDED_UNPART, A.CONTENDED_SF10])
    rev["contended_estimate_accuracy"] = estimate_accuracy(rev["contended"] + scaling(S, [A.SCALE]))
    rev["history"] = scaling(S, [A.HISTORY])
    rev["equivalence"] = equivalence(gate)
    rev["coverage"] = coverage(gate)
    # M3 (2026-10-08): guard invocations of the chains, summarized by scripts/guard_events.py (inputs, shipped)
    rev["guard_events"] = {}
    for prefix in ("minor_ALL", "rereview_ALL"):
        f = A.RESULTS / f"guard_events_{prefix}.json"
        if f.exists():
            rev["guard_events"][prefix] = json.loads(f.read_text())
    rev["estimate_bounds"] = estimate_bounds(gate)
    rev["validator_rejections"] = validator_rejections(gate)
    rev["split_vs_mixed"] = split_vs_mixed(gate)
    rev["recording_overhead"] = recording_overhead(gate)
    return rev


# ============================================================================ markdown

def _ci(e, k=3):
    if not e or e.get("value") is None:
        return "n/a"
    s = f"{e['value']:.{k}f}"
    if e.get("ci95"):
        s += f" [{e['ci95'][0]:.{k}f}, {e['ci95'][1]:.{k}f}]"
    return s


def markdown(S: dict) -> list[str]:
    R = S.get("revision") or {}
    L = ["\n# Revision analyses (2026-10-05)\n",
         "Sections 19-30 answer the review package. Definitions are in `analysis_revision.py`.\n"]
    for key, title in (("main", A.MAIN), ("nyc", A.NYC)):
        ct = R.get(f"contract_{key}")
        if not ct:
            continue
        L.append(f"\n## 19{'a' if key == 'main' else 'b'}. Contract-normalized outcomes (`{title}`)\n")
        rows = []
        for p in A.POLICY_ORDER + [x for x in ct if x not in A.POLICY_ORDER]:
            if p not in ct:
                continue
            c = ct[p]["counts"]
            rows.append([LABEL.get(p, p), c.get("commuting:merged_both", 0), c.get("commuting:merged_neither", 0),
                         c.get("commuting:rejected", 0), c.get("true_conflict:rejected", 0),
                         c.get("true_conflict:merged_ac_only", 0), c.get("true_conflict:merged_ca_only", 0),
                         c.get("true_conflict:merged_neither", 0), c.get("commuting:error", 0) + c.get("true_conflict:error", 0)])
        L.append(A.md_table(["Policy", "Comm.: merged = both", "Comm.: merged = neither", "Comm.: rejected",
                             "TC: rejected", "TC: merged = (A;C) only", "TC: merged = (C;A) only", "TC: merged = neither",
                             "Errors"], rows))
        m = R.get(f"mechanisms_{key}", {}).get("by_group", {})
        if m:
            L.append(f"\n## 20{'a' if key == 'main' else 'b'}. Information-matched controls and mechanism attribution (`{title}`)\n")
            pols = [p for p in CONTROL if p in m["all"]["rates"]]
            rows = [[g] + [_ci(m[g]["rates"].get(p)) for p in pols] for g in m]
            L.append(A.md_table(["Group"] + [LABEL.get(p, p) for p in pols], rows))
            rows = [[g] + [_ci(m[g]["differences"].get(name)) for name, _, _ in EFFECTS] for g in m]
            L.append("\nPaired differences of the sound auto-merge rate of commuting scenarios:\n")
            L.append(A.md_table(["Group"] + [name for name, _, _ in EFFECTS], rows))
            reasons = R.get(f"mechanisms_{key}", {}).get("conflict_reasons", {})
            for p, rs in reasons.items():
                L.append(f"\nConflict reasons, {LABEL.get(p, p)}: " + "; ".join(f"{k}: {v}" for k, v in list(rs.items())[:8]))
    sens = R.get("sensitivity")
    if sens and sens.get("levels"):
        L.append("\n## 21. Sensitivity to partition span, history length, selectivity and compaction\n")
        rows = []
        for e in sens["levels"]:
            r = e.get("rates") or {}
            rates, d = r.get("rates", {}), r.get("differences", {})
            rows.append([e["knob"], e["level"], e["scenarios"], e.get("commuting"), e["true_conflict_share"], e["ops_per_side_mean"],
                         _ci(rates.get("snapmerge")), _ci(rates.get("replay_intent_exact")), _ci(rates.get("replay_intent")),
                         _ci(rates.get("replay")), _ci(d.get("snapmerge - replay")),
                         _ci(d.get("snapmerge - replay_intent_exact")), e["snapmerge_unsound_merges_all_variants"],
                         e["snapmerge_recall"]])
        L.append(A.md_table(["Knob", "Level", "n", "Commuting", "TC share", "Ops/side", "SnapMerge", "Replay+exact", "Replay+intent",
                             "Replay", "SnapMerge - replay", "SnapMerge - replay+exact", "SnapMerge unsound", "Recall"], rows))
        L.append("\n" + sens["note"])
    ws = R.get("writer_single")
    if ws:
        L.append(f"\n## 22. Single-snapshot copy-on-write writer (`{A.WRITER_SINGLE}`)\n")
        L.append(f"{ws['scenarios']} scenarios, {ws['true_conflicts']} true conflicts, "
                 f"{ws['scenarios_with_mixed_snapshots']} with at least one mixed snapshot; snapshot roles {ws['snapshot_roles']}.\n")
        rows = [[LABEL.get(p, p), v["two_order"]["sound_merges"], v["two_order"]["reported_conflicts"],
                 v["two_order"]["missed_conflicts"], v["two_order"]["neither_order"], v["two_order"]["false_conflicts"],
                 v["rebase_order"]["merges_equal_ac"]] for p, v in ws["contract"].items()]
        L.append(A.md_table(["Policy", "Sound merges", "Conflicts reported", "Missed (one order)", "Neither order",
                             "False conflicts", "Merges = (A;C)"], rows))
        if ws.get("snapmerge_unsound_bound"):
            L.append(f"\nSnapMerge one-sided 95% bound on merges not equal to both orders: {ws['snapmerge_unsound_bound']}; "
                     f"submitted insert rule: {ws.get('append_rule_unsound_merges')} merges of true conflicts "
                     f"({ws.get('append_rule_one_order_merges')} equal to one order only, "
                     f"{ws.get('append_rule_neither_order_merges')} to neither order).")
    for key in ("main", "nyc", "writer_single"):
        pv = R.get(f"provenance_{key}")
        if pv:
            L.append(f"\n## 23 ({key}). Provenance-aware writer checks\n")
            L.append(f"{pv['scenarios']} scenarios, {pv['operations_checked']} operations, {pv['snapshots_checked']} snapshots; "
                     f"operations with violations: {pv['operations_with_violations']}; roles {pv['roles']}; "
                     f"violations {pv['violations'] or 'none'}.")
    for key in ("main", "nyc"):
        w = R.get(f"witnesses_{key}")
        if w:
            L.append(f"\n## 24 ({key}). SnapMerge conflict categories and witnesses\n")
            L.append(f"Categories {w['categories']}; logical witness kinds {w['logical_witness_kinds']}; rules {w['rules']}; "
                     f"order difference (A;C)(v)-(C;A)(v) of the witness value {w['order_difference']} "
                     f"(non-zero in {w['nonzero_order_difference']}).")
        lf = R.get(f"latency_{key}")
        if lf:
            L.append(f"\n## 25 ({key}). Latency by outcome, SnapMerge phases, order position\n")
            rows = []
            for p, d in lf["by_outcome"].items():
                rows.append([LABEL.get(p, p)] + [(d.get(k) or {}).get("median_ms") for k in
                                                 ("sound_merge", "merge", "rejection")]
                            + [(lf["by_order_position"].get(p, {}).get(k) or {}).get("median_ms") for k in ("first", "later")])
            L.append(A.md_table(["Policy", "Sound merge ms", "Any merge ms", "Rejection ms", "Ran first ms", "Ran later ms"],
                                rows))
            L.append(f"\nSnapMerge phase medians (ms): {json.dumps(lf['snapmerge_phases'])}")
    en = R.get("enumerator")
    if en:
        L.append(f"\n## 26. Exhaustive physical-state enumeration (`{A.ENUM}`)\n")
        L.append(json.dumps({k: v for k, v in en.items()}, default=str))
    wf = R.get("workflow")
    if wf:
        L.append(f"\n## 27. One-shot workflow trace and merge races (`{A.WORKFLOW}`)\n")
        L.append(A.md_table(["Step", "Category", "Rule", "Re-sequenced", "Check", "Target unchanged"],
                            [[s["step"], s.get("category"), s.get("rule"), s.get("resequenced"), s.get("check"),
                              s.get("target_unchanged")] for s in wf["trace"]]))
        L.append(f"\nRaces: {wf['races']} scenarios, {wf['fired']} with the injected commit fired; by target "
                 f"{json.dumps(wf['by_target'])}; checked merges {wf['checked_merges']} (both orders {wf['checked_both_orders']}, "
                 f"readers agree {wf['checked_readers_agree']}); re-sequenced counts {wf['resequenced']}.")
    for key, title in (("contended", "Contended-fraction scaling (isolated processes)"), ("history", "History-length scaling")):
        rows = R.get(key) or []
        if rows:
            L.append(f"\n## {'28' if key == 'contended' else '29'}. {title}\n")
            L.append(A.md_table(["Campaign", "Size", "Kind", "Policy", "Reps", "Verdicts", "Median ms", "Untimed ms",
                                 "Bytes read", "Estimate bytes", "Estimate rows", "Peak RSS GB", "RSS incr GB", "Temp bytes"],
                                [[r["campaign"], r["size"], r["kind"], r["policy"], r["reps"], r["verdicts"],
                                  r["latency_ms_median"], r.get("untimed_ms_median"), r["bytes_read_median"],
                                  r["estimate_read_bytes_median"], r["estimate_read_rows_median"], r["peak_rss_gb_max"],
                                  r["rss_increment_gb_max"], r["peak_temp_bytes_max"]] for r in rows]))
            L.append("\nUntimed ms: median latency outside the timed phases (lineage, logical, compose, reconcile, emit); "
                     "for SnapMerge essentially the preflight read estimate, which evaluates every insert-role file's "
                     "bounds against the other side's predicates before the logical phase evaluates them again.")
    acc = R.get("contended_estimate_accuracy")
    if acc:
        L.append(f"\nPreflight estimate vs actual SnapMerge data/delete bytes read: {acc}")
    eq = R.get("equivalence")
    if eq:
        L.append("\n## 30. Regression against superseded campaigns (same seeds; verdict and serial equivalence per policy)\n")
        for k, e in eq.items():
            L.append(f"- {k}: {e['scenarios_compared']} scenarios; identical by policy {e['identical_by_policy']}; "
                     f"{e['differences']} differences; examples {e['difference_examples'][:3]}")
    cv = R.get("coverage")
    if cv:
        L.append("\n## 31. Cost campaigns: timed runs, merges and oracle-checked merges\n")
        L.append(A.md_table(["Campaign", "Timed runs", "Distinct scenarios", "Merges", "Checked merges", "Checked = both orders",
                             "By policy (runs/merges/checked)", "Guard GB", "DuckDB MB", "Batch scale", "Runs by invocation"],
                            [[c, v["timed_runs"], v["distinct_scenarios"], v["merges"], v["checked_merges"],
                              v["checked_merges_equal_both_orders"],
                              "; ".join(f"{p} {b['runs']}/{b['merges']}/{b['checked']}" for p, b in v["by_policy"].items()),
                              ", ".join(v.get("guard_limits_gb", [])), ", ".join(v.get("duckdb_memory_limits_mb", [])),
                              ", ".join(v.get("batch_scales", [])),
                              "; ".join(f"{k} {n}" for k, n in (v.get("runs_by_invocation") or {}).items())]
                             for c, v in cv.items()]))
        for prefix, ge in (R.get("guard_events") or {}).items():
            invs = ge.get("invocations") or []
            stops = [f"{o['invocation']}: {a['stop_reason']} (tree {a['peak_tree_rss_gb']} GB, host {a['peak_host_used_gb']} GB, "
                     f"batch scale {a['batch_scale']}, step {o.get('interrupted_step')})"
                     for o in invs for a in o.get("attempts", []) if a.get("stop_reason")]
            role = "current campaigns" if prefix == "minor_ALL" else "superseded re-review campaigns"
            L.append(f"\nGuard invocations of `{prefix}` ({role}): {len(invs)}; tree limits "
                     f"{sorted({o['tree_limit_gb'] for o in invs})} GB, host limit "
                     f"{sorted({o['host_used_limit_gb'] for o in invs})} GB; stops: {'; '.join(stops) or 'none'}.")
        L.append("\nEach repetition is a different seeded scenario; only the first --check-reps repetitions of each merging "
                 "cell are oracle-checked. A run is listed iff its record was written atomically by a completed child "
                 "process; a guard stop leaves no record for the interrupted run, which the next invocation repeats with "
                 "the same settings. Per-run manifest (settings, absolute RSS before and at peak, increment, spill, "
                 "check outcome): results/coverage_cost_campaigns.json.")
    eb = R.get("estimate_bounds")
    if eb:
        L.append("\n## 32. Preflight read estimate vs bytes actually read, per phase (all SnapMerge decisions, differential campaigns)\n")
        L.append(f"{eb['decisions']} decisions; phase-bound violations (actual > estimate): {eb['phase_bound_violations']}; "
                 f"actual bytes by phase {eb['actual_bytes_by_phase']}; estimated {eb['estimated_bytes_by_phase']}; "
                 f"examples {eb['examples'][:3]}.")
    vr = R.get("validator_rejections")
    if vr:
        L.append(f"\nValidator (re-review G1): {vr['unsupported']} unsupported outcomes in {vr['decisions']} SnapMerge "
                 f"decisions of the differential campaigns; identity-guard rejections {vr['identity']}, phase-order "
                 f"rejections {vr['phase']}; by reason {vr['by_reason']}.")
    sv = R.get("split_vs_mixed")
    if sv:
        L.append("\n## 33. Matched histories, split vs single-snapshot (mixed) encoding: SnapMerge reads by phase\n")
        L.append(f"{sv['pairs']} matched scenarios ({sv['pairs_with_mixed_snapshots']} with mixed snapshots); same verdict in "
                 f"{sv['same_verdict']}; mean total bytes split {sv['total_mean_bytes']['split']} vs mixed "
                 f"{sv['total_mean_bytes']['mixed']}; median latency ms {sv['latency_ms_median']}; geometric-mean latency "
                 f"ratio mixed/split {sv['latency_geomean_ratio_mixed_over_split']}; mean metadata bytes read "
                 f"{sv['metadata_bytes_mean']}; median phase timings (ms) {sv['timing_ms_median']}.")
        L.append(A.md_table(["Phase", "Split mean B", "Split share > 0", "Mixed mean B", "Mixed share > 0", "Mixed p90 B", "Mixed max B"],
                            [[ph, d["split"]["mean_bytes"], d["split"]["share_nonzero"], d["mixed"]["mean_bytes"],
                              d["mixed"]["share_nonzero"], d["mixed"]["p90_bytes"], d["mixed"]["max_bytes"]]
                             for ph, d in sv["by_phase"].items()]))
    ro = R.get("recording_overhead")
    if ro:
        L.append("\n## 34. Recording overhead vs harness validation cost\n")
        L.append(A.md_table(["Writer", "Scenarios", "snapmerge.* B / snapshot (median, max)", "Share of metadata file",
                             "Metadata file B", "Snapshots / scenario", "Writer ms / op", "Provenance check ms / op",
                             "Writer check ms / scenario"],
                            [[k, v["scenarios"], f"{v['snapmerge_bytes_per_snapshot_median']}, {v['snapmerge_bytes_per_snapshot_max']}",
                              v["snapmerge_share_of_metadata_file_median"], v["metadata_file_bytes_median"],
                              v["snapshots_per_scenario_median"], v["writer_ms_per_op_median"],
                              v["provenance_check_ms_per_op_median"], v["writer_check_ms_per_scenario_median"]]
                             for k, v in ro.items()]))
    return L


# ============================================================================ figures (dataviz: validated
# palette #2a78d6 / #eb6834 / #1baf7a / #4a3aa7; composite encoding = hue family + marker shape/fill)

FAMILY = {  # policy -> (colour, marker, filled, label)
    "snapmerge": (A.PAL["snapmerge"], "o", True, "SnapMerge: exact + compose"),
    "snapmerge[no_net_effect]": (A.PAL["snapmerge"], "o", False, "Pairwise + compose"),
    "replay_intent_exact": (A.PAL["replay"], "^", True, "Exact + replay"),
    "replay_intent": (A.PAL["replay"], "s", False, "Pairwise + replay"),
    "replay": (A.PAL["replay"], "s", True, "Replay #13959"),
}
SHORT = {"partition_disjoint_ingest": "Disjoint ingest", "filter_corrections": "Filter corrections",
         "upsert_keys": "Upsert by key", "deletes_mor": "Deletes (MOR)", "deletes_cow": "Deletes (COW)",
         "compaction_on_main": "Compaction", "adversarial_overlap": "Adversarial", "all": "All mixes"}


def _err(e):
    v = 100 * e["value"]
    return v, [[v - 100 * e["ci95"][0]], [100 * e["ci95"][1] - v]] if e.get("ci95") else None


def fig_controls(S: dict, out: Path) -> dict | None:
    m = (S.get("revision") or {}).get("mechanisms_main", {}).get("by_group")
    if not m:
        return None
    plt = A._mpl()
    groups = list(reversed([g for g in m if g == "all" or g in A.MIXES]))
    pols = [p for p in ("snapmerge", "snapmerge[no_net_effect]", "replay_intent_exact", "replay_intent", "replay")
            if p in m["all"]["rates"]]
    off = dict(zip(pols, np.linspace(0.32, -0.32, len(pols))))
    fig, ax = plt.subplots(figsize=(A.COL_W, 3.05))
    fig.subplots_adjust(left=0.31, right=0.975, bottom=0.12, top=0.80)
    for gi, g in enumerate(groups):
        if g == "all":
            ax.axhspan(gi - 0.45, gi + 0.45, color="#f0efec", zorder=0, lw=0)
    for p in pols:
        col, mk, filled, lab = FAMILY[p]
        first = True
        for gi, g in enumerate(groups):
            e = m[g]["rates"].get(p)
            if not e or e.get("value") is None:
                continue
            x, xerr = _err(e)
            ax.errorbar([x], [gi + off[p]], xerr=xerr, fmt=mk, ms=3.6, color=col, mfc=col if filled else "white",
                        mec=col, mew=0.8, elinewidth=0.8, capsize=0, zorder=4 if p == "snapmerge" else 3,
                        label=lab if first else None)
            first = False
    ax.set_yticks(range(len(groups)))
    ax.set_yticklabels([f"{SHORT[g]} (n={m[g]['commuting']})" for g in groups])
    ax.set_ylim(-0.55, len(groups) - 0.45)
    ax.set_xlim(-3, 103)
    ax.set_xticks([0, 20, 40, 60, 80, 100])
    ax.set_xlabel("Sound auto-merge rate of commuting scenarios (%)")
    ax.grid(axis="x", zorder=0)
    ax.tick_params(axis="y", length=0)
    h, l = ax.get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=2, handletextpad=0.3, columnspacing=0.8,
               borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    return {"file": A._relpath(out), "caption": (
        f"Figure 6. Information-matched controls on the main campaign ({A.MAIN}): every policy uses the same recorded "
        "operations and insertion provenance except partition-level replay. Blue = composition of the physical lineages, "
        "orange = physical replay; filled triangle/circle = exact net-effect test, hollow = pairwise validation. Whiskers: "
        "95% bootstrap intervals over scenarios (stratified by mix); n = commuting scenarios.")}


def fig_sensitivity(S: dict, out: Path) -> dict | None:
    sens = ((S.get("revision") or {}).get("sensitivity") or {}).get("levels") or []
    if len(sens) < 2:
        return None
    default = {"n_parts": 4, "hist": 1, "sel": 1.0, "compact_p": 0.0}
    panels = [("n_parts", "Partitions spanned"), ("hist", "History length (x 1-4 ops)"),
              ("sel", "Predicate selectivity (x)"), ("compact_p", "P(compaction on target)")]
    base = next(e for e in sens if e["knob"] == "all")
    plt = A._mpl()
    fig, axes = plt.subplots(2, 2, figsize=(A.COL_W, 2.9), sharey=True)
    fig.subplots_adjust(left=0.14, right=0.985, bottom=0.10, top=0.84, wspace=0.10, hspace=0.62)
    pols = ["snapmerge", "replay_intent_exact", "replay"]
    for ax, (knob, title) in zip(axes.flat, panels):
        pts = sorted([(default[knob], base)] + [(e["level"], e) for e in sens if e["knob"] == knob], key=lambda t: t[0])
        xs = list(range(len(pts)))
        for p in pols:
            col, mk, filled, lab = FAMILY[p]
            ys, lo, hi = [], [], []
            for _, e in pts:
                r = ((e.get("rates") or {}).get("rates") or {}).get(p)
                v = 100 * r["value"] if r and r.get("value") is not None else np.nan
                ys.append(v)
                lo.append(v - 100 * r["ci95"][0] if r and r.get("ci95") else 0)
                hi.append(100 * r["ci95"][1] - v if r and r.get("ci95") else 0)
            ax.errorbar(xs, ys, yerr=[lo, hi], fmt=mk + "-", ms=3.2, color=col, mfc=col if filled else "white", mec=col,
                        mew=0.8, lw=0.9, elinewidth=0.7, capsize=1.2, label=lab, zorder=3)
        ax.set_xticks(xs)
        ax.set_xticklabels([("%g" % lv) + ("*" if lv == default[knob] else "") for lv, _ in pts])
        ax.set_title(title, pad=2)
        ax.set_ylim(-5, 105)
        ax.set_yticks([0, 50, 100])
        ax.grid(axis="y", zorder=0)
    for ax in axes[:, 0]:
        ax.set_ylabel("Sound merges (%)")
    h, l = axes[0, 0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=2, handletextpad=0.3, columnspacing=0.8,
               borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    return {"file": A._relpath(out), "caption": (
        "Figure 7. Sensitivity of the sound auto-merge rate of commuting scenarios to the generator's partition span, "
        "history length, predicate selectivity and compaction frequency (one factor at a time; * = default, taken from "
        "the first 60 scenarios per mix of the main campaign; other levels 420 scenarios each). Whiskers: 95% bootstrap "
        "intervals.")}


def fig_contended(S: dict, out: Path) -> dict | None:
    rows = (S.get("revision") or {}).get("contended") or []
    sm = [r for r in rows if r["policy"] == "snapmerge" and r["estimate_read_rows_median"]]
    if len(sm) < 2:
        return None
    plt = A._mpl()
    fig, axes = plt.subplots(1, 2, figsize=(A.COL_W, 1.95))
    fig.subplots_adjust(left=0.13, right=0.985, bottom=0.22, top=0.80, wspace=0.42)
    series = [(A.CONTENDED, True, "o", "SF 1, k partitions"), (A.CONTENDED_SF10, True, "s", "SF 10, k partitions"),
              (A.CONTENDED_UNPART, False, "D", "SF 1, unpartitioned")]
    for camp, part, mk, lab in series:
        pts = sorted([r for r in sm if r["campaign"] == camp], key=lambda r: r["estimate_read_rows_median"])
        if not pts:
            continue
        x = [r["estimate_read_rows_median"] for r in pts]
        axes[0].plot(x, [r["latency_ms_median"] / 1e3 for r in pts], mk + "-", color=A.PAL["snapmerge"], ms=3.2, lw=0.9,
                     mfc=A.PAL["snapmerge"] if part else "white", mec=A.PAL["snapmerge"], label=lab)
        axes[1].plot(x, [max(r["rss_increment_gb_max"] or 0, 1e-3) for r in pts], mk + "-", color=A.PAL["snapmerge"],
                     ms=3.2, lw=0.9, mfc=A.PAL["snapmerge"] if part else "white", mec=A.PAL["snapmerge"])
        rp = {(r["campaign"], r["kind"]): r for r in rows if r["policy"] == "replay"}
        rx = [(r["estimate_read_rows_median"], rp[(camp, r["kind"])]["latency_ms_median"]) for r in pts
              if (camp, r["kind"]) in rp]
        if rx:
            axes[0].plot([a for a, _ in rx], [b / 1e3 for _, b in rx], mk, color=A.PAL["replay"], ms=3.0, mfc="white",
                         mec=A.PAL["replay"], label="Replay: time to false conflict" if camp == A.CONTENDED else None)
    for ax, yl in zip(axes, ("SnapMerge latency (s)", "Policy RSS increment (GB)")):
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Rows in contended partitions")
        ax.set_ylabel(yl)
        ax.grid(axis="y", zorder=0)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=2, handletextpad=0.3, columnspacing=0.8,
               borderaxespad=0.2)
    fig.savefig(out)
    plt.close(fig)
    return {"file": A._relpath(out), "caption": (
        "Figure 8. SnapMerge's cost grows with the rows of the contended partitions (x: preflight estimate from manifest "
        "metadata). Each point is the median of isolated runs (fresh process, warm page cache); right: peak resident "
        "memory of the merge process above its baseline after loading the table.")}


def figures(S: dict, figdir: Path) -> list[dict]:
    out = []
    for fn, name in ((fig_controls, "fig6_controls_by_mix.pdf"), (fig_sensitivity, "fig7_sensitivity.pdf"),
                     (fig_contended, "fig8_contended.pdf")):
        f = fn(S, figdir / name)
        if f:
            out.append(f)
    return out
