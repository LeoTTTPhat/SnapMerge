#!/usr/bin/env python3
"""Counterexample campaign: minimise cases where baselines are unsound or over-conservative.

Sources (all on the independent fuzz schema rid/d/q/k/s of tests/test_adversarial_fuzz.py):
  * fuzz seeds of results/review_fuzz_v1 where replay / cherry_pick / rowlevel emitted a merge that
    equals NEITHER serial order (unsound), rebuilt with test_adversarial_fuzz.make_scenario(seed);
  * the hand-crafted identical-new-key-upsert case (tests/test_adversarial_edge.py);
  * replay over-conservative cases: the first --per-rule fuzz seeds per replay conflict rule where
    the two serial orders agree and SnapMerge merged soundly.
Property kept during minimisation (checked on every candidate with the pure-Python multiset
oracle, both readers and the writer check):
  unsound:            writer check passes; the policy merges; both readers agree; the merged rows
                      equal neither serial order (and SnapMerge, if it merges, equals both orders)
  overconservative:   writer check passes; the serial orders agree; replay reports a conflict;
                      SnapMerge merges and both readers equal the oracle
Greedy fixed-point minimisation (deterministic): drop whole operations; drop base rows (halving
chunks, then single rows); drop inserted rows of APPEND/OVERWRITE/UPSERT (UPSERT keys follow the
rows); replace AND/OR predicates by one argument; one base chunk.
Output: results/<campaign>/raw/<case>.json (atomic; a case whose JSON exists with the same
fingerprint is skipped), environment.json, summary.json.

Example:
  python scripts/minimize_counterexamples.py --campaign counterexamples_v1
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT / "src"))

import pyarrow as pa  # noqa: E402

import test_adversarial_edge as edge  # noqa: E402
import test_adversarial_fuzz as fz  # noqa: E402
from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.baselines import POLICIES  # noqa: E402
from snapmerge.envinfo import write_environment  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.writer import LogicalOp, apply_op, write_base  # noqa: E402

FUZZ_UNSOUND = {
    "replay": [439, 591, 604, 660, 753, 1609, 1847, 2684, 3188, 3649],
    "cherry_pick": [591, 604, 660, 753, 1609, 1847],
    "rowlevel": [1901, 2744, 2812, 2979, 3212, 3376, 3621, 3995],
}
REPORT_POLICIES = ["snapmerge", "replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "nessie_force",
                   "fast_forward"]


# ============================================================================ scenario model

class Case:
    def __init__(self, layout: str, base: list[dict], ops: dict[str, list[LogicalOp]], chunks: int):
        self.layout, self.base, self.ops, self.chunks = layout, base, ops, chunks

    def clone(self) -> "Case":
        return Case(self.layout, [dict(r) for r in self.base],
                    {s: [copy.deepcopy(o) for o in v] for s, v in self.ops.items()}, self.chunks)

    def size(self) -> tuple:
        rows = sum((o.rows.num_rows if o.rows is not None else 0) for v in self.ops.values() for o in v)
        atoms = sum(_atoms(o.filter) for v in self.ops.values() for o in v if o.filter is not None)
        return (sum(len(v) for v in self.ops.values()), len(self.base), rows, atoms, self.chunks)

    def to_json(self) -> dict:
        def op_json(o: LogicalOp) -> dict:
            d = {"kind": o.kind, "mode": o.mode}
            if o.filter is not None:
                d["filter"] = X.describe(o.filter)
                d["filter_ast"] = o.filter
            if o.keys is not None:
                d["keys"] = [int(k) for k in o.keys]
            if o.rows is not None:
                d["rows"] = fz.to_rows(o.rows)
            if o.dynamic:
                d["dynamic"] = True
            if o.sort_by:
                d["sort_by"] = o.sort_by
            return d

        return {"layout": self.layout, "partition": fz.LAYOUTS[self.layout], "chunks": self.chunks,
                "base": self.base, "ops": {s: [op_json(o) for o in self.ops[s]] for s in ("A", "C")}}


def _atoms(f) -> int:
    if not isinstance(f, dict):
        return 0
    if f.get("op") in ("and", "or"):
        return sum(_atoms(a) for a in f["args"])
    if f.get("op") == "not":
        return _atoms(f["arg"])
    return 1


def from_gen(g) -> Case:
    return Case(g.layout, [dict(r) for r in g.base], {s: [copy.deepcopy(o) for o in g.ops[s]] for s in ("A", "C")},
                g.chunks)


# ============================================================================ evaluation

def evaluate(case: Case, policies: list[str], work_root: Path) -> dict:
    work = Path(tempfile.mkdtemp(prefix="cx-", dir=work_root))
    oracle = Oracle()
    out = {"policies": {}}
    try:
        cat = C.open_catalog(work)
        base_t = fz.SCHEMA.empty_table() if not case.base else pa.Table.from_pylist(case.base, schema=fz.SCHEMA)
        table = C.create_table(cat, "t.t", fz.SCHEMA, fz.LAYOUTS[case.layout])
        table = write_base(table, base_t, chunks=case.chunks)
        bid = C.head_id(table, "main")
        table = C.create_branch(table, "A", bid)
        table = C.create_branch(table, "C", bid)
        for side in ("A", "C"):
            for i, op in enumerate(case.ops[side]):
                table = apply_op(table, side, copy.deepcopy(op), f"{side}{i}")
        ac = fz.bag(fz.py_apply(case.base, case.ops["A"] + case.ops["C"]))
        ca = fz.bag(fz.py_apply(case.base, case.ops["C"] + case.ops["A"]))
        out["true_conflict"] = ac != ca
        out["rows_ac"], out["rows_ca"] = sum(ac.values()), sum(ca.values())
        wc = True
        for side in ("A", "C"):
            exp = fz.bag(fz.py_apply(case.base, case.ops[side]))
            sid = C.head_id(table, side)
            wc = wc and fz.bag_arrow(C.scan_duckdb(oracle.con, table, sid)) == exp \
                and fz.bag_arrow(C.scan_pyiceberg(table, sid)) == exp
        out["writer_check"] = bool(wc)
        a_head = C.head_id(table, "A")
        for p in policies:
            br = "m_" + p
            table = C.create_branch(table, br, a_head)
            try:
                if p == "snapmerge":
                    o = snapmerge(table, br, "C")
                elif p == "rowlevel":
                    o = POLICIES[p](table, br, "C", con=oracle.con)
                else:
                    o = POLICIES[p](table, br, "C")
                if o.table is not None:
                    table = o.table
                pr = {"verdict": o.verdict, "rule": o.rule, "witness": o.witness}
                if o.emitted:
                    table = table.refresh()
                    rd = fz.bag_arrow(C.scan_duckdb(oracle.con, table, o.snapshot_id))
                    rp = fz.bag_arrow(C.scan_pyiceberg(table, o.snapshot_id))
                    pr.update({"readers_agree": rd == rp, "eq_ac": rd == ac and rp == ac,
                               "eq_ca": rd == ca and rp == ca, "vs_ac": fz.bag_diff(rd, ac),
                               "vs_ca": fz.bag_diff(rd, ca)})
                    pr["serial_equivalent"] = pr["readers_agree"] and (pr["eq_ac"] or pr["eq_ca"])
                    pr["merged_rows"] = sorted(([list(map(str, k)), v] for k, v in rd.items()), key=str)
                out["policies"][p] = pr
            except Exception as e:
                out["policies"][p] = {"verdict": "error", "error": repr(e)}
            table = table.refresh()
        out["oracle_rows_ac"] = sorted(([list(map(str, k)), v] for k, v in ac.items()), key=str)
        out["oracle_rows_ca"] = sorted(([list(map(str, k)), v] for k, v in ca.items()), key=str)
    except Exception as e:
        out["setup_error"] = repr(e)
    finally:
        oracle.close()
        shutil.rmtree(work, ignore_errors=True)
    return out


def interesting(kind: str, policy: str, ev: dict) -> bool:
    if "setup_error" in ev or not ev.get("writer_check"):
        return False
    pr = ev["policies"].get(policy, {})
    sm = ev["policies"].get("snapmerge", {})
    if kind == "unsound":
        if pr.get("verdict") != "merged" or not pr.get("readers_agree") or pr.get("serial_equivalent"):
            return False
        # SnapMerge must stay sound on the minimised case (it may report a conflict)
        return sm.get("verdict") == "conflict" or (sm.get("verdict") == "merged" and sm.get("serial_equivalent"))
    # overconservative
    return (not ev["true_conflict"] and pr.get("verdict") == "conflict" and sm.get("verdict") == "merged"
            and sm.get("serial_equivalent") and sm.get("eq_ac") and sm.get("eq_ca"))


# ============================================================================ minimisation

def _simplify_pred(f):
    """Candidate simplifications of a predicate (one level)."""
    if f.get("op") in ("and", "or"):
        return [a for a in f["args"]]
    if f.get("op") == "not":
        return []
    return []


def candidates(case: Case):
    # 1. drop whole operations
    for s in ("A", "C"):
        for i in range(len(case.ops[s])):
            c = case.clone()
            del c.ops[s][i]
            yield f"drop-op {s}{i}", c
    # 2. drop base rows: halving chunks, then single rows
    n = len(case.base)
    k = n // 2
    while k >= 1:
        for start in range(0, n, k):
            c = case.clone()
            del c.base[start:start + k]
            yield f"drop-base {start}:{start + k}", c
        k //= 2
    # 3. drop inserted rows
    for s in ("A", "C"):
        for i, op in enumerate(case.ops[s]):
            if op.rows is None or op.rows.num_rows == 0:
                continue
            for j in range(op.rows.num_rows):
                c = case.clone()
                o = c.ops[s][i]
                rows = fz.to_rows(o.rows)
                del rows[j]
                if o.kind == "UPSERT":
                    if not rows:
                        continue
                    o.keys = sorted({int(r["rid"]) for r in rows})
                o.rows = pa.Table.from_pylist(rows, schema=fz.SCHEMA) if rows else fz.SCHEMA.empty_table()
                yield f"drop-row {s}{i}[{j}]", c
    # 4. simplify predicates
    for s in ("A", "C"):
        for i, op in enumerate(case.ops[s]):
            if op.filter is None or op.dynamic or op.kind == "REPLACE":
                continue
            for alt in _simplify_pred(op.filter):
                c = case.clone()
                c.ops[s][i].filter = copy.deepcopy(alt)
                yield f"simplify-pred {s}{i}", c
    # 5. one base chunk
    if case.chunks > 1:
        c = case.clone()
        c.chunks = 1
        yield "one-chunk", c


def minimise(case: Case, kind: str, policy: str, work_root: Path, max_evals: int) -> tuple[Case, list[str], int]:
    steps: list[str] = []
    evals = 0
    changed = True
    while changed and evals < max_evals:
        changed = False
        for label, cand in candidates(case):
            if evals >= max_evals:
                break
            if cand.size() >= case.size():
                continue
            evals += 1
            ev = evaluate(cand, ["snapmerge", policy], work_root)
            if interesting(kind, policy, ev):
                case = cand
                steps.append(label)
                changed = True
                break
    return case, steps, evals


# ============================================================================ campaign

def case_list(per_rule: int) -> list[dict]:
    out = []
    for pol, seeds in FUZZ_UNSOUND.items():
        for s in seeds:
            out.append({"name": f"{pol}_unsound_fuzz{s}", "kind": "unsound", "policy": pol, "seed": s})
    out.append({"name": "replay_unsound_identical_new_key_upserts", "kind": "unsound", "policy": "replay",
                "explicit": "identical_new_key_upserts"})
    fp_dir = ROOT / "results" / "review_fuzz_v1" / "seeds"
    by_rule: dict[str, list[int]] = {}
    for f in sorted(fp_dir.glob("*.json")):
        r = json.loads(f.read_text())
        sm, rp = r["policies"]["snapmerge"], r["policies"]["replay"]
        if rp.get("outcome") == "FP" and sm.get("outcome") == "TN" and sm.get("commuting_equivalent"):
            by_rule.setdefault(rp.get("rule"), []).append(r["scenario"]["seed"])
    for rule in sorted(by_rule):
        for s in by_rule[rule][:per_rule]:
            out.append({"name": f"replay_overconservative_{rule.split(':')[-1]}_fuzz{s}", "kind": "overconservative",
                        "policy": "replay", "seed": s, "replay_rule": rule})
    return out


def build_case(spec: dict) -> Case:
    if spec.get("explicit") == "identical_new_key_upserts":
        row = edge.R(990, edge.D2, 1.0, 1, "b")
        return Case("month", [dict(r) for r in edge.B], {"A": [edge.ups([row], "mor")], "C": [edge.ups([row], "cow")]}, 2)
    return from_gen(fz.make_scenario(spec["seed"]))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", default="counterexamples_v1")
    ap.add_argument("--per-rule", type=int, default=2)
    ap.add_argument("--max-evals", type=int, default=400)
    ap.add_argument("--only", default="", help="comma-separated case names")
    ap.add_argument("--summarize", action="store_true")
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--work-root", default=str(ROOT / "work"))
    a = ap.parse_args(argv)
    rdir = Path(a.results_root) / a.campaign
    raw = rdir / "raw"
    code = [ROOT / "scripts" / "minimize_counterexamples.py", ROOT / "tests" / "test_adversarial_fuzz.py",
            ROOT / "tests" / "test_adversarial_edge.py"] + sorted((ROOT / "src" / "snapmerge").glob("*.py"))
    h = hashlib.sha256()
    for p in code:
        h.update(p.name.encode())
        h.update(p.read_bytes())
    code_fp = h.hexdigest()[:16]
    cfg = {"per_rule": a.per_rule, "max_evals": a.max_evals, "fuzz_unsound": FUZZ_UNSOUND}
    if not a.summarize:
        write_environment(rdir, campaign=a.campaign, config=cfg, code_fingerprint=code_fp, code_files=code,
                          source_files=[], argv=sys.argv if argv is None else list(argv))
        work_root = Path(a.work_root) / a.campaign
        work_root.mkdir(parents=True, exist_ok=True)
        delay = float(os.environ.get("MEMGUARD_DELAY_SECONDS", "0") or 0)
        only = set(x for x in a.only.split(",") if x)
        for spec in case_list(a.per_rule):
            if only and spec["name"] not in only:
                continue
            path = raw / f"{spec['name']}.json"
            fp = hashlib.sha256(json.dumps([code_fp, cfg, spec], sort_keys=True).encode()).hexdigest()[:24]
            if path.exists():
                try:
                    if json.loads(path.read_text()).get("fingerprint") == fp:
                        continue
                except json.JSONDecodeError:
                    pass
            t0 = time.perf_counter()
            rec = {"case": spec, "fingerprint": fp, "code_fingerprint": code_fp}
            try:
                case = build_case(spec)
                ev0 = evaluate(case, REPORT_POLICIES, work_root)
                rec["original"] = {"size": case.size(), "scenario": case.to_json(), "evaluation": ev0,
                                   "property_holds": interesting(spec["kind"], spec["policy"], ev0)}
                if rec["original"]["property_holds"]:
                    mcase, steps, evals = minimise(case, spec["kind"], spec["policy"], work_root, a.max_evals)
                    ev1 = evaluate(mcase, REPORT_POLICIES, work_root)
                    rec["minimised"] = {"size": mcase.size(), "scenario": mcase.to_json(), "evaluation": ev1,
                                        "steps": steps, "evaluations": evals,
                                        "property_holds": interesting(spec["kind"], spec["policy"], ev1)}
            except Exception as e:
                rec["error"] = repr(e)
                rec["traceback"] = traceback.format_exc()[-2000:]
            rec["seconds"] = round(time.perf_counter() - t0, 2)
            raw.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".json.tmp{os.getpid()}")
            tmp.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")
            os.replace(tmp, path)
            m = rec.get("minimised", {})
            print(f"{spec['name']}: holds={rec.get('original', {}).get('property_holds')} "
                  f"size {rec.get('original', {}).get('size')} -> {m.get('size')} "
                  f"({m.get('evaluations')} evals, {rec['seconds']} s)", flush=True)
            if delay:
                time.sleep(delay)
        shutil.rmtree(work_root, ignore_errors=True)
    recs = [json.loads(p.read_text()) for p in sorted(raw.glob("*.json"))] if raw.exists() else []
    summ = {"cases": len(recs), "errors": sum(1 for r in recs if "error" in r),
            "original_property_holds": sum(1 for r in recs if r.get("original", {}).get("property_holds")),
            "minimised_property_holds": sum(1 for r in recs if r.get("minimised", {}).get("property_holds")),
            "by_case": {r["case"]["name"]: {"kind": r["case"]["kind"], "policy": r["case"]["policy"],
                                            "original_size": r.get("original", {}).get("size"),
                                            "minimised_size": r.get("minimised", {}).get("size"),
                                            "true_conflict": r.get("minimised", {}).get("evaluation", {}).get("true_conflict"),
                                            "snapmerge": r.get("minimised", {}).get("evaluation", {}).get("policies", {})
                                            .get("snapmerge", {}).get("verdict")}
                        for r in recs},
            "size_fields": ["ops", "base_rows", "inserted_rows", "predicate_atoms", "base_chunks"]}
    rdir.mkdir(parents=True, exist_ok=True)
    tmp = rdir / f"summary.json.tmp{os.getpid()}"
    tmp.write_text(json.dumps(summ, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, rdir / "summary.json")
    print(json.dumps({k: summ[k] for k in ("cases", "errors", "original_property_holds", "minimised_property_holds")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
