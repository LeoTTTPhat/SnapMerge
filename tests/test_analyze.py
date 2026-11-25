"""Tests of analyze.py: the fingerprint gate, the statistics helpers and the runner cross-check.

Uses small copies of real campaign records in a temporary directory; the real results/ tree is only
read, never written.
"""
from __future__ import annotations

import json
import math
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import analyze as AN  # noqa: E402

SRC = ROOT / "data" / "tpch" / "sf0.1" / "lineitem.parquet"
MAIN = ROOT / "results" / AN.MAIN  # current reference campaign, read only


def _synthetic_campaign(dst: Path, kind: str, n: int) -> Path:
    """A campaign directory as the runners write it, produced with the CURRENT code fingerprint (the
    gate must verify it as exact), so these tests do not depend on frozen result directories."""
    files = AN.runner_code_files(kind)
    code_fp, _ = AN.code_fingerprint(files)
    d = dst / f"synthetic_{kind}"
    (d / "raw").mkdir(parents=True)
    sha = AN.sha256_file(SRC)
    if kind == "difftest":
        cfg = {"source": "tpch_lineitem", "mixes": ["deletes_mor"], "per_mix": n, "seed_base": 7,
               "policies": ["snapmerge"], "source_sha256": {SRC.name: sha}}
        for i in range(n):
            sid = f"tpch_lineitem-deletes_mor-{i:05d}"
            seed = AN.scenario_seed(7, "tpch_lineitem", "deletes_mor", i)
            rec = {"scenario": {"scenario_id": sid, "seed": seed}, "code_fingerprint": code_fp,
                   "fingerprint": AN.fp_difftest(code_fp, cfg, sid, seed), "policies": {"snapmerge": {}}}
            (d / "raw" / f"{sid}.json").write_text(json.dumps(rec))
    else:
        cfg = {"sizes": "tpch:0.1", "reps": n, "kinds": "disjoint", "policies": "snapmerge"}
        for rep in range(n):
            rec = {"size": "tpch:0.1", "kind": "disjoint", "rep": rep, "policy": "snapmerge",
                   "fingerprint": AN.fp_scale(code_fp, "tpch:0.1", "disjoint", rep, "snapmerge")}
            (d / "raw" / f"tpch_0.1__disjoint__r{rep}__snapmerge.json").write_text(json.dumps(rec))
    (d / "campaign.json").write_text(json.dumps({"campaign": d.name, "config": cfg, "code_fingerprint": code_fp}))
    (d / "environment.json").write_text(json.dumps({
        "config": cfg, "code_fingerprint": code_fp, "code_sha256": {p.name: AN.sha256_file(p) for p in files},
        "source_files": {str(SRC): {"sha256": sha, "bytes": SRC.stat().st_size}}}))
    return d


def _copy_campaign(src: Path, dst: Path, files: list[str]) -> Path:
    d = dst / ("copy_" + src.name)
    (d / "raw").mkdir(parents=True)
    for f in ("campaign.json", "environment.json"):
        if (src / f).exists():
            shutil.copy(src / f, d / f)
    for f in files:
        shutil.copy(src / "raw" / f, d / "raw" / f)
    return d


@unittest.skipUnless(SRC.exists(), "prepared TPC-H SF 0.1 not present")
class Gate(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.main = _synthetic_campaign(self.tmp / "src", "difftest", 3)
        self.scale = _synthetic_campaign(self.tmp / "src", "scale", 2)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_strip_function_exact(self):
        src = "a = 1\n\n\ndef f(x):\n    return x\n\n\ndef g():\n    pass\n"
        self.assertEqual(AN.strip_toplevel_function(src, "f"), "a = 1\n\n\ndef g():\n    pass\n")
        self.assertEqual(AN.strip_toplevel_function(src, "missing"), src)

    def test_difftest_records_accepted_and_tampering_rejected(self):
        names = sorted(p.name for p in (self.main / "raw").glob("*.json"))[:3]
        d = _copy_campaign(self.main, self.tmp, names)
        g = AN.gate_campaign(d)
        self.assertEqual(g["code"]["status"], "exact")
        self.assertTrue(g["sources"]["ok"])
        self.assertEqual(len(g["accepted"]), 3)
        self.assertEqual(g["status"], "accepted")
        # tamper: fingerprint, then seed
        r = json.loads((d / "raw" / names[0]).read_text())
        r["fingerprint"] = "0" * 24
        (d / "raw" / names[0]).write_text(json.dumps(r))
        r1 = json.loads((d / "raw" / names[1]).read_text())
        r1["scenario"]["seed"] += 1
        (d / "raw" / names[1]).write_text(json.dumps(r1))
        g = AN.gate_campaign(d)
        reasons = {x["file"]: x["reason"] for x in g["rejected"]}
        self.assertEqual(reasons[names[0]], "fingerprint mismatch")
        self.assertIn("not planned", reasons[names[1]])
        self.assertEqual(len(g["accepted"]), 1)
        self.assertEqual(g["status"], "partial")

    def test_changed_code_record_rejects_campaign(self):
        d = _copy_campaign(self.main, self.tmp, sorted(p.name for p in (self.main / "raw").glob("*.json"))[:1])
        meta = json.loads((d / "campaign.json").read_text())
        meta["code_fingerprint"] = "deadbeefdeadbeef"
        (d / "campaign.json").write_text(json.dumps(meta))
        g = AN.gate_campaign(d)
        self.assertEqual(g["code"]["status"], "mismatch")
        self.assertEqual(g["status"], "rejected")
        self.assertEqual(len(g["accepted"]), 0)

    def test_changed_source_sha_rejects_campaign(self):
        d = _copy_campaign(self.main, self.tmp, sorted(p.name for p in (self.main / "raw").glob("*.json"))[:1])
        env = json.loads((d / "environment.json").read_text())
        for v in env["source_files"].values():
            v["sha256"] = "0" * 64
        (d / "environment.json").write_text(json.dumps(env))
        g = AN.gate_campaign(d)
        self.assertFalse(g["sources"]["ok"])
        self.assertEqual(g["status"], "rejected")

    def test_scale_record_identity_checked(self):
        f = "tpch_0.1__disjoint__r0__snapmerge.json"
        d = _copy_campaign(self.scale, self.tmp, [f, "tpch_0.1__disjoint__r1__snapmerge.json"])
        g = AN.gate_campaign(d)
        self.assertEqual(g["code"]["status"], "exact")
        self.assertEqual(len(g["accepted"]), 2)
        r = json.loads((d / "raw" / f).read_text())
        r["rep"] = 3  # identity no longer matches the file name / fingerprint
        (d / "raw" / f).write_text(json.dumps(r))
        g = AN.gate_campaign(d)
        self.assertEqual(len(g["accepted"]), 1)
        self.assertEqual(len(g["rejected"]), 1)


class Stats(unittest.TestCase):
    def test_clopper_pearson_zero_events(self):
        lo, hi = AN.clopper_pearson(0, 1341)
        self.assertEqual(lo, 0.0)
        self.assertAlmostEqual(hi, 1 - 0.025 ** (1 / 1341), places=5)
        self.assertEqual(AN.clopper_pearson(5, 5)[1], 1.0)

    def test_stratified_weights_keep_stratum_sizes(self):
        strata = np.array(["a"] * 7 + ["b"] * 3)
        for W in AN.weight_chunks(strata, 40, seed=1, chunk=16):
            self.assertTrue(np.all(W[:, :7].sum(1) == 7))
            self.assertTrue(np.all(W[:, 7:].sum(1) == 3))

    def test_weighted_median_matches_numpy_for_unit_weights(self):
        rng = np.random.default_rng(0)
        vals = rng.normal(size=11)
        W = np.ones((2, 11))
        self.assertAlmostEqual(float(AN.weighted_medians(W, np.arange(11), vals)[0]), float(np.median(vals)))

    def test_bootstrap_ratio_point_and_paired_difference(self):
        strata = np.array(["x"] * 50)
        num = np.column_stack([np.r_[np.ones(30), np.zeros(20)], np.r_[np.ones(20), np.zeros(30)]])
        den = np.ones_like(num)
        R, _ = AN.bootstrap(strata, num, den, [], 2000, seed=3)
        self.assertAlmostEqual(float(np.mean(R[:, 0])), 0.6, delta=0.02)
        d = R[:, 0] - R[:, 1]  # paired: identical resamples -> difference is exactly 0.2 * (resampled share)
        self.assertGreater(float(np.quantile(d, 0.025)), 0)


class AuditFacts(unittest.TestCase):
    """Helpers added in the Finalize stage (analyze.audit_facts)."""

    def test_one_sided_clopper_pearson(self):
        e = AN.clopper_pearson_one_sided(0, 1341)
        self.assertAlmostEqual(e["upper_one_sided"], 1 - 0.05 ** (1 / 1341), places=5)
        self.assertLess(e["upper_one_sided"], e["two_sided_95"][1])
        self.assertAlmostEqual(AN.clopper_pearson_one_sided(1662, 1662)["lower_one_sided"], 0.05 ** (1 / 1662), places=5)

    def test_predicate_evaluator(self):
        row = {"rid": 5, "s": "a", "q": 1.5, "d": "2024-02-01"}
        f = {"op": "and", "args": [{"op": "cmp", "col": "rid", "cmp": ">", "val": 1},
                                   {"op": "or", "args": [{"op": "in", "col": "s", "vals": ["b"]},
                                                         {"op": "not", "arg": {"op": "cmp", "col": "d", "cmp": "<",
                                                                               "val": "2024-01-15"}}]}]}
        self.assertTrue(AN._pred_eval(f, row))
        self.assertFalse(AN._pred_eval({"op": "in", "col": "s", "vals": ["b"]}, row))
        self.assertFalse(AN._pred_eval({"op": "false"}, row))

    def test_append_only_cow_classification(self):
        base = [{"rid": 1, "k": 0, "s": "x"}]
        new = [{"rid": 7, "k": 1, "s": "y"}]
        up = {"kind": "UPSERT", "mode": "cow", "keys": [7], "rows": new}
        self.assertEqual(len(AN._append_only_cow_ops({"base": base, "ops": {"C": [up]}}, "C")), 1)
        self.assertEqual(AN._append_only_cow_ops({"base": base, "ops": {"C": [dict(up, mode="mor")]}}, "C"), [])
        self.assertEqual(AN._append_only_cow_ops({"base": base, "ops": {"C": [dict(up, keys=[1, 7])]}}, "C"), [])
        # a later COW upsert of a key the side itself appended earlier does match a row
        app = {"kind": "APPEND", "mode": "cow", "rows": new}
        self.assertEqual(AN._append_only_cow_ops({"base": base, "ops": {"C": [app, up]}}, "C"), [])


@unittest.skipUnless(MAIN.exists(), "campaign results not present")
class CrossCheck(unittest.TestCase):
    def test_counts_equal_runner_policy_metrics(self):
        files = []
        for mix in AN.MIXES:
            files += sorted((MAIN / "raw").glob(f"tpch_lineitem-{mix}-0000[0-1].json"))
        c = AN.analyse_difftest(AN.MAIN, files, 50, 7)
        sc, pols, policies = c.pop("_cross_check_inputs")
        cc = AN.cross_check_with_runner(AN.MAIN, sc, pols, policies, c["policies"])
        self.assertEqual(cc["status"], "ok", cc["mismatches"][:3])
        snap = c["policies"]["snapmerge"]["groups"]["all"]
        self.assertEqual(snap["counts"]["n"], 14)
        self.assertEqual(snap["counts"]["soundness_violations"], 0)
        e = snap["metrics"]["conflict_recall"]
        self.assertTrue(math.isclose(e["value"], 1.0))


if __name__ == "__main__":
    unittest.main()
