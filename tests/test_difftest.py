"""Randomized small-scenario differential tests against the oracle, and checkpoint crash/resume of
the campaign runners (difftest.py, scale.py) in subprocesses."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from helpers import ROOT, nyc_like, tpch_lineitem

sys.path.insert(0, str(ROOT))
import difftest  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.scenarios import MIXES, SourcePool, generate, scenario_seed  # noqa: E402
from snapmerge.sources import LINEITEM, NYC  # noqa: E402

PY = sys.executable


def run_many(pool, source, partition, per_mix, seed_base, base_rows, policies):
    tmp = Path(tempfile.mkdtemp(prefix="snapmerge-dt-"))
    oracle = Oracle()
    cfg = {"policies": policies, "record": True, "base_chunks": 2, "partition": list(partition)}
    recs = []
    try:
        for mix in MIXES:
            for i in range(per_mix):
                seed = scenario_seed(seed_base, source, mix, i)
                sc = generate(pool, mix, seed, base_rows, scenario_id=f"{mix}-{i}", source=source)
                recs.append(difftest.run_scenario(sc, cfg, tmp / f"{mix}-{i}", oracle))
    finally:
        oracle.close()
        shutil.rmtree(tmp, ignore_errors=True)
    return recs


class RandomizedDifferential(unittest.TestCase):
    def check(self, recs):
        for r in recs:
            sid = r["scenario"]["scenario_id"]
            for side in ("A", "C"):
                self.assertTrue(r["writer_check"][side]["ok"], (sid, side))
                self.assertTrue(r["writer_check"][side]["readers_agree"], (sid, side))
            sm = r["policies"]["snapmerge"]
            self.assertNotEqual(sm["verdict"], "error", (sid, sm.get("error")))
            if r["oracle"]["true_conflict"]:
                self.assertEqual(sm["verdict"], "conflict", (sid, "missed conflict"))
            if sm["verdict"] == "merged":
                self.assertTrue(sm["readers_agree"], sid)
                self.assertTrue(sm["eq_ac"] and sm["eq_ca"], (sid, sm["check"]))
            for p, rec in r["policies"].items():
                self.assertNotEqual(rec["verdict"], "error", (sid, p, rec.get("error")))

    def test_tpch_scenarios_all_mixes(self):
        pool = SourcePool(LINEITEM, tpch_lineitem(0.01))
        recs = run_many(pool, "tpch_lineitem", LINEITEM.partition, 3, 99, 500,
                        ["snapmerge", "replay", "rowlevel", "snapmerge[no_net_effect]"])
        self.assertEqual(len(recs), 3 * len(MIXES))
        self.check(recs)
        self.assertTrue(any(r["oracle"]["true_conflict"] for r in recs))
        self.assertTrue(any(not r["oracle"]["true_conflict"] for r in recs))

    def test_timestamp_day_partitioned_scenarios(self):
        pool = SourcePool(NYC, nyc_like())
        recs = run_many(pool, "nyc_yellow", NYC.partition, 1, 5, 400, ["snapmerge", "replay"])
        self.check(recs)


class CheckpointResume(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="snapmerge-ck-"))
        self.args = [PY, str(ROOT / "difftest.py"), "--campaign", "ck", "--sf", "0.01", "--allow-generate",
                     "--mixes", "deletes_mor,compaction_on_main", "--per-mix", "2", "--base-rows", "300",
                     "--policies", "replay", "--results-root", str(self.tmp / "results"),
                     "--work-root", str(self.tmp / "work")]

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cmd(self, extra_env=None, args=None):
        env = dict(os.environ)
        env.update(extra_env or {})
        return subprocess.run(args or self.args, capture_output=True, text=True, env=env, timeout=600)

    def files(self):
        d = self.tmp / "results" / "ck" / "raw"
        return {p.name: p.read_bytes() for p in sorted(d.glob("*.json"))} if d.exists() else {}

    def test_crash_after_checkpoint_then_resume(self):
        r = self.run_cmd({"SNAPMERGE_CRASH_AFTER": "2"})
        self.assertEqual(r.returncode, 3, r.stderr[-2000:])
        first = self.files()
        self.assertEqual(len(first), 2)
        self.assertFalse(list((self.tmp / "results" / "ck" / "raw").glob("*.tmp*")))
        r = self.run_cmd({"MEMGUARD_BATCH_SCALE": "0.5", "MEMGUARD_DELAY_SECONDS": "0.01"})
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])
        self.assertIn("skipped=2", r.stdout)
        second = self.files()
        self.assertEqual(len(second), 4)
        for name, blob in first.items():
            self.assertEqual(second[name], blob)  # completed configurations are never rewritten
        r = self.run_cmd()
        self.assertIn("new=0 skipped=4", r.stdout)

    def test_crash_inside_a_scenario_reruns_it_from_its_seed(self):
        r = self.run_cmd({"SNAPMERGE_CRASH_MID_AT": "1"})
        self.assertEqual(r.returncode, 3)
        self.assertEqual(len(self.files()), 0)
        r = self.run_cmd()
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])
        self.assertEqual(len(self.files()), 4)

    def test_changed_config_refuses_to_reuse_results_directory(self):
        r = self.run_cmd({"SNAPMERGE_CRASH_AFTER": "1"})
        before = self.files()
        args = list(self.args)
        args[args.index("--base-rows") + 1] = "301"
        r = self.run_cmd(args=args)
        self.assertEqual(r.returncode, 2)
        self.assertIn("ABORT", r.stderr)
        self.assertEqual(self.files(), before)

    def test_summary(self):
        self.run_cmd()
        r = self.run_cmd(args=self.args[:4] + ["--summarize", "--results-root", str(self.tmp / "results")])
        s = json.loads(r.stdout)
        self.assertEqual(s["scenarios"], 4)
        self.assertIn("snapmerge", s["by_policy"])


class ScaleRunner(unittest.TestCase):
    def test_scale_smoke_and_resume(self):
        tmp = Path(tempfile.mkdtemp(prefix="snapmerge-scale-"))
        try:
            args = [PY, str(ROOT / "scale.py"), "--campaign", "sc", "--sizes", "tpch:0.01", "--reps", "1",
                    "--allow-generate", "--duckdb-memory-gb", "1", "--work-root", str(tmp / "work"),
                    "--results-root", str(tmp / "results")]
            env = dict(os.environ, SNAPMERGE_CRASH_AFTER="2")
            r = subprocess.run(args, capture_output=True, text=True, env=env, timeout=600)
            self.assertEqual(r.returncode, 3, r.stderr[-2000:])
            r = subprocess.run(args, capture_output=True, text=True, timeout=600)
            self.assertEqual(r.returncode, 0, r.stderr[-2000:])
            self.assertIn("done: wrote 4 records", r.stdout)
            recs = [json.loads(p.read_text()) for p in (tmp / "results" / "sc" / "raw").glob("*.json")]
            self.assertEqual(len(recs), 6)
            for rec in recs:
                self.assertNotEqual(rec["verdict"], "error", rec.get("error"))
                if rec["verdict"] == "merged":
                    self.assertTrue(rec["check"]["serial_equivalent"], rec)
            ov = [r for r in recs if r["kind"] == "overlap"]
            self.assertEqual({r["policy"]: r["verdict"] for r in ov}["replay"], "conflict")
            self.assertEqual({r["policy"]: r["verdict"] for r in ov}["snapmerge"], "merged")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
