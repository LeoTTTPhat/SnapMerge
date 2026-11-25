"""Adversarial review (stage Review, 2026-10-02): evaluation validity of the runners.

Hard properties (must pass):
  * the oracle module does not import the merge path (rules / lineage / emit / merge / baselines);
  * the scenario generator is deterministic across processes and PYTHONHASHSEED values;
  * difftest decisions are reproducible run-to-run and invariant to MEMGUARD_BATCH_SCALE /
    MEMGUARD_DELAY_SECONDS (only latency, timing, random snapshot ids, metadata byte counts,
    the row-level baseline's Parquet sizes and byte counts that include position delete files, whose
    data-file paths contain random UUIDs, may differ);
  * a corrupted (truncated) checkpoint plus a stale tmp file are recomputed identically and the
    other scenarios are skipped;
  * the scale runner refuses to reuse a campaign directory under a different --sizes/--kinds
    (so campaigns cannot be split by size under ONE campaign name; the ledger must use one name
    per invocation set).
Findings recorded as expected failures in the Review stage; all four were fixed in the Experiment
stage (2026-10-02) and these tests now run as ordinary tests:
  * scale.py's base-table layout depends on MEMGUARD_BATCH_SCALE (append chunk size), i.e. a guard
    restart changes the measured workload;
  * scale.py's serial-equivalence check omits PyIceberg-vs-(C;A);
  * difftest's summary drops `error` records from the recall denominator;
  * replay commits one snapshot per replayed source snapshot, while the #13959 proposal replays
    into ONE transaction (latency of replay includes extra catalog commits).
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from helpers import ROOT, Env, LogicalOp, X, rows  # noqa: F401

PY = sys.executable
DIFFTEST = ROOT / "difftest.py"
SCALE = ROOT / "scale.py"
VOLATILE = {"latency_ms", "timing_s", "snapshot_id", "created", "updated", "traceback", "memguard",
            "metadata_bytes_read", "metadata_bytes_written", "snapshot",
            # host telemetry added in the Experiment stage (F2/F16), not decisions:
            "loadavg_1m", "loadavg_1m_start", "loadavg_1m_after",
            # re-review 2026-10-07: the metadata file's size varies with random snapshot ids and timestamps
            "metadata_file_bytes",
            # 2026-10-08: position delete files store data-file paths, which contain random UUIDs, so their
            # compressed Parquet size varies by a few bytes between runs (seen: 26,579 vs 26,585 delete bytes
            # read by the row-level baseline).  Every byte count that includes delete-file sizes is volatile:
            # delete bytes, per-phase read bytes, SnapMerge's byte estimates (estimate.read_bytes and
            # estimate.phases.<phase>.bytes, from manifest file sizes) and the snapshot summaries' size
            # (Iceberg's own file-size properties).  Row and file counts and all decisions are still compared.
            "delete_bytes_read", "delete_bytes_written", "read_bytes_by_phase", "read_bytes", "bytes",
            "summary_bytes",
            # revision 2026-10-05: per-phase timings, random file names in witnesses, pinned snapshot ids
            "timing_ms", "file", "source_snapshot"}


def _strip(obj, policy: str | None = None):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in VOLATILE:
                continue
            if policy == "rowlevel" and k == "data_bytes_written":
                continue
            out[k] = _strip(v, k if k in ("rowlevel",) else policy)
        return out
    if isinstance(obj, list):
        return [_strip(x, policy) for x in obj]
    return obj


def _run_difftest(results: Path, work: Path, extra_env: dict | None = None, **kw) -> subprocess.CompletedProcess:
    args = [PY, str(DIFFTEST), "--campaign", "adv", "--sf", "0.01", "--per-mix", "1", "--base-rows", "300",
            "--mixes", "filter_corrections,compaction_on_main,adversarial_overlap,upsert_keys",
            "--variants", "full,no_net_effect", "--policies", "replay,cherry_pick,rowlevel",
            "--results-root", str(results), "--work-root", str(work)]
    env = dict(os.environ)
    env.pop("MEMGUARD_BATCH_SCALE", None)
    env.pop("MEMGUARD_DELAY_SECONDS", None)
    env.update(extra_env or {})
    return subprocess.run(args, capture_output=True, text=True, env=env, timeout=900)


def _records(results: Path) -> dict:
    return {p.name: json.loads(p.read_text()) for p in sorted((results / "adv" / "raw").glob("*.json"))}


class OracleIndependence(unittest.TestCase):
    def test_oracle_module_does_not_import_the_merge_path(self):
        tree = ast.parse((ROOT / "src" / "snapmerge" / "oracle.py").read_text())
        mods = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                mods.add((node.module or "") + ":" + ",".join(a.name for a in node.names))
            elif isinstance(node, ast.Import):
                mods.update(a.name for a in node.names)
        joined = " ".join(sorted(mods))
        for forbidden in ("rules", "lineage", "emit", "merge", "baselines", "scenarios"):
            self.assertNotIn(forbidden, joined, joined)
        # it uses only the logical operations (writer.LogicalOp), the SQL filter backend and the readers
        self.assertIn("writer:LogicalOp", joined)


class GeneratorDeterminism(unittest.TestCase):
    CODE = r"""
import hashlib, sys, io
sys.path.insert(0, %r)
import pyarrow as pa, pyarrow.ipc as ipc
from snapmerge.scenarios import MIXES, SourcePool, generate, scenario_seed
from snapmerge.sources import load_source
spec, src = load_source("tpch_lineitem", sf=0.01)
pool = SourcePool(spec, src)
h = hashlib.sha256()
def tb(t):
    s = io.BytesIO(); w = ipc.new_stream(s, t.schema); w.write_table(t.combine_chunks()); w.close(); return s.getvalue()
for mix in MIXES:
    for i in range(2):
        sc = generate(pool, mix, scenario_seed(20261002, "tpch_lineitem", mix, i), 500)
        h.update(repr(sc.summary()).encode()); h.update(tb(sc.base))
        for side in ("A", "C"):
            for op in sc.ops[side]:
                h.update(repr((op.kind, op.filter, op.keys, op.mode, op.dynamic, op.sort_by)).encode())
                if op.rows is not None:
                    h.update(tb(op.rows))
print(h.hexdigest())
"""

    def test_same_seed_same_scenarios_across_processes_and_hash_seeds(self):
        code = self.CODE % str(ROOT / "src")
        outs = set()
        for hs in ("1", "4242"):
            env = dict(os.environ, PYTHONHASHSEED=hs)
            r = subprocess.run([PY, "-c", code], capture_output=True, text=True, env=env, timeout=600)
            self.assertEqual(r.returncode, 0, r.stderr[-2000:])
            outs.add(r.stdout.strip())
        self.assertEqual(len(outs), 1, outs)


class RunnerReproducibility(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = Path(tempfile.mkdtemp(prefix="adv-eval-"))
        cls.r1 = _run_difftest(cls.dir / "r1", cls.dir / "w1")
        cls.r2 = _run_difftest(cls.dir / "r2", cls.dir / "w2",
                               {"MEMGUARD_BATCH_SCALE": "0.25", "MEMGUARD_DELAY_SECONDS": "0.05"})

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_decisions_reproducible_and_memguard_invariant(self):
        self.assertEqual(self.r1.returncode, 0, self.r1.stderr[-2000:])
        self.assertEqual(self.r2.returncode, 0, self.r2.stderr[-2000:])
        a, b = _records(self.dir / "r1"), _records(self.dir / "r2")
        self.assertEqual(sorted(a), sorted(b))
        self.assertEqual(len(a), 4)
        for k in a:
            self.assertEqual(_strip(a[k]), _strip(b[k]), k)
            self.assertEqual(b[k]["memguard"]["duckdb_memory_limit_mb"], 512)

    def test_corrupted_checkpoint_and_stale_tmp_are_recomputed_identically(self):
        res = self.dir / "r1" / "adv" / "raw"
        victim = sorted(res.glob("*.json"))[1]
        before = json.loads(victim.read_text())
        victim.write_text(victim.read_text()[:200])  # truncated JSON (e.g. disk full outside os.replace)
        (res / (victim.name + ".tmp99999")).write_text("{partial")
        r = _run_difftest(self.dir / "r1", self.dir / "w1")
        self.assertEqual(r.returncode, 0, r.stderr[-2000:])
        m = re.search(r"done: new=(\d+) skipped=(\d+)", r.stdout)
        self.assertEqual((int(m.group(1)), int(m.group(2))), (1, 3), r.stdout[-500:])
        after = json.loads(victim.read_text())
        self.assertEqual(_strip(before), _strip(after))


class ScaleRunner(unittest.TestCase):
    """Uses TPC-H SF 0.01 (prepared) in temp roots."""

    @classmethod
    def setUpClass(cls):
        cls.dir = Path(tempfile.mkdtemp(prefix="adv-scale-"))
        cls.base_args = [PY, str(SCALE), "--campaign", "adv", "--sizes", "tpch:0.01", "--reps", "1",
                         "--policies", "snapmerge,replay", "--results-root", str(cls.dir / "res"),
                         "--work-root", str(cls.dir / "work"), "--duckdb-memory-gb", "1"]
        cls.r1 = subprocess.run(cls.base_args + ["--kinds", "disjoint"], capture_output=True, text=True, timeout=900)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_campaign_config_is_frozen_so_size_splits_need_new_campaign_names(self):
        self.assertEqual(self.r1.returncode, 0, self.r1.stderr[-2000:])
        r2 = subprocess.run(self.base_args + ["--kinds", "overlap"], capture_output=True, text=True, timeout=900)
        self.assertEqual(r2.returncode, 2, r2.stdout + r2.stderr)
        self.assertIn("different code/config", r2.stderr)

    def test_serial_equivalence_check_covers_both_readers_for_both_orders(self):  # F5 fixed (stage Experiment)
        rec = json.loads((self.dir / "res" / "adv" / "raw" / "tpch_0.01__disjoint__r0__snapmerge.json").read_text())
        self.assertIn("pyiceberg_vs_ca", rec["check"], rec["check"])

    def test_base_layout_does_not_depend_on_memguard_batch_scale(self):  # F1 fixed (stage Experiment)
        """build_base() appends chunks of max(50k, 2M * MEMGUARD_BATCH_SCALE) rows: a guard restart at
        scale 0.5/0.25 rebuilds the base with more, smaller files per partition (a different workload)."""
        sys.path.insert(0, str(ROOT))
        import scale as S
        from snapmerge import catalog as C
        from snapmerge.sources import SPECS, tpch_path

        spec = SPECS["tpch_lineitem"]
        f = tpch_path(0.1, "lineitem")
        import pyarrow.parquet as pq

        counts = []
        for sc in (1.0, 0.25):
            d = self.dir / f"layout_{sc}"
            cat = C.open_catalog(d)
            t = C.create_table(cat, "sm.t", pq.read_schema(f), spec.partition)
            counts.append(S.build_base(t, [f], spec, sc)["data_files"])
        self.assertEqual(counts[0], counts[1], counts)


class SummaryDefinitions(unittest.TestCase):
    def test_recall_counts_errors_on_true_conflicts_as_misses(self):  # F6 fixed (stage Experiment)
        """plan.md: recall = share of true-conflict scenarios REPORTED as conflicts.  summarize()
        computes TP/(TP+FN) and drops `error` records, so an exception on a true conflict is invisible
        in recall (it is only counted under `errors`)."""
        sys.path.insert(0, str(ROOT))
        import difftest as D

        d = Path(tempfile.mkdtemp(prefix="adv-sum-"))
        try:
            for i, pol in enumerate(({"verdict": "conflict", "outcome": "TP"}, {"verdict": "error", "error": "x"})):
                rec = {"scenario": {"mix": "m"}, "oracle": {"true_conflict": True},
                       "writer_check": {"A": {"ok": True}, "C": {"ok": True}},
                       "policies": {"snapmerge": dict(pol, latency_ms=1.0, io={"data_bytes_read": 0,
                                                                                 "delete_bytes_read": 0,
                                                                                 "data_bytes_written": 0,
                                                                                 "delete_bytes_written": 0,
                                                                                 "metadata_bytes_written": 0})}}
                (d / "scenarios").mkdir(exist_ok=True)
                (d / "scenarios" / f"s{i}.json").write_text(json.dumps(rec))
            s = D.summarize(d)
            self.assertEqual(s["by_policy"]["snapmerge"]["conflict_recall"], 0.5, s["by_policy"]["snapmerge"])
        finally:
            shutil.rmtree(d, ignore_errors=True)


class ReplayFaithfulness(unittest.TestCase):
    def test_replay_commits_one_transaction(self):  # F3 fixed (stage Experiment)
        """Iceberg #13959 proposal (doc, retrieved 2026-10-02): 'Replay snapshot changes into one single
        transaction'.  baselines._replay_commit commits once per replayed snapshot, so replay's
        latency includes extra catalog commits (and intermediate states are visible)."""
        from snapmerge.baselines import replay

        env = Env()
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(3, 1000, month=1)))
            from helpers import month_pred

            env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(3), X.cmp("q", ">", 47.0)), mode="mor"),
                      LogicalOp("APPEND", rows=rows(3, 2000, month=2)))
            from helpers import C

            env.table = C.create_branch(env.table, "m", C.head_id(env.table, "A"))
            out = replay(env.table, "m", "C")
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertEqual(out.details.get("commits"), 1, out.details)
        finally:
            env.close()


if __name__ == "__main__":
    unittest.main()
