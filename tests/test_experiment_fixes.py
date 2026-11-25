"""Regression tests for the Experiment-stage fixes of the review findings (2026-10-02).

  F3  replay stages every replayed snapshot (own sequence number, parent chain) in ONE transaction
      and publishes them with ONE catalog commit; nothing is visible before that commit.
  F4  row-level merge streams its result in fixed chunks (chunk boundaries inside partitions are
      exercised with a tiny chunk) and the scale checks stream both readers into DuckDB and detect a
      wrong snapshot (the check is not vacuous).
  F1  scale.build_base slices exact fixed chunks: the per-file record counts do not depend on the
      read batch (MEMGUARD_BATCH_SCALE), also when chunks straddle read batches.
  F11 rules.match_rows matches -0.0 with 0.0 (SQL / Arrow equality).
  F2  difftest records the policy order and the load average with every policy record.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from helpers import ROOT, C, Env, LogicalOp, X, month_pred, rows  # noqa: F401

PY = sys.executable


class ReplaySingleTransaction(unittest.TestCase):
    def _env(self):
        env = Env()
        env.apply("A", LogicalOp("APPEND", rows=rows(3, 1000, month=1)))
        env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(3), X.cmp("q", ">", 47.0)), mode="mor"),
                  LogicalOp("APPEND", rows=rows(3, 2000, month=2)),
                  LogicalOp("APPEND", rows=rows(2, 3000, month=2)))
        env.table = C.create_branch(env.table, "m", C.head_id(env.table, "A"))
        return env

    def test_one_catalog_commit_and_correct_chain(self):
        from snapmerge.baselines import replay

        env = self._env()
        try:
            self.assertFalse(env.serial()["true_conflict"])
            before_log = len(env.table.metadata.metadata_log)
            before_snaps = len(env.table.metadata.snapshots)
            a_head = C.head_id(env.table, "A")
            out = replay(env.table, "m", "C")
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertEqual(out.details["commits"], 1)
            self.assertEqual(out.details["replayed_snapshots"], 3)
            t = out.table.refresh()
            self.assertEqual(len(t.metadata.metadata_log), before_log + 1)  # ONE new metadata version
            self.assertEqual(len(t.metadata.snapshots), before_snaps + 3)
            chain = C.ancestors(t, out.snapshot_id)[:4]
            self.assertEqual(chain[3].snapshot_id, a_head)  # replayed snapshots hang off the target head
            seqs = [s.sequence_number for s in reversed(chain[:3])]
            self.assertEqual(seqs, sorted(set(seqs)))  # distinct, increasing sequence numbers
            env.table = t
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
        finally:
            env.close()

    def test_failed_catalog_commit_leaves_target_unchanged(self):
        from snapmerge import baselines as B

        env = self._env()
        try:
            a_head = C.head_id(env.table, "A")
            orig = C.commit_staged

            def boom(tx, crash_hook=None):
                raise C.CommitAborted("simulated crash before the catalog commit")

            B.C.commit_staged = boom
            try:
                with self.assertRaises(C.CommitAborted):
                    B.replay(env.table, "m", "C")
            finally:
                B.C.commit_staged = orig
            t = env.cat.load_table("t.t")
            self.assertEqual(C.head_id(t, "m"), a_head)  # no replayed snapshot became visible
        finally:
            env.close()


class RowlevelChunkedWrite(unittest.TestCase):
    def test_tiny_chunks_inside_partitions_give_the_oracle_rows(self):
        from snapmerge import baselines as B

        env = Env(base=rows(120))
        old = B.ROWLEVEL_WRITE_CHUNK_ROWS
        B.ROWLEVEL_WRITE_CHUNK_ROWS = 7
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(9, 1000, month=1)))
            env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(2), X.cmp("q", "<", 20.0)), mode="mor"))
            self.assertFalse(env.serial()["true_conflict"])
            env.table = C.create_branch(env.table, "m", C.head_id(env.table, "A"))
            out = B.rowlevel(env.table, "m", "C", con=env.oracle.con)
            self.assertEqual(out.verdict, "merged", out.to_json())
            env.table = out.table.refresh()
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
            n_rows = chk["rows_duckdb"]
            self.assertEqual(out.details["new_data_files"] >= -(-n_rows // 7), True, out.details)
            st = C.load_state(env.table, out.snapshot_id)
            self.assertEqual(st.delete_files(), [])  # full overwrite: no delete file survives
        finally:
            B.ROWLEVEL_WRITE_CHUNK_ROWS = old
            env.close()


class ScaleChecksAndLayout(unittest.TestCase):
    def test_streaming_check_detects_a_wrong_snapshot_and_accepts_the_right_one(self):
        sys.path.insert(0, str(ROOT))
        import scale as S
        from snapmerge.oracle import Oracle

        env = Env(base=rows(90))
        d = Path(tempfile.mkdtemp(prefix="adv-chk-"))
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(5, 1000, month=1)))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0), mode="mor"))
            base_parquet = d / "base.parquet"
            pq.write_table(env.base, base_parquet)
            o = Oracle(con=S.oracle_connection(d / "o.duckdb", 512, 2))
            schema = env.table.schema().as_arrow()
            o.apply_sql("o_ac", f"read_parquet('{base_parquet}')", schema, env.ops["A"] + env.ops["C"])
            o.apply_sql("o_ca", f"read_parquet('{base_parquet}')", schema, env.ops["C"] + env.ops["A"])
            tc = not o.diff_tables("o_ac", "o_ca").equal
            self.assertFalse(tc)
            out = env.merge()
            self.assertEqual(out.verdict, "merged")
            good = S.check_merge(o, env.table, out.snapshot_id, tc, 2, 2)
            self.assertTrue(good["serial_equivalent"] and good["readers_agree"], good)
            self.assertEqual(set(good) >= {"duckdb_vs_ac", "duckdb_vs_ca", "pyiceberg_vs_ac", "pyiceberg_vs_ca"},
                             True)
            bad = S.check_merge(o, env.table, C.head_id(env.table, "A"), tc, 2, 2)  # A alone misses C's delete
            self.assertFalse(bad["serial_equivalent"], bad)
            self.assertGreater(bad["duckdb_vs_ac"]["only_left"], 0)
            o.close()
        finally:
            env.close()
            shutil.rmtree(d, ignore_errors=True)

    def test_build_base_exact_chunks_independent_of_read_batch(self):
        sys.path.insert(0, str(ROOT))
        import scale as S
        from snapmerge.sources import SPECS, tpch_path

        spec = SPECS["tpch_lineitem"]
        f = tpch_path(0.01, "lineitem")
        d = Path(tempfile.mkdtemp(prefix="adv-base-"))
        old = S.BASE_CHUNK_ROWS
        S.BASE_CHUNK_ROWS = 7_777  # chunks straddle read batches of every tested scale
        try:
            layouts = []
            for sc in (1.0, 0.25, 0.013):
                cat = C.open_catalog(d / f"s{sc}")
                t = C.create_table(cat, "sm.t", pq.read_schema(f), spec.partition)
                info = S.build_base(t, [f], spec, sc)
                t = cat.load_table("sm.t")
                st = C.load_state(t, info["snapshot_id"])
                layouts.append((info["append_chunks"], info["rows"],
                                sorted((lf.partition_key, lf.record_count) for lf in st.data_files())))
            self.assertEqual(layouts[0], layouts[1])
            self.assertEqual(layouts[0], layouts[2])
            self.assertEqual(layouts[0][0], -(-pq.ParquetFile(f).metadata.num_rows // 7_777))
        finally:
            S.BASE_CHUNK_ROWS = old
            shutil.rmtree(d, ignore_errors=True)


class GroupedPyIcebergReader(unittest.TestCase):
    def test_grouped_readers_equal_the_single_reader_with_deletes(self):
        """F4 follow-up: the scale check reads PyIceberg in bounded groups of scan tasks."""
        env = Env(base=rows(150), chunks=3)
        try:
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", ">", 40.0), mode="mor"),
                      LogicalOp("APPEND", rows=rows(7, 5000, month=2)))
            sid = C.head_id(env.table, "C")
            whole = C.scan_pyiceberg(env.table, sid)
            for g in (1, 2, 100):
                parts = [r.read_all() for r in C.pyiceberg_reader_groups(env.table, sid, g)]
                if g == 1:
                    self.assertGreater(len(parts), 1)
                got = pa.concat_tables(parts)
                env.oracle._schema = whole.schema
                self.assertTrue(env.oracle.compare(got, env.oracle.load("_whole", whole)).equal, g)
                self.assertEqual(got.num_rows, whole.num_rows)
        finally:
            env.close()


class NegativeZero(unittest.TestCase):
    def test_match_rows_matches_negative_zero(self):
        from snapmerge.rules import match_rows

        li, ri = match_rows(pa.table({"x": [0.0, 1.0, -0.0]}), pa.table({"x": [-0.0, 1.0, 0.0]}))
        self.assertEqual(len(li), 3)


class DifftestRecordsOrderAndLoad(unittest.TestCase):
    def test_policy_records_carry_order_and_loadavg(self):
        tmp = Path(tempfile.mkdtemp(prefix="adv-order-"))
        try:
            args = [PY, str(ROOT / "difftest.py"), "--campaign", "o", "--sf", "0.01", "--per-mix", "1",
                    "--base-rows", "300", "--mixes", "deletes_mor", "--variants", "full",
                    "--policies", "replay,rowlevel", "--order", "fixed",  # default is random since 2026-10-05
                    "--results-root", str(tmp / "r"), "--work-root", str(tmp / "w")]
            r = subprocess.run(args, capture_output=True, text=True, timeout=600)
            self.assertEqual(r.returncode, 0, r.stderr[-2000:])
            rec = json.loads(next((tmp / "r" / "o" / "raw").glob("*.json")).read_text())
            self.assertEqual([rec["policies"][p]["order"] for p in ("snapmerge", "replay", "rowlevel")], [0, 1, 2])
            for p in rec["policies"].values():
                self.assertIn("loadavg_1m", p)
            env = json.loads((tmp / "r" / "o" / "environment.json").read_text())
            self.assertIn("lineitem.parquet", " ".join(env["source_files"]))
            self.assertIn("pyiceberg", env["packages"])
            cfg = json.loads((tmp / "r" / "o" / "campaign.json").read_text())["config"]
            self.assertIn("lineitem.parquet", cfg["source_sha256"])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
