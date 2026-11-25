"""Merge-snapshot emission: two-reader round trip, sequence-number applicability, crash safety."""
import unittest

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

from helpers import C, Env, LogicalOp, X, month_pred, rows
from snapmerge.lineage import compute_lineage
from snapmerge.emit import emit_merge
from snapmerge.rules import RuleConfig, decide


class Crash(RuntimeError):
    pass


def crash_at(stage):
    def hook(s):
        if s == stage:
            raise Crash(s)
    return hook


class EmitRoundTrip(unittest.TestCase):
    def test_merged_snapshot_is_read_identically_by_both_readers(self):
        env = Env()
        try:
            a_rows = rows(10, 1000, q=30.0)
            a_rows = a_rows.set_column(3, "s", pa.array(["a"] * 10))
            env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"),
                      LogicalOp("APPEND", rows=a_rows))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 4.0), mode="mor"),
                      LogicalOp("DELETE", filter=X.cmp("s", "=", "x2")),
                      LogicalOp("APPEND", rows=rows(10, 2000, q=2.0)))
            self.assertFalse(env.serial()["true_conflict"])
            out = env.merge()
            self.assertEqual(out.verdict, "merged")
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["readers_agree"] and chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)
            props = C.summary_props(env.table.metadata.snapshot_by_id(out.snapshot_id))
            self.assertEqual(props["snapmerge.merge-source-head"], str(C.head_id(env.table, "C")))
            self.assertEqual(env.table.metadata.snapshot_by_id(out.snapshot_id).parent_snapshot_id,
                             C.head_id(env.table, "A"))
        finally:
            env.close()


class SequenceNumbers(unittest.TestCase):
    """Source-side data file g with a source-side position delete on g."""

    def setUp(self):
        self.env = Env()
        self.env.apply("A", LogicalOp("APPEND", rows=rows(5, 5000, q=1.0)))
        self.env.apply("C", LogicalOp("APPEND", rows=rows(20, 1000, q=2.0)),
                       LogicalOp("DELETE", filter=X.cmp("rid", "<", 1010), mode="mor"))
        self.env.serial()

    def tearDown(self):
        self.env.close()

    def _source_files(self):
        st = C.load_state(self.env.table, C.head_id(self.env.table, "C"))
        base = C.load_state(self.env.table, self.env.base_id)
        new = [f for p, f in st.files.items() if p not in base.files]
        return [f for f in new if f.content == "data"], [f for f in new if f.content == "pos"]

    def test_snapmerge_keeps_original_sequence_numbers(self):
        out = self.env.merge()
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)
        merged = C.load_state(self.env.table, out.snapshot_id)
        data, dels = self._source_files()
        for f in data + dels:
            self.assertEqual(merged.files[f.path].data_seq, f.data_seq)
        self.assertTrue(all(d.data_seq > data[0].data_seq for d in dels))

    def test_naive_readd_of_data_file_drops_source_deletes(self):
        """Re-adding g as ADDED (new, higher sequence number) while carrying its delete files with
        the old sequence number makes those deletes inapplicable under the v2 rule
        (data_seq <= delete_seq): the 10 deleted rows of g come back in PyIceberg.

        Characterisation of DuckDB 1.5.6 / iceberg extension 890b78a9c: it still applies the
        path-scoped delete (ignores the sequence-number condition), so the two readers disagree on
        such a snapshot.  SnapMerge never emits one (carried files keep their sequence numbers)."""
        data, dels = self._source_files()
        t = C.create_branch(self.env.table, "naive", C.head_id(self.env.table, "A"))
        st = C.load_state(t, C.head_id(t, "naive"))
        t, sid = C.commit_delta(t, "naive", st, remove_paths=set(), add_files=[f.data_file for f in data],
                                carry_entries=[d.entry for d in dels], operation="overwrite", properties={})
        self.env.table = t
        chk = self.env.check(sid)
        self.assertFalse(chk["pyiceberg_eq_o_ac"])
        self.assertEqual(chk["rows_pyiceberg"] - self.env.oracle.count("o_ac"), 10)
        self.assertFalse(chk["readers_agree"])  # DuckDB applies the stale delete anyway (see docstring)
        self.assertTrue(chk["duckdb_eq_o_ac"])

    def test_equal_sequence_numbers_still_apply(self):
        """Boundary of the v2 rule (data_seq <= delete_seq): re-adding both in one snapshot is correct."""
        data, dels = self._source_files()
        t = C.create_branch(self.env.table, "both", C.head_id(self.env.table, "A"))
        st = C.load_state(t, C.head_id(t, "both"))
        t, sid = C.commit_delta(t, "both", st, remove_paths=set(),
                                add_files=[f.data_file for f in data] + [d.data_file for d in dels],
                                operation="overwrite", properties={})
        self.env.table = t
        chk = self.env.check(sid)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)


class CrashSafety(unittest.TestCase):
    def setUp(self):
        self.env = Env()
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0), mode="mor"))
        self.env.serial()

    def tearDown(self):
        self.env.close()

    def test_crash_between_manifest_write_and_catalog_commit_leaves_no_partial_merge(self):
        for stage in ("after_delete_files", "after_manifest_list", "before_catalog_commit"):
            with self.subTest(stage=stage):
                t = self.env.table.refresh()
                if "m" not in t.metadata.refs:
                    t = C.create_branch(t, "m", C.head_id(t, "A"))
                self.env.table = t
                before_head = C.head_id(t, "m")
                n_snaps = len(t.metadata.snapshots)
                meta_loc = t.metadata_location
                with self.assertRaises(Crash):
                    self.env.merge(crash_hook=crash_at(stage))
                t2 = self.env.cat.load_table("t.t")
                self.assertEqual(C.head_id(t2, "m"), before_head)
                self.assertEqual(len(t2.metadata.snapshots), n_snaps)
                self.assertEqual(t2.metadata_location, meta_loc)
                self.env.table = t2
                self.env.oracle.apply("o_a", self.env.base, self.env.ops["A"])
                chk = self.env.check(before_head, ["o_a"])
                self.assertTrue(chk["duckdb_eq_o_a"] and chk["pyiceberg_eq_o_a"])
        out = self.env.merge()  # retry after the crashes succeeds
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)

    def test_concurrent_target_update_rejects_the_merge(self):
        t = C.create_branch(self.env.table, "m", C.head_id(self.env.table, "A"))
        lin = compute_lineage(t, "m", "C")
        dec = decide(lin, RuleConfig(), t.schema(), t.schema().as_arrow())
        self.assertEqual(dec.verdict, "merge")
        t.append(rows(3, 9000).cast(t.schema().as_arrow()), branch="m")  # concurrent writer
        t = t.refresh()
        concurrent = C.head_id(t, "m")
        with self.assertRaises(CommitFailedException):
            emit_merge(t, "m", lin, dec.plan)
        t = self.env.cat.load_table("t.t")
        self.assertEqual(C.head_id(t, "m"), concurrent)


if __name__ == "__main__":
    unittest.main()
