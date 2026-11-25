"""Baselines and minimized counterexamples (replay unsound / over-conservative, FORCE loss)."""
import unittest

import pyarrow as pa

from helpers import C, Env, LogicalOp, X, month_pred, rows, snapmerge
from snapmerge.baselines import POLICIES


def with_q(t, q):
    return t.set_column(2, "q", pa.array([q] * t.num_rows))


class BaselineCase(unittest.TestCase):
    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def run_policy(self, name):
        t = C.create_branch(self.env.table, f"m_{name}", C.head_id(self.env.table, "A"))
        fn = POLICIES[name]
        out = fn(t, f"m_{name}", "C", con=self.env.oracle.con) if name == "rowlevel" else fn(t, f"m_{name}", "C")
        self.env.table = (out.table or t).refresh()
        return out


class ReplayCounterexamples(BaselineCase):
    def test_replay_is_unsound_when_filters_match_inserts_outside_touched_partitions(self):
        """A: DELETE q = 49.5 (no base row matches) ; APPEND r_A (month 1, q = 47.5)
           C: DELETE q = 47.5 (no base row matches) ; APPEND r_C (month 2, q = 49.5)
        Each delete touches no partition, so partition-level replay accepts; the merge keeps r_A
        and r_C, which equals neither serial order."""
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "=", 49.5)),
                       LogicalOp("APPEND", rows=with_q(rows(1, 1000, month=1), 47.5)))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "=", 47.5)),
                       LogicalOp("APPEND", rows=with_q(rows(1, 2000, month=2), 49.5)))
        s = self.env.serial()
        self.assertTrue(s["true_conflict"])
        out = self.run_policy("replay")
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertFalse(chk["duckdb_eq_o_ac"] or chk["duckdb_eq_o_ca"], chk)
        self.assertFalse(chk["pyiceberg_eq_o_ac"] or chk["pyiceberg_eq_o_ca"], chk)
        t = C.create_branch(self.env.table, "m_snap", C.head_id(self.env.table, "A"))
        sm = snapmerge(t, "m_snap", "C")
        self.assertEqual(sm.verdict, "conflict")
        self.assertIn("example_row", sm.witness)

    def test_replay_is_over_conservative_for_commuting_deletes_in_one_partition(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0)), mode="mor"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("s", "=", "x3")), mode="mor"))
        self.assertFalse(self.env.serial()["true_conflict"])
        self.assertEqual(self.run_policy("replay").verdict, "conflict")
        out = self.env.merge()
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"])

    def test_replay_merges_disjoint_partitions_correctly(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0))))
        self.env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(2), X.cmp("q", "<", 10.0)), mode="mor"),
                       LogicalOp("APPEND", rows=with_q(rows(5, 3000, month=3), 30.0)))
        self.assertFalse(self.env.serial()["true_conflict"])
        out = self.run_policy("replay")
        self.assertEqual(out.verdict, "merged")
        self.assertEqual(out.details["replayed_snapshots"], 2)
        self.assertEqual(out.details["commits"], 1)  # #13959: one transaction, one catalog commit
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)


class PointerPolicies(BaselineCase):
    def test_nessie_force_silently_loses_target_rows(self):
        self.env.apply("A", LogicalOp("APPEND", rows=rows(10, 1000)))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", ">", 47.0), mode="mor"))
        self.env.serial()
        out = self.run_policy("nessie_force")
        self.assertEqual(out.verdict, "merged")
        self.assertEqual(out.snapshot_id, C.head_id(self.env.table, "C"))
        chk = self.env.check(out.snapshot_id)
        self.assertEqual(chk["vs_o_ac"]["only_right"], 10)  # all of A's appended rows (q 0..9) are lost
        self.assertEqual(self.run_policy("nessie_normal").verdict, "conflict")

    def test_fast_forward(self):
        self.env.apply("C", LogicalOp("APPEND", rows=rows(3, 1000)))
        out = self.run_policy("fast_forward")
        self.assertEqual(out.verdict, "merged")
        self.assertEqual(out.snapshot_id, C.head_id(self.env.table, "C"))
        self.env.apply("A", LogicalOp("APPEND", rows=rows(3, 2000)))
        self.assertEqual(self.run_policy("fast_forward").verdict, "conflict")

    def test_lakefs_accepts_compaction_only_side(self):
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0)))
        self.env.serial()
        out = self.run_policy("lakefs")
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"])

    def test_cherry_pick_supports_appends_and_replace_partitions_only(self):
        self.env.apply("A", LogicalOp("APPEND", rows=rows(3, 1000, month=1)))
        self.env.apply("C", LogicalOp("APPEND", rows=rows(3, 2000, month=2)),
                       LogicalOp("OVERWRITE", filter=month_pred(3), rows=rows(4, 3000, month=3), dynamic=True))
        self.env.serial()
        out = self.run_policy("cherry_pick")
        self.assertEqual(out.verdict, "merged", out.to_json())
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"])
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0)))
        out = self.run_policy("cherry_pick")
        self.assertEqual(out.verdict, "conflict")
        self.assertTrue(out.rule.startswith("unsupported"))


class RowLevel(BaselineCase):
    def test_disjoint_changes_merge_with_full_read(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", ">", 47.0)))
        self.env.apply("C", LogicalOp("APPEND", rows=with_q(rows(3, 1000), 1.0)))
        self.env.serial()
        out = self.run_policy("rowlevel")
        self.assertEqual(out.verdict, "merged")
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)
        base = C.load_state(self.env.table, self.env.base_id)
        self.assertGreaterEqual(out.stats.data_bytes_read, sum(f.size for f in base.data_files()))
        self.assertGreater(out.stats.data_bytes_written, 0)

    def test_same_key_changed_differently_conflicts(self):
        k = rows(1, 5, q=91.0)
        self.env.apply("A", LogicalOp("UPSERT", rows=k, keys=[5]))
        self.env.apply("C", LogicalOp("UPSERT", rows=with_q(k, 92.0), keys=[5]))
        out = self.run_policy("rowlevel")
        self.assertEqual(out.verdict, "conflict")
        self.assertEqual(out.witness["key"], 5)


if __name__ == "__main__":
    unittest.main()
