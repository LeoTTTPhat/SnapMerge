import unittest

import pyarrow as pa

from helpers import C, Env, LogicalOp, Oracle, X, rows
from snapmerge.lineage import compute_lineage, lca


class Lineage(unittest.TestCase):
    def test_lca_and_side_operations(self):
        env = Env()
        try:
            env.apply("A", LogicalOp("OVERWRITE", filter=X.cmp("q", "<", 5.0), rows=rows(3, 1000, q=7.0)))
            env.apply("C", LogicalOp("APPEND", rows=rows(4, 2000)), LogicalOp("DELETE", filter=X.cmp("q", ">", 48.0),
                                                                                   mode="mor"))
            t = env.table
            self.assertEqual(lca(t, C.head_id(t, "A"), C.head_id(t, "C")), env.base_id)
            lin = compute_lineage(t, "A", "C")
            self.assertEqual([o.kind for o in lin.ops["A"]], ["OVERWRITE"])
            ow = lin.ops["A"][0]
            # PyIceberg copy-on-write overwrite = rewrite snapshot(s) + one append snapshot, one logical op
            self.assertGreaterEqual(len(ow.snaps), 2)
            self.assertEqual(sum(f.record_count for f in ow.insert_files()), 3)
            self.assertEqual([o.kind for o in lin.ops["C"]], ["APPEND", "DELETE"])
            self.assertTrue(lin.deltas["C"].added_deletes)
            self.assertFalse(lin.structural)
        finally:
            env.close()

    def test_lca_follows_recorded_merge_parent(self):
        env = Env()
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(3, 1000)))
            env.apply("C", LogicalOp("APPEND", rows=rows(3, 2000)))
            out = env.merge(target="A")  # merge C into A itself
            self.assertEqual(out.verdict, "merged")
            c_head = C.head_id(env.table, "C")
            self.assertEqual(lca(env.table, out.snapshot_id, c_head), c_head)
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("rid", "=", 2000), mode="mor"))
            lin = compute_lineage(env.table, "A", "C")
            self.assertEqual(lin.base_id, c_head)
            self.assertEqual([o.kind for o in lin.ops["C"]], ["DELETE"])
        finally:
            env.close()

    def test_unrecorded_snapshots_become_unknown_operations(self):
        env = Env(record=False)
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 5.0)))
            env.apply("C", LogicalOp("APPEND", rows=rows(2, 2000)))
            lin = compute_lineage(env.table, "A", "C")
            self.assertTrue(all(o.kind.startswith("?") for o in lin.ops["A"] + lin.ops["C"]))
            self.assertIsNone(lin.ops["A"][0].delete_pred())
        finally:
            env.close()


class OracleTests(unittest.TestCase):
    def test_multiset_semantics_and_true_conflict(self):
        o = Oracle()
        base = rows(10)
        dup = pa.concat_tables([base, base.slice(0, 2)])
        a = [LogicalOp("DELETE", filter=X.cmp("q", ">", 7.0))]
        c = [LogicalOp("APPEND", rows=rows(2, 100, q=9.0))]
        s = o.serial(dup, a, c)
        self.assertTrue(s["true_conflict"])
        self.assertEqual(s["rows_base"], 12)
        self.assertEqual(s["rows_ac"], 12 - 2 + 2)  # q in {8, 9} deleted, then 2 appended
        self.assertEqual(s["rows_ca"], 12 - 2)       # appended q=9 rows deleted too
        self.assertEqual(o.diff_tables("o_ac", "o_ca").to_json(), {"only_left": 2, "only_right": 0})
        # duplicates are counted: base has rows 0 and 1 twice
        o.load("x", dup)
        o.load("y", base)
        self.assertEqual(o.diff_tables("x", "y").to_json(), {"only_left": 2, "only_right": 0})
        o.close()

    def test_commuting_scenario(self):
        o = Oracle()
        base = rows(10)
        s = o.serial(base, [LogicalOp("DELETE", filter=X.cmp("q", ">", 7.0))],
                     [LogicalOp("APPEND", rows=rows(2, 100, q=1.0))])
        self.assertFalse(s["true_conflict"])
        o.close()


if __name__ == "__main__":
    unittest.main()
