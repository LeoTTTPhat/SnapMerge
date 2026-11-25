"""One test per commutativity rule (commute and conflict cases); every merge is checked
against the serial-order oracle through both readers."""
import unittest

import pyarrow as pa

from helpers import C, Env, LogicalOp, RuleConfig, X, month_pred, rows


def with_q(t: pa.Table, q: float) -> pa.Table:
    return t.set_column(2, "q", pa.array([q] * t.num_rows))


def upsert(keys, q):
    t = with_q(rows(len(keys), 0), q)
    return LogicalOp("UPSERT", rows=t.set_column(0, "rid", pa.array(keys, pa.int64())), keys=list(keys),
                     mode="cow")


class RuleCase(unittest.TestCase):
    def setUp(self):
        self.env = Env()

    def tearDown(self):
        self.env.close()

    def merged_ok(self, cfg=None):
        s = self.env.serial()
        self.assertFalse(s["true_conflict"], s)
        out = self.env.merge(cfg)
        self.assertEqual(out.verdict, "merged", out.to_json())
        chk = self.env.check(out.snapshot_id)
        self.assertTrue(chk["readers_agree"], chk)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)
        return out

    def conflict(self, rule_prefix=None, true_conflict=True, cfg=None):
        s = self.env.serial()
        self.assertEqual(s["true_conflict"], true_conflict, s)
        out = self.env.merge(cfg)
        self.assertEqual(out.verdict, "conflict", out.to_json())
        self.assertIsNone(out.snapshot_id)
        if rule_prefix:
            self.assertTrue(out.rule.startswith(rule_prefix), out.rule)
        self.assertEqual(C.head_id(self.env.table, "m"), C.head_id(self.env.table, "A"))
        return out


class R1AppendAppend(RuleCase):
    def test_commute_without_data_reads(self):
        self.env.apply("A", LogicalOp("APPEND", rows=rows(10, 1000)))
        self.env.apply("C", LogicalOp("APPEND", rows=rows(10, 2000)))
        out = self.merged_ok()
        self.assertEqual(out.stats.data_bytes_read, 0)

    def test_identical_appends_both_kept(self):
        r = rows(5, 1000)
        self.env.apply("A", LogicalOp("APPEND", rows=r))
        self.env.apply("C", LogicalOp("APPEND", rows=r))
        self.merged_ok()


class R2AppendVsDelete(RuleCase):
    def test_commute_by_bounds(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0), mode="mor"))
        self.env.apply("C", LogicalOp("APPEND", rows=with_q(rows(10, 1000), 1.0)))
        out = self.merged_ok()
        self.assertEqual(out.stats.data_bytes_read, 0)
        self.assertGreater(out.details["hits"].get("bounds:cannot-match", 0), 0)

    def test_commute_by_bounded_read(self):
        # bounds of the appended file straddle the predicate but no row matches it
        t = rows(4, 1000)
        t = t.set_column(2, "q", pa.array([1.0, 49.0, 2.0, 48.0]))
        t = t.set_column(3, "s", pa.array(["y", "y", "z", "z"]))
        self.env.apply("A", LogicalOp("DELETE", filter=X.and_(X.cmp("q", ">", 45.0), X.cmp("s", "=", "y0")),
                                      mode="cow"))
        self.env.apply("C", LogicalOp("APPEND", rows=t))
        out = self.merged_ok()
        self.assertGreater(out.details["hits"].get("bounded-read:clear", 0), 0)

    def test_conflict_with_witness_row(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0)))
        self.env.apply("C", LogicalOp("APPEND", rows=with_q(rows(3, 1000), 47.0)))
        out = self.conflict("R2")
        self.assertEqual(out.witness["example_row"]["q"], 47.0)
        self.assertEqual(out.conflict_class, "logical")

    def test_no_bounded_reads_turns_maybe_into_conflict(self):
        t = rows(2, 1000, month=1).set_column(2, "q", pa.array([1.0, 49.0]))
        # s bounds of the appended file are [x0, x1]; "x0a" lies inside, so bounds say "maybe"
        self.env.apply("A", LogicalOp("DELETE", filter=X.and_(X.cmp("q", ">", 45.0), X.cmp("s", "=", "x0a"))))
        self.env.apply("C", LogicalOp("APPEND", rows=t))
        self.conflict(true_conflict=False, cfg=RuleConfig(bounded_reads=False))


class R3DeleteDelete(RuleCase):
    def test_mor_mor_union_on_the_same_files(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 10.0), mode="mor"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("s", "=", "x3"), mode="mor"))
        out = self.merged_ok()
        self.assertGreater(out.details["hits"].get("R3:delete-file-union", 0), 0)
        self.assertEqual(out.stats.data_bytes_read, 0)

    def test_cow_cow_same_files_converted_to_position_deletes(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 10.0)))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("s", "=", "x3")))
        out = self.merged_ok()
        self.assertGreater(out.details["hits"].get("R3:cow-to-position", 0), 0)

    def test_cow_vs_mor_same_files(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 10.0)))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("s", "=", "x3"), mode="mor"))
        self.merged_ok()

    def test_delete_union_disabled_is_a_conflict(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 10.0), mode="mor"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("s", "=", "x3"), mode="mor"))
        out = self.conflict(true_conflict=False, cfg=RuleConfig(delete_union=False))
        self.assertEqual(out.conflict_class, "ablation")


class R4UpsertUpsert(RuleCase):
    def test_disjoint_keys_commute(self):
        self.env.apply("A", upsert([1, 2, 3], 91.0))
        self.env.apply("C", upsert([10, 11, 5000], 92.0))
        self.merged_ok()

    def test_shared_key_conflicts_with_key_witness(self):
        self.env.apply("A", upsert([1, 2, 3], 91.0))
        self.env.apply("C", upsert([3, 4], 92.0))
        out = self.conflict("R4")
        self.assertEqual(out.witness["key"], 3)

    def test_identical_upserts_commute_and_are_not_duplicated(self):
        self.env.apply("A", upsert([1, 2, 3], 91.0))
        self.env.apply("C", upsert([1, 2, 3], 91.0))
        out = self.merged_ok()
        self.assertGreater(out.details["hits"].get("net:exact-equal", 0), 0)


class R5OverwriteOverwrite(RuleCase):
    def test_commute(self):
        self.env.apply("A", LogicalOp("OVERWRITE", filter=X.cmp("s", "=", "x1"), rows=with_q(rows(3, 1000), 1.0)))
        c_rows = with_q(rows(3, 2000), 2.0).set_column(3, "s", pa.array(["y"] * 3))
        self.env.apply("C", LogicalOp("OVERWRITE", filter=X.cmp("q", ">", 45.0), rows=c_rows, mode="mor"))
        # A inserts q = 1.0 rows (not q > 45); C inserts s = "y" rows (not s = x1)
        self.merged_ok()

    def test_conflict(self):
        self.env.apply("A", LogicalOp("OVERWRITE", filter=X.cmp("s", "=", "x1"), rows=with_q(rows(3, 1000), 46.0)))
        self.env.apply("C", LogicalOp("OVERWRITE", filter=X.cmp("q", ">", 45.0), rows=with_q(rows(3, 2000), 2.0)))
        self.conflict("R5")


class R6Replace(RuleCase):
    def test_target_compaction_rebases_source_position_deletes(self):
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0), mode="mor"))
        out = self.merged_ok()
        self.assertGreater(out.details["hits"].get("R6:compaction-rebase", 0), 0)
        self.assertEqual(out.details["plan"]["layouts"], ["A"])

    def test_source_compaction_keeps_source_layout(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0), mode="mor"))
        self.env.apply("C", LogicalOp("REPLACE", filter=X.or_(month_pred(1), month_pred(2)), sort_by="q"))
        out = self.merged_ok()
        self.assertEqual(out.details["plan"]["layouts"], ["C"])

    def test_compaction_vs_copy_on_write_rewrite(self):
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(2), sort_by="s"))
        self.env.apply("C", LogicalOp("OVERWRITE", filter=X.and_(month_pred(2), X.cmp("q", "<", 20.0)),
                                      rows=with_q(rows(2, 3000, month=2), 30.0)))
        self.merged_ok()

    def test_both_sides_compact_the_same_partition(self):
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(3), sort_by="q"))
        self.env.apply("C", LogicalOp("REPLACE", filter=month_pred(3), sort_by="s"),
                       LogicalOp("DELETE", filter=X.cmp("q", "<", 4.0), mode="mor"))
        self.merged_ok()

    def test_rebase_disabled_is_a_conflict(self):
        self.env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "<", 3.0), mode="mor"))
        out = self.conflict(true_conflict=False, cfg=RuleConfig(compaction_rebase=False))
        self.assertEqual(out.rule, "R6:compaction-rebase-disabled")


class R7Structural(RuleCase):
    def test_schema_change_is_a_structural_conflict(self):
        from pyiceberg.types import LongType

        with self.env.table.update_schema() as u:
            u.add_column("extra", LongType())
        self.env.table = self.env.table.refresh()
        t = rows(3, 1000).append_column("extra", pa.array([1, 2, 3], pa.int64()))
        self.env.table.append(t, branch="C")
        self.env.table = self.env.table.refresh()
        self.env.table = C.create_branch(self.env.table, "m", C.head_id(self.env.table, "A"))
        from helpers import snapmerge

        out = snapmerge(self.env.table, "m", "C")
        self.assertEqual(out.verdict, "conflict")
        self.assertEqual(out.conflict_class, "structural")

    def test_equality_deletes_are_a_structural_conflict(self):
        from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

        t = self.env.table
        st = C.load_state(t, C.head_id(t, "C"))
        some = st.data_files()[0].data_file
        eq = DataFile.from_args(content=DataFileContent.EQUALITY_DELETES,
                                file_path=some.file_path.replace(".parquet", "-eq.parquet"),
                                file_format=FileFormat.PARQUET, partition=some.partition, record_count=1,
                                file_size_in_bytes=1, column_sizes={}, value_counts={}, null_value_counts={},
                                nan_value_counts={}, lower_bounds={}, upper_bounds={}, key_metadata=None,
                                split_offsets=None, equality_ids=[1], sort_order_id=None)
        eq.spec_id = some.spec_id
        t, _ = C.commit_delta(t, "C", st, remove_paths=set(), add_files=[eq], operation="delete", properties={})
        self.env.table = C.create_branch(t, "m", C.head_id(t, "A"))
        from helpers import snapmerge

        out = snapmerge(self.env.table, "m", "C")
        self.assertEqual(out.conflict_class, "structural")
        self.assertIn("equality", " ".join(out.witness["reasons"]))

    def test_unknown_predicate_in_filelevel_mode(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0)))
        self.env.apply("C", LogicalOp("APPEND", rows=with_q(rows(3, 1000), 1.0)))
        out = self.conflict("R7", true_conflict=False, cfg=RuleConfig(mode="filelevel"))
        self.assertEqual(out.conflict_class, "logical")

    def test_filelevel_mode_still_merges_delete_delete(self):
        self.env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 10.0)))
        self.env.apply("C", LogicalOp("DELETE", filter=X.cmp("s", "=", "x3"), mode="mor"))
        self.merged_ok(cfg=RuleConfig(mode="filelevel"))


class NetEffect(RuleCase):
    def test_masked_insert_commutes_but_pairwise_check_conflicts(self):
        pred = X.cmp("q", ">", 45.0)
        self.env.apply("A", LogicalOp("DELETE", filter=pred))
        self.env.apply("C", LogicalOp("APPEND", rows=with_q(rows(3, 1000), 47.0)),
                       LogicalOp("DELETE", filter=pred, mode="mor"))
        self.merged_ok()
        self.env.table = C.create_branch(self.env.table, "m2", C.head_id(self.env.table, "A"))
        from helpers import snapmerge

        out = snapmerge(self.env.table, "m2", "C", RuleConfig(net_effect=False))
        self.assertEqual(out.verdict, "conflict")


if __name__ == "__main__":
    unittest.main()
