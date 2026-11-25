import unittest

from helpers import C, Env, LogicalOp, X, month_pred, rows


def upsert_rows(keys, q=99.0):
    t = rows(len(keys), 0, q=q)
    import pyarrow as pa

    return t.set_column(0, "rid", pa.array(keys, pa.int64()))


CASES = {
    "append": LogicalOp("APPEND", rows=rows(20, 1000)),
    "delete_cow": LogicalOp("DELETE", filter=X.cmp("q", ">", 40.0)),
    "delete_mor": LogicalOp("DELETE", filter=X.cmp("q", ">", 40.0), mode="mor"),
    "delete_nothing": LogicalOp("DELETE", filter=X.cmp("q", ">", 1000.0)),
    "overwrite_cow": LogicalOp("OVERWRITE", filter=X.cmp("s", "=", "x1"), rows=rows(5, 2000, q=1.0)),
    "overwrite_mor": LogicalOp("OVERWRITE", filter=X.cmp("s", "=", "x1"), rows=rows(5, 2000, q=1.0), mode="mor"),
    "overwrite_dynamic": LogicalOp("OVERWRITE", filter=month_pred(2), rows=rows(7, 3000, month=2), dynamic=True),
    "upsert_cow": LogicalOp("UPSERT", rows=upsert_rows([1, 2, 3, 9999]), keys=[1, 2, 3, 9999]),
    "upsert_mor": LogicalOp("UPSERT", rows=upsert_rows([1, 2, 3, 9999]), keys=[1, 2, 3, 9999], mode="mor"),
    "replace": LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"),
}


class WriterOps(unittest.TestCase):
    def test_every_operation_matches_the_oracle_in_both_readers(self):
        for name, op in CASES.items():
            with self.subTest(op=name):
                env = Env()
                try:
                    env.apply("A", op)
                    env.oracle.apply("o_a", env.base, env.ops["A"])
                    head = C.head_id(env.table, "A")
                    chk = env.check(head, ["o_a"])
                    self.assertTrue(chk["readers_agree"], chk)
                    self.assertTrue(chk["duckdb_eq_o_a"] and chk["pyiceberg_eq_o_a"], chk)
                    props = C.summary_props(env.table.metadata.snapshot_by_id(head))
                    self.assertEqual(props["snapmerge.op"], op.kind)
                    self.assertEqual(props["snapmerge.op-id"], "A0")
                    if op.kind in ("DELETE", "OVERWRITE"):
                        self.assertEqual(X.loads(props["snapmerge.filter"]), op.filter)
                    if op.kind == "UPSERT":
                        self.assertIn("9999", props["snapmerge.keys"])
                finally:
                    env.close()

    def test_noop_delete_commits_a_marker_snapshot(self):
        env = Env()
        try:
            before = C.head_id(env.table, "A")
            env.apply("A", CASES["delete_nothing"])
            snap = env.table.metadata.snapshot_by_id(C.head_id(env.table, "A"))
            self.assertNotEqual(snap.snapshot_id, before)
            self.assertEqual(C.summary_props(snap).get("snapmerge.marker"), "true")
        finally:
            env.close()

    def test_mor_delete_writes_file_scoped_position_deletes(self):
        env = Env()
        try:
            env.apply("A", CASES["delete_mor"])
            st = C.load_state(env.table, C.head_id(env.table, "A"))
            dels = st.delete_files()
            self.assertTrue(dels)
            for d in dels:
                self.assertEqual(d.content, "pos")
                self.assertIn(d.ref_path, st.files)
                self.assertGreaterEqual(d.data_seq, st.files[d.ref_path].data_seq)
        finally:
            env.close()

    def test_replace_preserves_rows_and_drops_deletes_of_the_group(self):
        env = Env()
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "<", 5.0), mode="mor"),
                      LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
            st = C.load_state(env.table, C.head_id(env.table, "A"))
            for d in st.delete_files():
                self.assertIn(d.ref_path, st.files)
            env.oracle.apply("o_a", env.base, env.ops["A"])
            chk = env.check(C.head_id(env.table, "A"), ["o_a"])
            self.assertTrue(chk["duckdb_eq_o_a"] and chk["pyiceberg_eq_o_a"], chk)
        finally:
            env.close()

    def test_unrecorded_writer_records_no_logical_metadata(self):
        env = Env(record=False)
        try:
            env.apply("A", CASES["delete_cow"])
            props = C.summary_props(env.table.metadata.snapshot_by_id(C.head_id(env.table, "A")))
            self.assertFalse(any(k.startswith("snapmerge.") for k in props))
        finally:
            env.close()


if __name__ == "__main__":
    unittest.main()
