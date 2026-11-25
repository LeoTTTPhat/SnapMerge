"""Nullable regression matrix (2026-10-05, review R2 MC3 / F5).

A recorded predicate f denotes P_f(v) = [f(v) IS TRUE]; "not P_f" is its Boolean complement.  These
tests put NULLs into filterable columns (the prepared TPC-H and NYC sources have none) and check the
writer, SnapMerge's decision and composition, and the oracle: null-bearing inserted rows, masking by a
predicate that is NULL on the row, bounds exclusion of all-NULL files, exact-equality comparison of
rows containing NULL, and matching inside a contended partition.  It also documents why the writer
hands PyIceberg expr.is_true_form: PyIceberg 0.12's copy-on-write delete keeps every row with a NULL
in a referenced column, which differs from SQL for disjunctions.
"""
import datetime as dt
import unittest

import duckdb
import pyarrow as pa

from helpers import C, Env, LogicalOp, X, month_pred
from snapmerge.writer import apply_op

SCHEMA = pa.schema([("rid", pa.int64()), ("d", pa.date32()), ("x", pa.int64()), ("y", pa.float64()),
                    ("s", pa.string())])


def nrows(n, off=0, x=None, y=None, month=None, all_x=False):
    xs = [x if all_x else (None if i % 3 == 0 else (x if x is not None else i % 5)) for i in range(n)]
    ys = [y if y is not None else (None if i % 4 == 1 else float(i % 7)) for i in range(n)]
    return pa.table({"rid": pa.array(range(off, off + n), pa.int64()),
                     "d": pa.array([dt.date(2024, month or 1 + i % 3, 1 + i % 28) for i in range(n)], pa.date32()),
                     "x": pa.array(xs, pa.int64()), "y": pa.array(ys, pa.float64()),
                     "s": pa.array([None if i % 5 == 2 else f"v{i % 4}" for i in range(n)], pa.string())},
                    schema=SCHEMA)


PREDS = {
    "x = 1": X.cmp("x", "=", 1),
    "x != 1": X.cmp("x", "!=", 1),
    "NOT x = 1": X.not_(X.cmp("x", "=", 1)),
    "x = 1 OR y = 2": X.or_(X.cmp("x", "=", 1), X.cmp("y", "=", 2.0)),
    "NOT (x = 1 AND y = 2)": X.not_(X.and_(X.cmp("x", "=", 1), X.cmp("y", "=", 2.0))),
    "x IN (1,3)": X.isin("x", [1, 3]),
    "NOT x IN (1,3)": X.not_(X.isin("x", [1, 3])),
    "s = 'v1' OR x IS NULL": X.or_(X.cmp("s", "=", "v1"), X.is_null("x")),
}


class PredicateSemantics(unittest.TestCase):
    def test_arrow_sql_and_two_valued_form_agree(self):
        t = nrows(40)
        con = duckdb.connect()
        con.register("t", t)
        for name, f in PREDS.items():
            with self.subTest(pred=name):
                sql = [r[0] for r in con.execute(f"SELECT coalesce(({X.to_sql(f, SCHEMA)}), false) FROM t").fetchall()]
                self.assertEqual(X.eval_arrow(f, t).to_pylist(), sql)
                self.assertEqual(X.eval_arrow(X.is_true_form(f), t).to_pylist(), sql)
                tf = [r[0] for r in con.execute(f"SELECT ({X.to_sql(X.is_true_form(f), SCHEMA)}) FROM t").fetchall()]
                self.assertNotIn(None, tf)  # two-valued
        con.close()

    def test_writer_cow_delete_matches_sql_for_every_predicate(self):
        for name, f in PREDS.items():
            with self.subTest(pred=name):
                env = Env(nrows(60), partition=("d", "month"))
                try:
                    env.apply("A", LogicalOp("DELETE", filter=f, mode="cow"))
                    env.oracle.apply("o_a", env.base, env.ops["A"])
                    chk = env.check(C.head_id(env.table, "A"), ["o_a"])
                    self.assertTrue(chk["duckdb_eq_o_a"] and chk["pyiceberg_eq_o_a"], chk)
                finally:
                    env.close()

    def test_pyiceberg_plain_delete_differs_on_disjunctions_with_nulls(self):
        """Documents the discrepancy the v2 writer avoids: the schema-v1 writer passes the predicate
        unchanged, and PyIceberg keeps rows with a NULL in any referenced column."""
        env = Env(nrows(60), partition=("d", "month"))
        try:
            f = PREDS["x = 1 OR y = 2"]
            env.table = apply_op(env.table, "A", LogicalOp("DELETE", filter=f, mode="cow"), "A0", schema_version=1)
            env.oracle.apply("o_a", env.base, [LogicalOp("DELETE", filter=f, mode="cow")])
            chk = env.check(C.head_id(env.table, "A"), ["o_a"])
            self.assertFalse(chk["duckdb_eq_o_a"])
            self.assertGreater(chk["vs_o_a"]["only_left"], 0)  # rows SQL deletes that PyIceberg kept
        finally:
            env.close()


class MergeWithNulls(unittest.TestCase):
    def _merged_ok(self, env):
        out = env.merge()
        self.assertEqual(out.verdict, "merged", out.to_json())
        chk = env.check(out.snapshot_id)
        self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
        return out

    def test_null_bearing_inserts_do_not_satisfy_a_comparison(self):
        env = Env(nrows(90))
        try:
            env.apply("A", LogicalOp("APPEND", rows=nrows(6, 5000, all_x=True, x=None, month=1)))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("x", "=", 1), mode="mor"))
            self.assertFalse(env.serial()["true_conflict"])
            self._merged_ok(env)
        finally:
            env.close()

    def test_masking_by_a_predicate_that_is_null_on_the_row(self):
        for later, conflict in ((X.not_(X.cmp("x", "=", 1)), True), (X.is_null("x"), False)):
            with self.subTest(later=X.describe(later)):
                env = Env(nrows(90))
                try:
                    r = nrows(1, 6000, all_x=True, x=None, y=5.5, month=2)
                    env.apply("A", LogicalOp("DELETE", filter=X.cmp("y", "=", 5.5), mode="cow"))
                    env.apply("C", LogicalOp("APPEND", rows=r), LogicalOp("DELETE", filter=later, mode="mor"))
                    self.assertEqual(env.serial()["true_conflict"], conflict)
                    out = env.merge()
                    if conflict:
                        self.assertEqual(out.category, "logical-conflict", out.to_json())
                        self.assertEqual(out.witness["example_row"]["rid"], 6000)
                    else:
                        self.assertEqual(out.verdict, "merged", out.to_json())
                        self.assertTrue(env.check(out.snapshot_id)["duckdb_eq_o_ac"])
                finally:
                    env.close()

    def test_all_null_file_is_excluded_by_bounds(self):
        env = Env(nrows(90))
        try:
            env.apply("A", LogicalOp("APPEND", rows=nrows(8, 7000, all_x=True, x=None, month=3)))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("x", "=", 3), mode="cow"))
            self.assertFalse(env.serial()["true_conflict"])
            out = self._merged_ok(env)
            self.assertGreaterEqual(out.details["hits"].get("bounds:cannot-match", 0), 1)
            self.assertEqual(out.details["hits"].get("bounded-read", 0), 0)
        finally:
            env.close()

    def test_exact_equality_of_rows_containing_null(self):
        env = Env(nrows(60))
        try:
            same = nrows(2, 8000, all_x=True, x=None, y=1.0, month=1)
            env.apply("A", LogicalOp("UPSERT", rows=same, keys=[8000, 8001], mode="mor"))
            env.apply("C", LogicalOp("UPSERT", rows=same, keys=[8000, 8001], mode="cow"))
            self.assertFalse(env.serial()["true_conflict"])
            out = self._merged_ok(env)
            self.assertGreaterEqual(out.details["hits"].get("net:exact-equal", 0), 1)
        finally:
            env.close()

    def test_contended_partition_matching_with_nulls(self):
        env = Env(nrows(150))
        try:
            env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="y"))
            env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("y", "<", 3.0)), mode="mor"),
                      LogicalOp("DELETE", filter=X.and_(month_pred(1), X.is_null("s")), mode="cow"))
            self.assertFalse(env.serial()["true_conflict"])
            out = self._merged_ok(env)
            self.assertGreater(out.details["plan"]["matched_rows"], 0)
        finally:
            env.close()


if __name__ == "__main__":
    unittest.main()
