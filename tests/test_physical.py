"""Position-level regressions for physical composition (2026-10-05, review R2 MC5).

Each case is checked three ways: both readers against both serial orders (oracle), the per-position
liveness of the merged snapshot computed from its manifests and delete files (catalog.live_rows), and
the per-value multiplicities of Corollary 4.3 computed DIRECTLY from the formula
  N*(v) = [~P_A ~P_C] B(v) + [~P_C] I_A(v) + [~P_A] I_C(v) + [P_A P_C] I_A(v)
with the net operations of Lemma 4.1 (independent of the serial-order oracle and of the merge code).
It also checks that every delete file of the merged snapshot references a live data file and has a
sequence number at least that file's.  Cases: a contended partition that also holds an untouched
file; same-side MOR deletes on a file the side added; two compactions on one side; duplicate values
in different files; exact-equality reconciliation together with a compaction rebase.
"""
import unittest
from collections import Counter

import pyarrow as pa

from helpers import C, Env, LogicalOp, X, month_pred, rows

NAMES = ["rid", "d", "q", "s"]


def key(r: dict) -> tuple:
    return tuple(r[n] for n in NAMES)


def holds(pred, r: dict) -> bool:
    if pred is None or X.is_false(pred):
        return False
    t = pa.table({n: [r[n]] for n in NAMES}, schema=pa.schema([("rid", pa.int64()), ("d", pa.date32()),
                                                                 ("q", pa.float64()), ("s", pa.string())]))
    return bool(X.eval_arrow(pred, t).to_pylist()[0])


def net(ops):
    """Lemma 4.1: (list of predicates, Counter of surviving inserts)."""
    preds, ins = [], Counter()
    for op in ops:
        p = op.delete_pred()
        ins = Counter({k: c for k, c in ins.items() if not holds(p, dict(zip(NAMES, k)))})
        preds.append(p)
        r = op.insert_rows()
        if r is not None:
            ins.update(key(x) for x in r.to_pylist())
    return preds, ins


def n_star(base: pa.Table, ops_a, ops_c) -> Counter:
    pa_, ia = net(ops_a)
    pc_, ic = net(ops_c)
    B = Counter(key(x) for x in base.to_pylist())
    out = Counter()
    for v in set(B) | set(ia) | set(ic):
        r = dict(zip(NAMES, v))
        a, c = any(holds(p, r) for p in pa_), any(holds(p, r) for p in pc_)
        n = (B[v] if not a and not c else 0) + (ia[v] if not c else 0) + (ic[v] if not a else 0) + \
            (ia[v] if a and c else 0)
        if n:
            out[v] = n
    return out


class PhysicalComposition(unittest.TestCase):
    def run_case(self, base, ops_a, ops_c, chunks=2):
        env = Env(base, chunks=chunks)
        try:
            env.apply("A", *ops_a)
            env.apply("C", *ops_c)
            self.assertFalse(env.serial()["true_conflict"])
            out = env.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
            st = C.load_state(env.table, out.snapshot_id)
            live = Counter()
            for f in st.data_files():
                t, _ = C.live_rows(st, f)
                live.update(key(x) for x in t.select(NAMES).to_pylist())
            self.assertEqual(live, n_star(base, ops_a, ops_c))
            for d in st.delete_files():
                self.assertIn(d.ref_path, st.files, "dangling delete file")
                self.assertGreaterEqual(d.data_seq, st.files[d.ref_path].data_seq)
            return out
        finally:
            env.close()

    def test_contended_partition_with_an_untouched_file(self):
        base = rows(120)  # two base files per month partition
        # A rewrites only rows with q < 3 of month 1 (one COW rewrite per file holding them); C
        # MOR-deletes other rows of month 1, some in files A rewrote and some it did not touch.
        out = self.run_case(base, [LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 3.0)), mode="cow")],
                            [LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", ">", 40.0)), mode="mor")])
        self.assertGreater(out.details["plan"]["contended_partitions"], 0)

    def test_same_side_mor_deletes_on_a_file_the_side_added(self):
        self.run_case(rows(90), [LogicalOp("REPLACE", filter=month_pred(1), sort_by="q")],
                      [LogicalOp("APPEND", rows=rows(6, 5000, month=1)),
                       LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 2.0)), mode="mor")])

    def test_two_compactions_on_one_side(self):
        self.run_case(rows(90), [LogicalOp("REPLACE", filter=month_pred(2), sort_by="q"),
                                 LogicalOp("DELETE", filter=X.cmp("q", "=", 7.0), mode="mor"),
                                 LogicalOp("REPLACE", filter=month_pred(2), sort_by="rid")],
                      [LogicalOp("DELETE", filter=X.and_(month_pred(2), X.cmp("q", ">", 30.0)), mode="mor")])

    def test_duplicate_values_in_different_files(self):
        dup = rows(4, 100, q=5.0, month=1)
        base = pa.concat_tables([rows(30), dup, rows(30, 200), dup])  # the 4 rows twice, in both chunks
        self.run_case(base, [LogicalOp("REPLACE", filter=month_pred(1), sort_by="q")],
                      [LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 1.0)), mode="mor"),
                       LogicalOp("APPEND", rows=dup)])

    def test_exact_equality_reconciliation_with_compaction_rebase(self):
        same = rows(3, 9000, q=12.0, month=1)
        self.run_case(rows(90), [LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"),
                                 LogicalOp("UPSERT", rows=same, keys=[9000, 9001, 9002], mode="mor")],
                      [LogicalOp("UPSERT", rows=same, keys=[9000, 9001, 9002], mode="cow"),
                       LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 4.0)), mode="mor")])

    def test_cow_versus_cow_on_the_same_file(self):
        self.run_case(rows(90), [LogicalOp("DELETE", filter=X.and_(month_pred(3), X.cmp("q", "<", 10.0)), mode="cow")],
                      [LogicalOp("DELETE", filter=X.and_(month_pred(3), X.cmp("q", ">", 35.0)), mode="cow")])


if __name__ == "__main__":
    unittest.main()
