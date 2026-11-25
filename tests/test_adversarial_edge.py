"""Adversarial review (stage Review, 2026-10-02): hand-crafted edge cases.

Each case is an explicit (layout, base rows, operations of A and C) scenario run through the
same checker as the fuzzer (tests/test_adversarial_fuzz.py): the pure-Python multiset oracle,
both readers, every SnapMerge variant and every baseline.  Hard properties: P0-P5 (see the fuzz
module).  Cases marked `expect="merged"` commute and lie inside SnapMerge's rule set, so a
conflict there is a precision bug; `expect="conflict"` cases are true conflicts.
Also covers a concurrent source-branch commit injected between lineage computation and the
merge commit.
"""
from __future__ import annotations

import datetime as dt
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa

import test_adversarial_fuzz as fz
from helpers import C, LogicalOp, Oracle, X, snapmerge
from snapmerge.writer import apply_op, write_base

D1, D2, D3 = dt.date(2024, 1, 15), dt.date(2024, 2, 15), dt.date(2024, 3, 15)
JAN = X.and_(X.cmp("d", ">=", dt.date(2024, 1, 1)), X.cmp("d", "<", dt.date(2024, 2, 1)))
FEB = X.and_(X.cmp("d", ">=", dt.date(2024, 2, 1)), X.cmp("d", "<", dt.date(2024, 3, 1)))
L17 = "DELIVER IN PERSON"
L17b = "DELIVER IN PERSOO"


def R(rid, d=D1, q=0.0, k=0, s="a"):
    return {"rid": rid, "d": d, "q": float(q), "k": k, "s": s}


def T(*rows):
    return pa.Table.from_pylist(list(rows), schema=fz.SCHEMA) if rows else fz.SCHEMA.empty_table()


def base_rows(n=24):
    return [R(i, [D1, D2, D3][i % 3], float(i % 5) / 2, i % 5, ["a", "b", L17][i % 3]) for i in range(n)]


def app(*rows):
    return LogicalOp("APPEND", rows=T(*rows))


def dele(f, mode="cow"):
    return LogicalOp("DELETE", filter=f, mode=mode)


def ups(rows, mode="cow"):
    return LogicalOp("UPSERT", rows=T(*rows), keys=sorted({r["rid"] for r in rows}), mode=mode, key_col="rid")


def ovw(f, rows, mode="cow", dynamic=False):
    return LogicalOp("OVERWRITE", filter=f, rows=T(*rows), mode=mode, dynamic=dynamic)


def rep(f=None, sort_by="q"):
    return LogicalOp("REPLACE", filter=f if f is not None else dict(X.TRUE), sort_by=sort_by)


class ExplicitGen(fz.FuzzGen):
    def __init__(self, name, layout, base, ops_a, ops_c, chunks=2):  # noqa: D401 - no FuzzGen.__init__
        self.seed, self.name, self.layout, self.base, self.chunks = 0, name, layout, base, chunks
        self.ops = {"A": [], "C": []}
        for side, ops in (("A", ops_a), ("C", ops_c)):
            for op in ops:
                if op.rows is not None:
                    op.rows = op.rows.select(fz.NAMES).cast(fz.SCHEMA)
                self.ops[side].append(op)

    def describe(self):
        return {"name": self.name, "layout": self.layout, "base_rows": len(self.base), "chunks": self.chunks,
                "ops": {s: [o.summary() for o in self.ops[s]] for s in ("A", "C")}}


def run_case(gen: ExplicitGen) -> dict:
    with mock.patch.object(fz, "make_scenario", lambda seed: gen):
        return fz.run_fuzz_scenario(0)


B = base_rows()
DUP = [R(0, D1, 1.0, 1, "a")] * 3 + [R(i, D1, 0.5, 2, "b") for i in range(1, 7)] + [R(50, D2, 2.0, 3, L17)]
DUPKEY = base_rows(12) + [R(5, D2, 2.0, 4, "b")]  # rid 5 twice with different values
r_new = R(900, D2, 2.0, 3, "b")
r_new2 = R(901, D1, 1.5, 1, L17)

CASES = [
    # name, layout, base, ops A, ops C, chunks, expect (merged | conflict | None)
    ("both_sides_empty", "month", B, [], [], 2, "merged"),
    ("source_noop", "month", B, [dele(X.cmp("q", ">", 1.0))], [], 2, "merged"),
    ("target_noop", "month", B, [], [dele(X.cmp("q", ">", 1.0), "mor"), app(r_new)], 2, "merged"),
    ("empty_appends_both", "month", B, [app()], [app()], 2, "merged"),
    ("delete_nothing_vs_append", "month", B, [dele(X.cmp("q", ">", 99.0))], [app(r_new)], 2, "merged"),
    ("delete_all_cow_vs_delete_all_mor", "month", B, [dele(dict(X.TRUE))], [dele(X.cmp("q", ">=", 0.0), "mor")], 2,
     "merged"),
    ("delete_all_vs_append", "month", B, [dele(dict(X.TRUE))], [app(r_new)], 2, "conflict"),
    ("dup_base_rows_cow_vs_mor", "none", DUP, [dele(X.cmp("k", "=", 3))], [dele(X.cmp("s", "=", "b"), "mor")], 1,
     "merged"),
    ("dup_base_rows_deleted_by_source", "none", DUP, [dele(X.cmp("k", "=", 2))], [dele(X.cmp("q", "=", 1.0), "mor")],
     1, "merged"),
    ("dup_base_rows_compaction_vs_mor", "none", DUP, [rep(sort_by="s")], [dele(X.cmp("k", "=", 1), "mor")], 2,
     "merged"),
    ("dup_base_rows_both_cow_overlapping", "none", DUP, [dele(X.cmp("q", "<=", 1.0))], [dele(X.cmp("k", "<=", 1))],
     1, "merged"),
    ("noop_upsert_identical_both_cow", "month", B, [ups([B[4]])], [ups([B[4]])], 2, "merged"),
    ("noop_upsert_identical_cow_mor", "month", B, [ups([B[4]])], [ups([B[4]], "mor")], 2, "merged"),
    ("noop_upsert_identical_both_mor", "month", B, [ups([B[4]], "mor")], [ups([B[4]], "mor")], 2, "merged"),
    ("identical_upsert_changed_value_mor_cow", "month", B, [ups([dict(B[7], q=9.0)], "mor")],
     [ups([dict(B[7], q=9.0)])], 2, "merged"),
    ("identical_upsert_plus_disjoint_key", "month", B, [ups([dict(B[7], q=9.0), dict(B[8], q=7.0)])],
     [ups([dict(B[7], q=9.0)], "mor")], 2, "merged"),
    ("different_upsert_same_key", "month", B, [ups([dict(B[7], q=9.0)])], [ups([dict(B[7], q=8.0)])], 2, "conflict"),
    ("exact_equal_appends_after_own_delete_cow", "month", B, [dele(X.cmp("q", "=", 2.0)), app(r_new)],
     [dele(X.cmp("q", "=", 2.0)), app(r_new)], 2, "merged"),
    ("exact_equal_appends_after_own_delete_mor", "month", B, [dele(X.cmp("q", "=", 2.0), "mor"), app(r_new)],
     [dele(X.cmp("q", "=", 2.0), "mor"), app(r_new)], 2, "merged"),
    ("exact_equal_two_copies_each", "month", B, [dele(X.cmp("q", "=", 2.0)), app(r_new, r_new)],
     [dele(X.cmp("q", "=", 2.0), "mor"), app(r_new, r_new)], 2, "merged"),
    ("unequal_copies_two_vs_one", "month", B, [dele(X.cmp("q", "=", 2.0)), app(r_new, r_new)],
     [dele(X.cmp("q", "=", 2.0)), app(r_new)], 2, "conflict"),
    ("exact_equal_plus_extra_match_one_side", "month", B, [dele(X.cmp("q", "=", 2.0)), app(r_new)],
     [dele(X.cmp("q", "=", 2.0)), app(r_new, dict(r_new, rid=902))], 2, "conflict"),
    ("tie_inclusive_bound_conflict", "month", B, [dele(X.cmp("q", ">=", 1.5))], [app(R(903, D2, 1.5, 1, "b"))], 2,
     "conflict"),
    ("tie_strict_bound_commutes", "month", B, [dele(X.cmp("q", ">", 1.5))], [app(R(903, D2, 1.5, 1, "b"))], 2,
     "merged"),
    ("long_string_differs_at_char17", "month", B, [dele(X.cmp("s", "=", L17b))], [app(R(904, D1, 0.0, 0, L17))], 2,
     "merged"),
    ("long_string_equal_conflict", "month", B, [dele(X.cmp("s", "=", L17b))], [app(R(904, D1, 0.0, 0, L17b))], 2,
     "conflict"),
    ("key_in_list_vs_new_key_upsert", "month", B, [dele(X.isin("rid", [5, 905]))], [ups([R(905, D2, 1.0, 1, "a")])],
     2, "conflict"),
    ("month_boundary_dates", "month", B + [R(60, dt.date(2024, 1, 31)), R(61, dt.date(2024, 2, 1))],
     [dele(X.cmp("d", "<", dt.date(2024, 2, 1)), "mor")], [app(R(906, dt.date(2024, 2, 1), 0.0, 0, "a"))], 2,
     "merged"),
    ("both_compact_same_partition_diff_sort_plus_mor", "month", B,
     [rep(JAN, "s"), dele(X.cmp("q", "=", 0.0), "mor")], [rep(JAN, "k"), dele(X.cmp("q", "=", 1.0), "mor")], 2,
     "merged"),
    ("target_compacts_twice_source_mor_and_append", "month", B, [rep(JAN, "s"), rep(JAN, "rid")],
     [dele(X.and_(JAN, X.cmp("q", "<", 1.0)), "mor"), app(R(907, D1, 2.0, 2, "a"))], 2, "merged"),
    ("source_mor_deletes_own_append_target_compacts", "month", B, [rep(JAN, "q")],
     [app(R(908, D1, 1.0, 1, "c"), R(909, D1, 1.0, 1, "d")), dele(X.cmp("s", "=", "c"), "mor"),
      dele(X.and_(JAN, X.cmp("q", "=", 0.5)), "mor")], 2, "merged"),
    ("same_positions_mor_both", "month", B, [dele(X.cmp("k", "=", 2), "mor")], [dele(X.cmp("k", "=", 2), "mor")], 2,
     "merged"),
    ("drop_partition_vs_mor_in_it", "month", B, [dele(FEB)], [dele(X.and_(FEB, X.cmp("q", ">", 0.0)), "mor")], 2,
     "merged"),
    ("drop_partition_vs_cow_rewrite_in_it", "month", B, [dele(FEB)], [dele(X.and_(FEB, X.cmp("k", "=", 1)))], 2,
     "merged"),
    ("dynamic_overwrite_vs_mor_delete_same_partition", "month", B,
     [ovw(FEB, [dict(r, q=5.0) for r in B if r["d"] == D2], dynamic=True)],
     [dele(X.and_(FEB, X.cmp("k", "=", 1)), "mor")], 2, None),
    ("unpartitioned_compaction_cow_mor_append", "none", B, [rep(sort_by="s"), dele(X.cmp("k", "=", 4), "mor")],
     [dele(X.cmp("q", "=", 0.5)), app(R(910, D3, 0.0, 9, "z"))], 3, "merged"),
    ("upsert_mor_vs_source_compaction", "month", B, [ups([dict(B[3], q=7.0)], "mor")],
     [rep(JAN, "k"), dele(X.cmp("k", "=", 3), "mor")], 2, None),
    ("duplicate_keys_in_base_upsert", "month", DUPKEY, [ups([R(5, D2, 3.0, 4, "b")])], [dele(X.cmp("q", "=", 9.0))],
     2, "merged"),
    ("identity_partition_mixed", "identity_k", B, [rep(X.cmp("k", "=", 1), "s"), dele(X.cmp("q", "<", 1.0), "mor")],
     [ovw(X.cmp("s", "=", "b"), [], "cow"), app(R(911, D1, 1.0, 1, "zz"))], 2, "merged"),
    ("masking_delete_net_effect", "month", B, [dele(X.cmp("q", ">", 1.5))],
     [app(R(912, D1, 2.0, 1, "a")), dele(X.cmp("q", ">", 1.5), "mor")], 2, "merged"),
    ("one_row_base_delete_both", "none", [R(1)], [dele(dict(X.TRUE), "mor")], [dele(dict(X.TRUE))], 1, "merged"),
]


class EdgeCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recs = {}
        for name, layout, base, oa, oc, chunks, expect in CASES:
            cls.recs[name] = (run_case(ExplicitGen(name, layout, base, oa, oc, chunks)), expect)

    def test_properties_hold_on_every_edge_case(self):
        bad = {n: fz.violations(r) for n, (r, _) in self.recs.items() if fz.violations(r)}
        self.assertEqual(bad, {})

    def test_oracle_classification_matches_case_design(self):
        wrong = {n: r["oracle"]["true_conflict"] for n, (r, e) in self.recs.items()
                 if e is not None and r["oracle"]["true_conflict"] != (e == "conflict")}
        self.assertEqual(wrong, {}, "edge-case design error: the oracle disagrees with the expected class")

    def test_snapmerge_merges_every_commuting_case_inside_its_rule_set(self):
        fp = {n: (r["policies"]["snapmerge"]["verdict"], r["policies"]["snapmerge"].get("rule"))
              for n, (r, e) in self.recs.items() if e == "merged" and r["policies"]["snapmerge"]["verdict"] != "merged"}
        self.assertEqual(fp, {})


class ConcurrentSourceCommit(unittest.TestCase):
    """A writer commits to ANOTHER branch (here the source C) while the merge is between planning and
    its catalog commit.  The submitted version computed its sequence number from stale metadata, so
    the catalog rejected the commit ("older than last sequence number") and the merge failed without
    retry (finding F10).  Since 2026-10-05 the source is pinned by snapshot id before planning and a
    commit lost to another branch's commit is re-sequenced (new manifest list) and retried: the merge
    commits the PINNED source head, the late source commit is not merged, and the target never
    exposes a partial merge."""

    def test_source_advances_during_merge_is_pinned_and_resequenced(self):
        work = Path(tempfile.mkdtemp(prefix="adv-conc-"))
        o = Oracle()
        try:
            cat = C.open_catalog(work)
            base = T(*B)
            t = C.create_table(cat, "t.t", fz.SCHEMA, ("d", "month"))
            t = write_base(t, base, chunks=2)
            bid = C.head_id(t, "main")
            t = C.create_branch(t, "A", bid)
            t = C.create_branch(t, "C", bid)
            ops_a = [dele(X.cmp("q", "=", 0.0), "mor")]
            ops_c = [dele(X.cmp("k", "=", 2)), app(R(950, D3, 1.0, 1, "a"))]
            late = app(R(951, D1, 1.5, 0, "late"))  # q=1.5: matches no predicate of A
            for i, op in enumerate(ops_a):
                t = apply_op(t, "A", op, f"A{i}")
            for i, op in enumerate(ops_c):
                t = apply_op(t, "C", op, f"C{i}")
            t = C.create_branch(t, "m", C.head_id(t, "A"))
            pinned = C.head_id(t, "C")
            fired = []

            def hook(stage):
                if stage == "after_delete_files" and not fired:
                    other = cat.load_table("t.t")  # an independent writer on the source branch
                    apply_op(other, "C", late, "C-late")
                    fired.append(stage)

            out = snapmerge(t, "m", "C", crash_hook=hook)
            self.assertEqual(fired, ["after_delete_files"])
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertEqual(out.category, "merged")
            self.assertGreaterEqual(out.details["resequenced"], 1)  # the late commit took the sequence number
            self.assertEqual(out.source_snapshot, pinned)
            t = out.table.refresh()
            props = C.summary_props(t.metadata.snapshot_by_id(out.snapshot_id))
            self.assertEqual(int(props["snapmerge.merge-source-head"]), pinned)
            exp = fz.bag(fz.py_apply(B, ops_a + ops_c))  # the pinned source, without the late commit
            self.assertEqual(fz.bag_arrow(C.scan_duckdb(o.con, t, out.snapshot_id)), exp)
            self.assertEqual(fz.bag_arrow(C.scan_pyiceberg(t, out.snapshot_id)), exp)
            self.assertNotEqual(C.head_id(t, "C"), pinned)  # the late commit stays on the source branch
        finally:
            o.close()
            shutil.rmtree(work, ignore_errors=True)

    def test_target_moved_is_retryable_and_cleans_up(self):
        work = Path(tempfile.mkdtemp(prefix="adv-tgt-"))
        try:
            cat = C.open_catalog(work)
            t = C.create_table(cat, "t.t", fz.SCHEMA, ("d", "month"))
            t = write_base(t, T(*B), chunks=2)
            bid = C.head_id(t, "main")
            t = C.create_branch(t, "A", bid)
            t = C.create_branch(t, "C", bid)
            t = apply_op(t, "A", dele(X.cmp("q", "=", 0.0), "mor"), "A0")
            t = apply_op(t, "C", dele(X.cmp("q", ">=", 2.0), "mor"), "C0")  # writes a new delete file at merge
            t = C.create_branch(t, "m", C.head_id(t, "A"))
            files_before = {p for p in work.rglob("*") if p.is_file()}
            fired = []

            def hook(stage):
                if stage == "after_delete_files" and not fired:
                    other = cat.load_table("t.t")  # an independent writer on the TARGET branch
                    apply_op(other, "m", app(R(952, D1, 1.5, 0, "t")), "M-late")  # q=1.5: matches no predicate of C
                    fired.append(stage)

            out = snapmerge(t, "m", "C", crash_hook=hook)
            self.assertEqual(out.verdict, "conflict")
            self.assertEqual(out.category, "retryable")
            self.assertEqual(out.rule, "target-moved")
            t = cat.load_table("t.t")
            leftovers = {p for p in work.rglob("*") if p.is_file()} - files_before
            # only the late target commit's own files remain; the merge attempt's files were removed
            merge_files = [p for p in leftovers if "pos-del" in p.name and p.stat().st_size and
                           not any(str(p) in f.path for f in C.load_state(t, C.head_id(t, "m")).files.values())]
            self.assertEqual(merge_files, [])
            out2 = snapmerge(t, "m", "C")  # re-plan against the new target head
            self.assertEqual(out2.verdict, "merged", out2.to_json())
        finally:
            shutil.rmtree(work, ignore_errors=True)


class ReplayDuplicateKeyCounterexample(unittest.TestCase):
    """Counterexample found by the review fuzzer (seed 591 of results/review_fuzz_v1): both branches
    upsert the SAME NEW key with an identical row.  The serial orders agree (one row), SnapMerge
    merges correctly, but partition-level replay re-applies the source's upsert as a plain append
    (its delete part matched nothing on the source) and duplicates the key: an unsound merge."""

    def test_identical_new_key_upserts(self):
        row = R(990, D2, 1.0, 1, "b")
        gen = ExplicitGen("identical_new_key_upserts", "month", B, [ups([row], "mor")], [ups([row], "cow")])
        rec = run_case(gen)
        self.assertEqual(fz.violations(rec), [])
        self.assertFalse(rec["oracle"]["true_conflict"])
        self.assertEqual(rec["policies"]["snapmerge"]["verdict"], "merged")
        rp = rec["policies"]["replay"]
        self.assertEqual(rp["verdict"], "merged")
        self.assertFalse(rp["serial_equivalent"])
        self.assertEqual(rp["vs_ac"], {"only_left": 1, "only_right": 0})  # one duplicated row


class RepeatedMerge(unittest.TestCase):
    """Finding (review): after a first merge of C into A, a second merge between the same branches
    is refused as a structural conflict although both sides only appended (LCA = previous source
    head, which is not on A's first-parent chain).  Sound, but a precision/coverage limitation."""

    @unittest.expectedFailure
    def test_second_merge_of_commuting_appends_is_merged(self):
        from helpers import Env, rows

        env = Env()
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(3, 1000)))
            env.apply("C", LogicalOp("APPEND", rows=rows(3, 2000)))
            self.assertEqual(env.merge(target="A").verdict, "merged")
            env.table = env.table.refresh()
            env.table = apply_op(env.table, "A", LogicalOp("APPEND", rows=rows(2, 3000)), "A9")
            env.table = apply_op(env.table, "C", LogicalOp("APPEND", rows=rows(2, 4000)), "C9")
            env.table = C.create_branch(env.table, "m2", C.head_id(env.table, "A"))
            out = snapmerge(env.table, "m2", "C")
            self.assertEqual(out.verdict, "merged", out.to_json())
        finally:
            env.close()


if __name__ == "__main__":
    unittest.main()
