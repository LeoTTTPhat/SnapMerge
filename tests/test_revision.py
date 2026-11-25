"""Regression tests for the 2026-10-05 revision (review package R1-R4, F1-F10).

Insertion provenance (F1): the review's single-snapshot copy-on-write example, accepted wrongly by the
submitted rule, is now a logical conflict with a net-effect witness; contradictory, incomplete and
inconsistent schema-v2 records are rejected with explicit reasons.  Also: exact matching under hash
collisions (F6), net-effect witnesses, the provenance-aware writer check (F7), information-matched
controls, pinned sources and re-sequencing, missing history, the composition budget, and the
generator's sensitivity knobs.
"""
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pyarrow as pa

from helpers import C, Env, LogicalOp, Oracle, X, month_pred, rows, snapmerge
from snapmerge import provenance, rules
from snapmerge.intent import INTENT_POLICIES
from snapmerge.rules import RuleConfig
from snapmerge.writer import apply_op, op_properties

S2 = pa.schema([("k", pa.int64()), ("x", pa.int64())])


def T(*rs):
    return pa.table({"k": [r[0] for r in rs], "x": [r[1] for r in rs]}, schema=S2)


class R2Example:
    """Base F={(1,0),(2,0)}, H={(3,1)} in one (unpartitioned) partition.  A: OVERWRITE(k=1,{(1,1)})
    committed as ONE copy-on-write snapshot (remove F, add G = {(1,1),(2,0)}).  C: DELETE(x=1).
    (A;C) = {(2,0)}, (C;A) = {(1,1),(2,0)}: a true conflict."""

    def __init__(self, schema_version=2, part="mixed", op_end=True, inserted=None):
        self.dir = Path(tempfile.mkdtemp(prefix="r2-"))
        cat = C.open_catalog(self.dir)
        t = C.create_table(cat, "t.t", S2, None)
        t.append(T((1, 0), (2, 0)), snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": "b0"})
        t.append(T((3, 1)), snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": "b1"})
        t = t.refresh()
        self.base_id = C.head_id(t, "main")
        t = C.create_branch(t, "A", self.base_id)
        t = C.create_branch(t, "C", self.base_id)
        st = C.load_state(t, self.base_id)
        F = [f for f in st.data_files() if f.record_count == 2][0]
        self.opA = LogicalOp("OVERWRITE", filter=X.cmp("k", "=", 1), rows=T((1, 1)), mode="cow", key_col="k")
        props = op_properties(self.opA, "A0", schema_version=schema_version, part=part, op_end=op_end, table=t)
        if inserted is not None:
            props["snapmerge.inserted-records"] = str(inserted)
        G = C.write_data_files(t, T((1, 1), (2, 0)))
        t, _ = C.commit_delta(t, "A", C.load_state(t, C.head_id(t, "A")), remove_paths={F.path}, add_files=G,
                              operation="overwrite", properties=props)
        self.opC = LogicalOp("DELETE", filter=X.cmp("x", "=", 1), mode="cow", key_col="k")
        self.table = apply_op(t.refresh(), "C", self.opC, "C0")
        self.oracle = Oracle()
        self.orc = self.oracle.serial(T((1, 0), (2, 0), (3, 1)), [self.opA], [self.opC])

    def merge(self, cfg=None, target="m"):
        t = C.create_branch(self.table, target, C.head_id(self.table, "A"))
        out = snapmerge(t, target, "C", cfg)
        self.table = (out.table or t).refresh()
        return out

    def close(self):
        self.oracle.close()
        shutil.rmtree(self.dir, ignore_errors=True)


class InsertionProvenance(unittest.TestCase):
    def test_review_example_is_a_true_conflict(self):
        ex = R2Example()
        try:
            self.assertTrue(ex.orc["true_conflict"])
        finally:
            ex.close()

    def test_single_snapshot_cow_detected_with_v1_metadata(self):
        ex = R2Example(schema_version=1)  # no roles: the rewrite-bearing snapshot is inferred mixed
        try:
            out = ex.merge()
            self.assertEqual(out.verdict, "conflict", out.to_json())
            self.assertEqual(out.category, "logical-conflict")
            w = out.witness
            self.assertEqual(w["kind"], "net-effect")
            self.assertEqual(w["example_row"], {"k": 1, "x": 1})
            self.assertEqual((w["H_A"], w["H_C"], w["order_difference"]), (1, 0, -1))
            self.assertEqual(w["inserted_by"]["op"], "A0")
            self.assertEqual(w["deleted_by"]["op"], "C0")
            self.assertGreaterEqual(out.details["hits"].get("provenance:mixed-snapshot", 0), 1)
        finally:
            ex.close()

    def test_single_snapshot_cow_detected_with_declared_mixed_part(self):
        ex = R2Example(schema_version=2, part="mixed")
        try:
            out = ex.merge()
            self.assertEqual(out.category, "logical-conflict", out.to_json())
        finally:
            ex.close()

    def test_submitted_append_rule_accepts_it_unsoundly(self):
        """Documents the bug the review found (only reachable through the measurement variant)."""
        ex = R2Example(schema_version=1)
        try:
            out = ex.merge(RuleConfig(discovery="append_rule"))
            self.assertEqual(out.verdict, "merged")
            chk = ex.oracle.check_snapshot(ex.table, out.snapshot_id, ["o_ac", "o_ca"])
            self.assertFalse(chk["duckdb_eq_o_ac"])
            self.assertTrue(chk["duckdb_eq_o_ca"])
        finally:
            ex.close()

    def test_rewrite_declared_delete_with_hidden_insert_is_rejected(self):
        ex = R2Example(schema_version=2, part="delete")  # lies: the rewrite holds an inserted row
        try:
            out = ex.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertTrue(any("inconsistent" in r for r in out.witness["reasons"]), out.witness)
        finally:
            ex.close()

    def test_insert_part_that_removes_files_is_contradictory(self):
        ex = R2Example(schema_version=2, part="insert")
        try:
            out = ex.merge()
            self.assertEqual(out.category, "unsupported")
            self.assertTrue(any("contradictory" in r for r in out.witness["reasons"]), out.witness)
        finally:
            ex.close()

    def test_missing_op_end_is_incomplete(self):
        ex = R2Example(schema_version=2, part="mixed", op_end=False)
        try:
            out = ex.merge()
            self.assertEqual(out.category, "unsupported")
            self.assertTrue(any("incomplete" in r for r in out.witness["reasons"]), out.witness)
        finally:
            ex.close()

    def test_statement_semantics_is_rejected(self):
        ex = R2Example(schema_version=2, part="mixed")
        try:
            t = ex.table
            op = LogicalOp("DELETE", filter=X.cmp("x", "=", 0), mode="mor", key_col="k")
            props = op_properties(op, "C1", table=t)
            props["snapmerge.semantics"] = "statement"  # e.g. UPDATE x = x + 1 to be re-executed
            st = C.load_state(t, C.head_id(t, "C"))
            f = sorted(st.data_files(), key=lambda z: z.path)[0]
            d = C.write_position_delete_file(t, f.data_file, [0])
            ex.table, _ = C.commit_delta(t, "C", st, remove_paths=set(), add_files=[d], operation="delete",
                                         properties=props)
            out = ex.merge()
            self.assertEqual(out.category, "unsupported")
            self.assertTrue(any("unsupported semantics" in r for r in out.witness["reasons"]), out.witness)
        finally:
            ex.close()

    def test_wrong_inserted_count_on_mixed_part_is_rejected(self):
        ex = R2Example(schema_version=2, part="mixed", inserted=2)
        try:
            out = ex.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertEqual(out.rule, "metadata:inserted-count")
        finally:
            ex.close()

    def test_commuting_single_snapshot_writer_merges_soundly(self):
        env = Env(rows(120))
        try:
            env.table = apply_op(env.table, "A", LogicalOp("OVERWRITE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0)),
                                                             rows=rows(3, 5000, q=40.0, month=1), mode="cow"),
                                 "A0", writer="single")
            env.ops["A"].append(LogicalOp("OVERWRITE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0)),
                                          rows=rows(3, 5000, q=40.0, month=1), mode="cow"))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0), mode="mor"))
            self.assertFalse(env.serial()["true_conflict"])
            out = env.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
        finally:
            env.close()

    def test_legacy_v1_writer_gives_the_same_decisions(self):
        """The submitted writer (schema v1) is still handled: its PyIceberg rewrite snapshots are
        treated as mixed and their exact difference is empty."""
        for v in (1, 2):
            with self.subTest(schema_version=v):
                env = Env(rows(120))
                try:
                    op = LogicalOp("OVERWRITE", filter=X.and_(month_pred(2), X.cmp("q", "<", 20.0)),
                                   rows=rows(4, 7000, q=30.0, month=2), mode="cow")
                    env.table = apply_op(env.table, "A", op, "A0", schema_version=v)
                    env.ops["A"].append(op)
                    env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "=", 30.0), mode="cow"))
                    self.assertTrue(env.serial()["true_conflict"])
                    out = env.merge()
                    self.assertEqual(out.category, "logical-conflict", out.to_json())
                    w = out.witness  # four distinct inserted rows match C's predicate; each value once
                    self.assertEqual((w["H_A"], w["H_C"], w["order_difference"]), (1, 0, -1))
                    self.assertEqual(w["rows_A_matching_P_C"], 4)
                    self.assertEqual(w["example_row"]["q"], 30.0)
                finally:
                    env.close()


class ExactMatching(unittest.TestCase):
    def test_match_rows_is_exact_when_every_hash_collides(self):
        rng = np.random.default_rng(7)
        vals = rng.integers(0, 4, size=(60, 2))
        left = pa.table({"a": vals[:, 0], "b": vals[:, 1]})
        right = pa.table({"a": vals[::-1, 0], "b": vals[::-1, 1]})
        with mock.patch.object(rules, "_row_hashes", lambda t, nullable=None: np.zeros(t.num_rows, np.uint64)):
            li, ri = rules.match_rows(left, right)
        self.assertEqual(li.size, 60)
        self.assertTrue((left.take(pa.array(li)).to_pandas().values == right.take(pa.array(ri)).to_pandas().values).all())

    def test_match_rows_treats_null_as_equal_and_is_maximum(self):
        left = pa.table({"a": pa.array([1, None, None, 2], pa.int64()), "b": pa.array(["x", None, "y", "z"])})
        right = pa.table({"a": pa.array([None, 1, 2, None], pa.int64()), "b": pa.array([None, "x", "w", "q"])})
        li, ri = rules.match_rows(left, right)
        pairs = sorted(zip(li.tolist(), ri.tolist()))
        self.assertEqual(pairs, [(0, 1), (1, 0)])

    def test_int_column_with_and_without_nulls_hash_consistently(self):
        left = pa.table({"a": pa.array([5, None], pa.int64())})
        right = pa.table({"a": pa.array([5], pa.int64())})
        li, ri = rules.match_rows(left, right)
        self.assertEqual((li.tolist(), ri.tolist()), ([0], [0]))

    def test_exact_equality_decision_survives_total_collision(self):
        env = Env(rows(60))
        try:
            same = rows(2, 9000, q=7.0, month=1)
            env.apply("A", LogicalOp("UPSERT", rows=same, keys=[9000, 9001], mode="mor"))
            env.apply("C", LogicalOp("UPSERT", rows=same, keys=[9000, 9001], mode="cow"))
            self.assertFalse(env.serial()["true_conflict"])
            with mock.patch.object(rules, "_row_hashes", lambda t, nullable=None: np.zeros(t.num_rows, np.uint64)):
                out = env.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertGreaterEqual(out.details["hits"].get("net:exact-equal", 0), 1)
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"], chk)
        finally:
            env.close()


class NetEffectWitness(unittest.TestCase):
    def test_order_difference_matches_the_oracle(self):
        cases = [
            ([LogicalOp("APPEND", rows=rows(2, 8000, q=3.0, month=1))],
             [LogicalOp("DELETE", filter=X.cmp("q", "=", 3.0), mode="mor")]),
            ([LogicalOp("DELETE", filter=X.cmp("q", "<", 5.0), mode="cow")],
             [LogicalOp("APPEND", rows=rows(3, 8100, q=1.0, month=2))]),
            ([LogicalOp("UPSERT", rows=rows(1, 8200, q=11.0, month=1), keys=[8200], mode="cow")],
             [LogicalOp("UPSERT", rows=rows(1, 8200, q=12.0, month=1), keys=[8200], mode="mor")]),
        ]
        for ops_a, ops_c in cases:
            with self.subTest(a=ops_a[0].kind, c=ops_c[0].kind):
                env = Env(rows(90))
                try:
                    env.apply("A", *ops_a)
                    env.apply("C", *ops_c)
                    self.assertTrue(env.serial()["true_conflict"])
                    out = env.merge()
                    w = out.witness
                    self.assertEqual(w["kind"], "net-effect", w)
                    v = w["example_row"]
                    where = " AND ".join(f'"{k}" = {repr(val) if not isinstance(val, str) else "'" + val + "'"}'
                                         if k != "d" else f"d = DATE '{val}'" for k, val in v.items())
                    n_ac = env.oracle.con.execute(f"SELECT count(*) FROM o_ac WHERE {where}").fetchone()[0]
                    n_ca = env.oracle.con.execute(f"SELECT count(*) FROM o_ca WHERE {where}").fetchone()[0]
                    self.assertEqual(n_ac - n_ca, w["order_difference"], w)
                finally:
                    env.close()

    def test_masking_is_not_a_conflict(self):
        env = Env(rows(80))
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "=", 4.0), mode="cow"))
            env.apply("C", LogicalOp("APPEND", rows=rows(2, 8300, q=4.0, month=1)),
                      LogicalOp("DELETE", filter=X.cmp("q", "=", 4.0), mode="mor"))
            self.assertFalse(env.serial()["true_conflict"])
            out = env.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertGreaterEqual(out.details["hits"].get("net:masked-by-later-op", 0), 1)
        finally:
            env.close()


class ProvenanceCheck(unittest.TestCase):
    NAMES = ["rid", "d", "q", "s"]

    def _check(self, env, side, op, writer="split"):
        before = C.head_id(env.table, side)
        env.table = apply_op(env.table, side, op, f"{side}x", writer=writer)
        return provenance.check_op(env.table, before, C.head_id(env.table, side), op, self.NAMES)

    def test_writer_operations_pass(self):
        ops = [LogicalOp("APPEND", rows=rows(3, 9100, month=1)),
               LogicalOp("DELETE", filter=X.cmp("q", "<", 4.0), mode="cow"),
               LogicalOp("DELETE", filter=X.cmp("q", ">", 45.0), mode="mor"),
               LogicalOp("OVERWRITE", filter=X.and_(month_pred(2), X.cmp("q", "<", 20.0)),
                         rows=rows(2, 9200, q=33.0, month=2), mode="cow"),
               LogicalOp("UPSERT", rows=rows(2, 9300, q=1.0, month=3), keys=[9300, 9301], mode="mor"),
               LogicalOp("REPLACE", filter=month_pred(1), sort_by="q")]
        for writer in ("split", "single"):
            env = Env(rows(150))
            try:
                for op in ops:
                    with self.subTest(writer=writer, op=op.kind, mode=op.mode):
                        r = self._check(env, "A", op, writer)
                        self.assertTrue(r["ok"], r)
            finally:
                env.close()

    def test_delete_and_reinsert_of_a_survivor_is_detected(self):
        """Same row multiset as keeping the instance (the value-level writer check passes), but the
        instance of a row that does NOT satisfy the predicate was position-deleted (A2 violated)."""
        env = Env(rows(60))
        try:
            st = C.load_state(env.table, C.head_id(env.table, "A"))
            f = sorted(st.data_files(), key=lambda x: x.path)[0]
            live, pos = C.live_rows(st, f)
            victim = live.slice(0, 1)
            op = LogicalOp("DELETE", filter=X.cmp("q", "=", -1.0), mode="mor")  # matches nothing
            dfile = C.write_position_delete_file(env.table, f.data_file, [int(pos[0])])
            again = C.write_data_files(env.table, victim.cast(env.table.schema().as_arrow()))
            before = C.head_id(env.table, "A")
            env.table, _ = C.commit_delta(env.table, "A", st, remove_paths=set(), add_files=[dfile] + again,
                                          operation="overwrite", properties=op_properties(op, "Ax", table=env.table))
            r = provenance.check_op(env.table, before, C.head_id(env.table, "A"), op, self.NAMES)
            self.assertFalse(r["ok"])
            self.assertTrue(any("position delete" in v for v in r["violations"]), r)
            env.oracle.apply("o_a", env.base, [op])
            chk = env.oracle.check_snapshot(env.table, C.head_id(env.table, "A"), ["o_a"])
            self.assertTrue(chk["duckdb_eq_o_a"])  # the value-level check cannot see it
        finally:
            env.close()


class InformationMatchedControls(unittest.TestCase):
    def _run(self, env, policy):
        t = C.create_branch(env.table, "m_" + policy, C.head_id(env.table, "A"))
        out = INTENT_POLICIES[policy](t, "m_" + policy, "C") if policy != "recompute" else \
            INTENT_POLICIES[policy](t, "m_" + policy, "C", con=env.oracle.con)
        env.table = (out.table or t).refresh()
        return out

    def test_compaction_vs_mor_delete_needs_composition(self):
        env = Env(rows(150))
        try:
            env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
            env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0)), mode="mor"))
            env.serial()
            for p in ("replay_intent", "replay_intent_exact", "rebase_occ"):
                with self.subTest(policy=p):
                    out = self._run(env, p)
                    self.assertEqual((out.verdict, out.conflict_class, out.rule), ("conflict", "physical", "physical:replay"))
            out = self._run(env, "recompute")
            self.assertEqual(out.verdict, "merged")
            self.assertTrue(env.check(out.snapshot_id)["duckdb_eq_o_ac"])
            self.assertEqual(env.merge().verdict, "merged")
        finally:
            env.close()

    def test_rebase_contract_accepts_designated_order_only(self):
        env = Env(rows(100))
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "=", 6.0), mode="mor"))
            env.apply("C", LogicalOp("APPEND", rows=rows(2, 8800, q=6.0, month=2)))  # into A's predicate
            self.assertTrue(env.serial()["true_conflict"])
            out = self._run(env, "rebase_occ")  # checks only C's predicates vs A's inserts
            self.assertEqual(out.verdict, "merged")
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and not chk["duckdb_eq_o_ca"], chk)
            self.assertEqual(self._run(env, "replay_intent").conflict_class, "logical")
        finally:
            env.close()

    def test_masking_separates_pairwise_from_exact(self):
        env = Env(rows(80))
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "=", 4.0), mode="mor"))
            env.apply("C", LogicalOp("APPEND", rows=rows(2, 8400, q=4.0, month=3)),
                      LogicalOp("DELETE", filter=X.cmp("q", "=", 4.0), mode="mor"))
            env.serial()
            self.assertEqual(self._run(env, "replay_intent").conflict_class, "logical")
            out = self._run(env, "replay_intent_exact")
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertTrue(env.check(out.snapshot_id)["duckdb_eq_o_ac"])
        finally:
            env.close()


class OperationalContract(unittest.TestCase):
    def test_missing_history_is_unsupported_and_target_unchanged(self):
        env = Env(rows(100))
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(3, 8500, q=5.0, month=1)))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "=", 5.0), mode="mor"))
            ins = [f for f in C.load_state(env.table, C.head_id(env.table, "A")).data_files()
                   if f.snapshot_id == C.head_id(env.table, "A")][0]
            Path(C.local_path(ins.path)).unlink()  # e.g. removed by an aggressive orphan-file cleanup
            t = C.create_branch(env.table, "m", C.head_id(env.table, "A"))
            out = snapmerge(t, "m", "C")
            self.assertEqual((out.category, out.rule), ("unsupported", "missing-history"))
            self.assertEqual(C.head_id(out.table.refresh(), "m"), C.head_id(env.table, "A"))
        finally:
            env.close()

    def test_missing_carried_file_that_was_never_read_is_unsupported(self):
        env = Env(rows(100))
        try:
            env.apply("A", LogicalOp("DELETE", filter=X.cmp("q", "=", 48.0), mode="mor"))
            env.apply("C", LogicalOp("APPEND", rows=rows(3, 8550, q=1.0, month=2)))  # bounds exclude q = 48
            ins = [f for f in C.load_state(env.table, C.head_id(env.table, "C")).data_files()
                   if f.snapshot_id == C.head_id(env.table, "C")][0]
            Path(C.local_path(ins.path)).unlink()
            t = C.create_branch(env.table, "m", C.head_id(env.table, "A"))
            out = snapmerge(t, "m", "C")
            self.assertEqual((out.category, out.rule), ("unsupported", "missing-history"), out.to_json())
            self.assertEqual(out.details["hits"].get("bounded-read", 0), 0)
            self.assertEqual(C.head_id(out.table.refresh(), "m"), C.head_id(env.table, "A"))
        finally:
            env.close()

    def test_budget_fails_closed_before_reading(self):
        env = Env(rows(150))
        try:
            env.apply("A", LogicalOp("REPLACE", filter=month_pred(1), sort_by="q"))
            env.apply("C", LogicalOp("DELETE", filter=X.and_(month_pred(1), X.cmp("q", "<", 10.0)), mode="mor"))
            out = env.merge(RuleConfig(match_budget_bytes=1))
            self.assertEqual((out.category, out.rule), ("unsupported", "over-budget"))
            est = out.details["estimate"]
            self.assertEqual(est["contended_partitions"], 1)
            self.assertGreater(est["read_bytes"], 1)
            out = env.merge(target="m2")
            self.assertEqual(out.verdict, "merged")
            self.assertLessEqual(out.stats.data_bytes_read, est["read_bytes"] + 10_000)
        finally:
            env.close()

    def test_commit_on_unrelated_branch_is_resequenced(self):
        env = Env(rows(100))
        try:
            env.apply("A", LogicalOp("APPEND", rows=rows(2, 8600, q=1.0, month=1)))
            env.apply("C", LogicalOp("DELETE", filter=X.cmp("q", "=", 49.0), mode="mor"))
            env.serial()
            env.table = C.create_branch(env.table, "other", env.base_id)
            fired = []

            def hook(stage):
                if stage == "before_catalog_commit" and not fired:
                    apply_op(env.cat.load_table("t.t"), "other", LogicalOp("APPEND", rows=rows(1, 8700)), "O0")
                    fired.append(1)

            out = env.merge(crash_hook=hook)
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertEqual(out.details["resequenced"], 1)
            chk = env.check(out.snapshot_id)
            self.assertTrue(chk["duckdb_eq_o_ac"] and chk["pyiceberg_eq_o_ac"] and chk["readers_agree"], chk)
        finally:
            env.close()


class GeneratorKnobs(unittest.TestCase):
    def test_knobs_are_recorded_and_change_the_workload(self):
        from helpers import tpch_lineitem
        from snapmerge.scenarios import SourcePool, generate
        from snapmerge.sources import LINEITEM
        pool = SourcePool(LINEITEM, tpch_lineitem(0.01))
        d = generate(pool, "deletes_mor", 11, 600)
        self.assertNotIn("knobs", d.meta)
        self.assertEqual(len(d.meta["partitions"]), 4)
        w = generate(pool, "deletes_mor", 11, 600, n_parts=8, hist=4)
        self.assertEqual(w.meta["knobs"]["n_parts"], 8)
        self.assertEqual(len(w.meta["partitions"]), 8)
        many = max(len(generate(pool, "deletes_mor", s, 600, hist=4).ops["A"]) for s in range(12))
        self.assertGreater(many, 4)
        c = [generate(pool, "upsert_keys", s, 600, compact_p=1.0) for s in range(3)]
        self.assertTrue(all(sc.ops["A"][-1].kind == "REPLACE" for sc in c))


if __name__ == "__main__":
    unittest.main()
