"""Regression tests for the 2026-10-07 re-review (gates G1-G3).

G1 physical-input contract: content files added after the base must have fresh identities (a file
registered on both branches, or re-registered on one branch, is rejected as unsupported); a recorded
operation is a deletion phase followed by an insertion phase, so an operation split into a marker and
several insert parts is supported, while a deletion part after an insertion part, or position deletes
on the operation's own new file, are rejected.  G2 read cost: insertion recovery from a mixed snapshot
reads the files it removed (here a base file); a logical read is needed even without any contended
file, and the budget is enforced on the estimate of every read phase before any data is read.
G3 commit outcomes: a snapshot installed before the confirmation was lost is recognised as committed;
an unresolvable outcome keeps the attempt's files; a failure known to precede installation cleans up.
"""
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from helpers import C, LogicalOp, Oracle, X, snapmerge
from snapmerge import lineage, provenance
from snapmerge.rules import RuleConfig
from snapmerge.writer import apply_op, op_properties

S2 = pa.schema([("k", pa.int64()), ("x", pa.int64())])
NAMES = ["k", "x"]


def T(*rs):
    return pa.table({"k": [r[0] for r in rs], "x": [r[1] for r in rs]}, schema=S2)


class Lab:
    """Unpartitioned (k, x) table; each base chunk is one data file; branches A and C at the base."""

    def __init__(self, *chunks):
        self.dir = Path(tempfile.mkdtemp(prefix="rereview-"))
        cat = C.open_catalog(self.dir)
        t = C.create_table(cat, "t.t", S2, None)
        for i, ch in enumerate(chunks):
            t.append(ch, snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": f"b{i}"})
        t = t.refresh()
        self.base_rows = pa.concat_tables(chunks)
        self.base_id = C.head_id(t, "main")
        t = C.create_branch(t, "A", self.base_id)
        self.table = C.create_branch(t, "C", self.base_id)
        self.ops = {"A": [], "C": []}
        self.oracle = Oracle()

    def base_file(self, n_rows):
        return [f for f in C.load_state(self.table, self.base_id).data_files() if f.record_count == n_rows][0]

    def commit(self, side, props, remove=(), add=(), operation="overwrite"):
        st = C.load_state(self.table, C.head_id(self.table, side))
        t, sid = C.commit_delta(self.table, side, st, remove_paths=set(remove), add_files=list(add),
                                operation=operation, properties=props)
        self.table = t.refresh()
        return sid

    def apply(self, side, op, writer="split"):
        self.table = apply_op(self.table, side, op, f"{side}{len(self.ops[side])}", writer=writer)
        self.ops[side].append(op)

    def branch(self, target="m"):
        self.table = C.create_branch(self.table, target, C.head_id(self.table, "A"))
        return self.table

    def merge(self, cfg=None, target="m"):
        t = self.branch(target)
        out = snapmerge(t, target, "C", cfg)
        self.table = (out.table or t).refresh()
        return out

    def orc(self):
        return self.oracle.serial(self.base_rows, self.ops["A"], self.ops["C"])

    def both_orders(self, sid):
        chk = self.oracle.check_snapshot(self.table, sid, ["o_ac", "o_ca"])
        return chk["duckdb_eq_o_ac"] and chk["duckdb_eq_o_ca"] and chk["readers_agree"]

    def close(self):
        self.oracle.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def _reasons(out):
    return " | ".join((out.witness or {}).get("reasons", []))


# ============================================================================ G1: physical identity

class SharedContentFileIdentity(unittest.TestCase):
    def _shared(self):
        lab = Lab(T((1, 0)))
        F = C.write_data_files(lab.table, T((5, 5)))  # one immutable file, registered on BOTH branches
        op = LogicalOp("APPEND", rows=T((5, 5)), mode="cow", key_col="k")
        for side in ("A", "C"):
            lab.commit(side, op_properties(op, f"{side}0", part="insert", op_end=True, table=lab.table),
                       add=F, operation="append")
            lab.ops[side].append(op)
        return lab

    def test_shared_post_base_file_is_rejected(self):
        lab = self._shared()
        try:
            orc = lab.orc()
            self.assertFalse(orc["true_conflict"])  # both orders hold the appended row twice
            out = lab.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertIn("added after the base on both branches", _reasons(out))
            self.assertEqual(C.head_id(lab.table, "m"), C.head_id(lab.table, "A"))  # target unchanged
        finally:
            lab.close()

    def test_without_the_guard_the_alias_would_lose_a_row(self):
        """Documents why the guard exists: path-keyed composition keeps the shared file once."""
        lab = self._shared()
        try:
            lab.orc()
            with mock.patch.object(lineage, "validate_identity", return_value=[]):
                out = lab.merge()
            self.assertEqual(out.verdict, "merged")
            chk = lab.oracle.check_snapshot(lab.table, out.snapshot_id, ["o_ac", "o_ca"])
            self.assertFalse(chk["duckdb_eq_o_ac"] or chk["duckdb_eq_o_ca"])  # equal to neither order
        finally:
            lab.close()

    def test_re_registered_base_file_is_rejected(self):
        lab = Lab(T((1, 0)), T((2, 0), (3, 0)))
        try:
            B1 = lab.base_file(2)
            lab.commit("A", {}, remove={B1.path}, operation="delete")      # unrecorded drop of B1 ...
            lab.commit("A", {}, add=[B1.data_file], operation="append")    # ... and the same path re-added
            lab.apply("C", LogicalOp("APPEND", rows=T((9, 9)), mode="cow", key_col="k"))
            out = lab.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertIn("re-registers content file", _reasons(out))
        finally:
            lab.close()

    def test_identical_rows_in_fresh_files_are_fine(self):
        lab = Lab(T((1, 0)))
        try:
            for side in ("A", "C"):
                lab.apply(side, LogicalOp("APPEND", rows=T((5, 5)), mode="cow", key_col="k"))
            lab.orc()
            out = lab.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertTrue(lab.both_orders(out.snapshot_id))
        finally:
            lab.close()


# ============================================================================ G1: A2 phases

class OperationPhases(unittest.TestCase):
    def _chunked(self, source_op):
        """The re-review's example: OVERWRITE(x=7, {(7,7),(7,7)}) on {(1,0)} committed as a marker part
        and two insert parts of one row each (one operation id, op-end on the last part)."""
        lab = Lab(T((1, 0)))
        op = LogicalOp("OVERWRITE", filter=X.cmp("x", "=", 7), rows=T((7, 7), (7, 7)), mode="cow", key_col="k")
        mk = lambda part, end: op_properties(op, "A0", part=part, op_end=end, table=lab.table)
        lab.commit("A", mk("marker", False))
        lab.commit("A", mk("insert", False), add=C.write_data_files(lab.table, T((7, 7))), operation="append")
        lab.commit("A", mk("insert", True), add=C.write_data_files(lab.table, T((7, 7))), operation="append")
        lab.ops["A"].append(op)
        lab.apply("C", source_op)
        return lab, op

    def test_chunked_insertion_phase_is_supported_and_faithful(self):
        lab, op = self._chunked(LogicalOp("APPEND", rows=T((9, 9)), mode="cow", key_col="k"))
        try:
            chk = provenance.check_op(lab.table, lab.base_id, C.head_id(lab.table, "A"), op, NAMES)
            self.assertTrue(chk["ok"], chk)
            self.assertEqual(chk["roles"], ["marker", "insert", "insert"])
            orc = lab.orc()
            self.assertFalse(orc["true_conflict"])
            out = lab.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertTrue(lab.both_orders(out.snapshot_id))
        finally:
            lab.close()

    def test_chunked_insertion_phase_conflict_has_net_witness(self):
        lab, _ = self._chunked(LogicalOp("DELETE", filter=X.cmp("x", "=", 7), mode="cow", key_col="k"))
        try:
            self.assertTrue(lab.orc()["true_conflict"])
            out = lab.merge()
            self.assertEqual(out.category, "logical-conflict", out.to_json())
            w = out.witness
            self.assertEqual((w["H_A"], w["H_C"], w["order_difference"]), (2, 0, -2))
        finally:
            lab.close()

    def test_deletion_part_after_insertion_part_is_rejected(self):
        lab = Lab(T((1, 0)), T((2, 3), (4, 3)))  # B0 = {(1,0)} matches x=0 entirely; B1 is unrelated
        try:
            op = LogicalOp("OVERWRITE", filter=X.cmp("x", "=", 0), rows=T((5, 0)), mode="cow", key_col="k")
            mk = lambda part, end: op_properties(op, "A0", part=part, op_end=end, table=lab.table)
            lab.commit("A", mk("insert", False), add=C.write_data_files(lab.table, T((5, 0))), operation="append")
            lab.commit("A", mk("delete", True), remove={lab.base_file(1).path}, operation="delete")
            lab.ops["A"].append(op)
            lab.apply("C", LogicalOp("APPEND", rows=T((9, 9)), mode="cow", key_col="k"))
            out = lab.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertIn("deletion part after an insertion part", _reasons(out))
        finally:
            lab.close()

    def test_operation_deleting_its_own_insert_is_rejected(self):
        lab = Lab(T((1, 0)))
        try:
            op = LogicalOp("OVERWRITE", filter=X.cmp("x", "=", 0), rows=T((5, 0)), mode="cow", key_col="k")
            mk = lambda part, end: op_properties(op, "A0", part=part, op_end=end, table=lab.table)
            G = C.write_data_files(lab.table, T((5, 0), (6, 0)))
            lab.commit("A", mk("insert", False), add=G, operation="append")
            D = C.write_position_delete_file(lab.table, G[0], [1])        # deletes its own (6,0)
            lab.commit("A", mk("delete", True), add=[D], operation="delete")
            lab.ops["A"].append(op)
            chk = provenance.check_op(lab.table, lab.base_id, C.head_id(lab.table, "A"), op, NAMES)
            self.assertTrue(any("itself inserted" in v for v in chk["violations"]), chk)
            lab.apply("C", LogicalOp("APPEND", rows=T((9, 9)), mode="cow", key_col="k"))
            out = lab.merge()
            self.assertEqual(out.category, "unsupported", out.to_json())
            self.assertIn("position deletes on a data file the operation added", _reasons(out))
        finally:
            lab.close()


# ============================================================================ G2: read phases and budget

class ReadPhasesAndBudget(unittest.TestCase):
    def _logical_only(self):
        """No contended file: A appends {99,100,101}; C records DELETE x=100, which matches nothing
        on its own branch (a marker snapshot)."""
        lab = Lab(T((1, 0), (2, 1)))
        lab.apply("A", LogicalOp("APPEND", rows=T((10, 99), (11, 100), (12, 101)), mode="cow", key_col="k"))
        lab.apply("C", LogicalOp("DELETE", filter=X.cmp("x", "=", 100), mode="cow", key_col="k"))
        appended = [f for f in C.load_state(lab.table, C.head_id(lab.table, "A")).data_files() if f.record_count == 3]
        return lab, appended[0]

    def test_logical_reads_are_estimated_without_contention(self):
        lab, f = self._logical_only()
        try:
            lab.orc()
            out = lab.merge()
            self.assertEqual(out.category, "logical-conflict", out.to_json())
            est = out.details["estimate"]
            self.assertEqual(est["contended_files"], 0)
            self.assertEqual(est["phases"]["compose"]["bytes"], 0)
            self.assertEqual(est["phases"]["logical"]["bytes"], f.size)
            io = out.to_json()["io"]
            self.assertEqual(io["read_bytes_by_phase"].get("logical"), f.size)
            self.assertEqual(out.witness["example_row"], {"k": 11, "x": 100})
        finally:
            lab.close()

    def test_budget_counts_logical_reads_and_stops_before_any_data_read(self):
        lab, f = self._logical_only()
        try:
            out = lab.merge(RuleConfig(match_budget_bytes=f.size - 1))
            self.assertEqual((out.category, out.rule), ("unsupported", "over-budget"), out.to_json())
            self.assertEqual(out.stats.data_bytes_read + out.stats.delete_bytes_read, 0)
        finally:
            lab.close()

    def _mixed_example(self, writer):
        """The re-review's C1 example: the base file {x: 0,1,3,4}; A overwrites x=0 with x=2 (as one
        mixed snapshot with writer="single", or delete + insert parts with writer="split"); C deletes x=3
        (merge-on-read).  The 3 in A's new file is a survivor copy, not an insertion: the sides commute."""
        lab = Lab(T((1, 0), (2, 1), (3, 3), (4, 4)))
        lab.apply("A", LogicalOp("OVERWRITE", filter=X.cmp("x", "=", 0), rows=T((5, 2)), mode="cow", key_col="k"),
                  writer=writer)
        lab.apply("C", LogicalOp("DELETE", filter=X.cmp("x", "=", 3), mode="mor", key_col="k"))
        return lab

    def test_mixed_snapshot_recovery_reads_the_removed_base_file(self):
        lab = self._mixed_example("single")
        try:
            base_file = lab.base_file(4)
            lab.orc()
            out = lab.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertTrue(lab.both_orders(out.snapshot_id))
            by_phase = out.to_json()["io"]["read_bytes_by_phase"]
            self.assertGreaterEqual(by_phase.get("recovery", 0), base_file.size)  # base data was read
            est = out.details["estimate"]["phases"]
            for ph, n in by_phase.items():
                if ph in est:
                    self.assertGreaterEqual(est[ph]["bytes"], n, (ph, est, by_phase))  # estimate bounds reads
        finally:
            lab.close()

    def test_split_encoding_needs_no_recovery_reads(self):
        lab = self._mixed_example("split")
        try:
            lab.orc()
            out = lab.merge()
            self.assertEqual(out.verdict, "merged", out.to_json())
            self.assertTrue(lab.both_orders(out.snapshot_id))
            by_phase = out.to_json()["io"]["read_bytes_by_phase"]
            self.assertEqual(by_phase.get("recovery", 0), 0)
            self.assertEqual(out.details["estimate"]["phases"]["recovery"]["bytes"], 0)
        finally:
            lab.close()


# ============================================================================ G3: commit outcomes

def _metadata_files(lab):
    root = C.local_path(lab.table.metadata.location) + "/metadata"
    return {p for p in os.listdir(root)}


class CommitOutcomes(unittest.TestCase):
    def _lab(self):
        lab = Lab(T((1, 0)))
        lab.apply("A", LogicalOp("APPEND", rows=T((5, 5)), mode="cow", key_col="k"))
        lab.apply("C", LogicalOp("APPEND", rows=T((6, 6)), mode="cow", key_col="k"))
        lab.orc()
        return lab

    def test_installed_before_confirmation_is_recognised_as_committed(self):
        lab = self._lab()
        try:
            t = lab.branch()
            orig = SqlCatalog.commit_table

            def installed_then_lost(self_, table, requirements, updates):
                orig(self_, table, requirements, updates)
                raise ConnectionError("commit confirmation lost")

            with mock.patch.object(SqlCatalog, "commit_table", installed_then_lost):
                out = snapmerge(t, "m", "C")
            lab.table = out.table.refresh()
            self.assertEqual(out.category, "merged", out.to_json())
            self.assertEqual(out.details["commit"]["commit_resolution"], "committed")
            self.assertEqual(C.head_id(lab.table, "m"), out.snapshot_id)
            self.assertTrue(lab.both_orders(out.snapshot_id))  # every referenced file is still there
        finally:
            lab.close()

    def test_unresolvable_outcome_keeps_the_attempts_files(self):
        lab = self._lab()
        try:
            t = lab.branch()
            before = _metadata_files(lab)
            orig = SqlCatalog.commit_table
            state = {"broken": False}

            def installed_then_unreachable(self_, table, requirements, updates):
                orig(self_, table, requirements, updates)
                state["broken"] = True
                raise ConnectionError("catalog unreachable")

            orig_load = SqlCatalog.load_table

            def load(self_, identifier):
                if state["broken"]:
                    raise ConnectionError("catalog unreachable")
                return orig_load(self_, identifier)

            with mock.patch.object(SqlCatalog, "commit_table", installed_then_unreachable), \
                    mock.patch.object(SqlCatalog, "load_table", load):
                out = snapmerge(t, "m", "C")
            self.assertEqual(out.category, "commit-unknown", out.to_json())
            sid = out.witness["snapshot_id"]
            self.assertGreater(out.witness["retained_files"], 0)
            self.assertTrue(before < _metadata_files(lab))  # the attempt's files were not removed
            status, t2 = C.attempt_status(lab.table, "m", sid)  # catalog reachable again
            self.assertEqual(status, "committed")
            lab.table = t2
            self.assertTrue(lab.both_orders(sid))
        finally:
            lab.close()

    def test_failure_known_to_precede_installation_cleans_up(self):
        lab = self._lab()
        try:
            t = lab.branch()
            head = C.head_id(t, "m")

            def fails_before_install(self_, table, requirements, updates):
                raise ConnectionError("connection reset before the commit was sent")

            with mock.patch.object(SqlCatalog, "commit_table", fails_before_install):
                out = snapmerge(t, "m", "C")
            self.assertEqual((out.category, out.rule), ("retryable", "catalog-error"), out.to_json())
            self.assertEqual(out.details["commit"]["commit_resolution"], "not-committed")
            self.assertGreater(out.witness["cleaned_files"], 0)
            lab.table = lab.table.refresh()
            self.assertEqual(C.head_id(lab.table, "m"), head)
        finally:
            lab.close()

    def test_interrupt_inside_the_catalog_call_keeps_files(self):
        lab = self._lab()
        try:
            t = lab.branch()
            before = _metadata_files(lab)

            def hook(stage):
                if stage == "catalog_commit":
                    raise KeyboardInterrupt

            with self.assertRaises(KeyboardInterrupt):
                snapmerge(t, "m", "C", crash_hook=hook)
            self.assertTrue(before < _metadata_files(lab))
        finally:
            lab.close()


if __name__ == "__main__":
    unittest.main()
