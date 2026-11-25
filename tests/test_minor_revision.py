"""Regression tests for the 2026-10-08 minor revision (metareview M1 and M2).

M1 unknown-commit resolution.  A snapshot installed before its confirmation was lost stays recognised as
committed when the target advanced again before the status check, when the branch was reset meanwhile,
and when the catalog reports a rejected guarded call after all (the attempt's own snapshot is checked
before a moved target is treated as a failure; with that check disabled the cleanup would delete
manifests a later snapshot reads).  If the ancestry needed to decide is no longer retained, or catalog
calls are not settled, the outcome stays commit-unknown and the attempt's files are kept;
emit.resolve_unknown_commit removes them only on positive evidence of non-installation.
M2 early stops.  An insufficient budget on a true conflict is an unsupported early stop, not a logical
conflict with a witness.  The budget caps the charged bytes cumulatively: the preflight stops before any
data read, the reconciliation check stops after recovery and logical reads (reporting them), and a
runtime guard refuses any file opening that would exceed the budget.
"""
import os
import unittest
from unittest import mock

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException

from helpers import C, LogicalOp, X, snapmerge
from snapmerge import emit, rules
from snapmerge.rules import RuleConfig
from test_rereview import Lab, T


def _exists(paths):
    return [os.path.exists(C.local_path(p)) for p in paths]


def _InstallThen(after, exc):
    """Replacement for SqlCatalog.commit_table: the merge's own call installs its snapshot, runs
    `after(cat)` with nested commits going to the real catalog, and then raises `exc` (a lost or
    misreported confirmation).  A plain function, so that it binds as a method."""
    orig, state = SqlCatalog.commit_table, {"inside": False}

    def commit_table(cat, table, requirements, updates):
        if state["inside"]:
            return orig(cat, table, requirements, updates)
        orig(cat, table, requirements, updates)
        state["inside"] = True
        try:
            after(cat)
        finally:
            state["inside"] = False
        raise exc

    return commit_table


def _append_to(cat, branch, rows, tag):
    t = cat.load_table("t.t")
    t.append(rows, branch=branch, snapshot_properties={"snapmerge.op": "APPEND", "snapmerge.op-id": tag})
    return C.head_id(cat.load_table("t.t"), branch)


class CommitResolution(unittest.TestCase):
    def _lab(self):
        lab = Lab(T((1, 0)))
        lab.apply("A", LogicalOp("APPEND", rows=T((5, 5)), mode="cow", key_col="k"))
        lab.apply("C", LogicalOp("APPEND", rows=T((6, 6)), mode="cow", key_col="k"))
        lab.orc()
        return lab

    def test_installed_then_target_advanced_is_committed(self):
        lab = self._lab()
        try:
            t = lab.branch()
            heads = {}
            patch = _InstallThen(lambda cat: heads.update(n=_append_to(cat, "m", T((7, 7)), "n1")),
                                 ConnectionError("commit confirmation lost"))
            with mock.patch.object(SqlCatalog, "commit_table", patch):
                out = snapmerge(t, "m", "C")
            self.assertEqual(out.category, "merged", out.to_json())
            self.assertEqual(out.details["commit"]["commit_resolution"], "committed")
            lab.table = lab.table.refresh()
            self.assertEqual(C.head_id(lab.table, "m"), heads["n"])  # the target advanced past the merge
            self.assertTrue(lab.both_orders(out.snapshot_id))      # the merge snapshot is intact
            self.assertEqual(C.scan_pyiceberg(lab.table, heads["n"]).num_rows, 4)
        finally:
            lab.close()

    def test_ancestry_gap_stays_unknown_and_keeps_files(self):
        lab = self._lab()
        try:
            t = lab.branch()
            heads = {}

            def advance_twice_and_expire(cat):
                n1 = _append_to(cat, "m", T((7, 7)), "n1")
                heads["n2"] = _append_to(cat, "m", T((8, 8)), "n2")
                tt = cat.load_table("t.t")
                merge_sid = C.ancestors(tt, n1)[1].snapshot_id  # the attempt's snapshot, parent of n1
                heads["m"] = merge_sid
                tt.maintenance.expire_snapshots().by_ids([merge_sid, n1]).commit()

            with mock.patch.object(SqlCatalog, "commit_table",
                                   _InstallThen(advance_twice_and_expire, ConnectionError("commit confirmation lost"))):
                out = snapmerge(t, "m", "C")
            self.assertEqual(out.category, "commit-unknown", out.to_json())
            self.assertEqual(out.details["commit"]["commit_resolution"], "unknown")
            w = out.witness
            self.assertEqual(w["snapshot_id"], heads["m"])
            self.assertTrue(w["attempt_files"] and all(_exists(w["attempt_files"])))  # nothing removed
            lab.table = lab.table.refresh()
            n2_rows = C.scan_pyiceberg(lab.table, heads["n2"])  # still readable: it carries the merge's manifests
            self.assertEqual(sorted(n2_rows.column("k").to_pylist()), [1, 5, 6, 7, 8])
            self.assertEqual(emit.resolve_unknown_commit(lab.table, "m", w), ("unknown", 0))
            self.assertTrue(all(_exists(w["attempt_files"])))
        finally:
            lab.close()

    def test_reset_after_install_is_committed_while_the_snapshot_is_retained(self):
        lab = self._lab()
        try:
            t = lab.branch()
            planned = C.head_id(t, "m")

            def reset(cat):
                tt = cat.load_table("t.t")
                C.set_branch(tt, "m", planned, expected_head=C.head_id(tt, "m"))

            with mock.patch.object(SqlCatalog, "commit_table", _InstallThen(reset, ConnectionError("lost"))):
                out = snapmerge(t, "m", "C")
            self.assertEqual(out.category, "merged", out.to_json())
            lab.table = lab.table.refresh()
            self.assertEqual(C.head_id(lab.table, "m"), planned)  # reset by someone else afterwards
            self.assertTrue(lab.both_orders(out.snapshot_id))     # but the installed merge is intact
        finally:
            lab.close()

    def _rejected_after_install(self):
        heads = {}
        patch = _InstallThen(lambda cat: heads.update(n=_append_to(cat, "m", T((7, 7)), "n1")),
                             CommitFailedException("Requirement failed: branch m has changed"))
        return heads, patch

    def test_moved_target_checks_the_attempts_own_snapshot_first(self):
        lab = self._lab()
        try:
            t = lab.branch()
            heads, patch = self._rejected_after_install()
            with mock.patch.object(SqlCatalog, "commit_table", patch):
                out = snapmerge(t, "m", "C")
            self.assertEqual(out.category, "merged", out.to_json())
            self.assertEqual(out.details["commit"]["commit_resolution"], "committed")
            lab.table = lab.table.refresh()
            self.assertEqual(C.scan_pyiceberg(lab.table, heads["n"]).num_rows, 4)
        finally:
            lab.close()

    def test_without_the_own_snapshot_check_cleanup_would_break_a_later_snapshot(self):
        lab = self._lab()
        try:
            t = lab.branch()
            heads, patch = self._rejected_after_install()
            with mock.patch.object(SqlCatalog, "commit_table", patch), \
                    mock.patch.object(C, "attempt_status", lambda tb, *a, **k: ("not-committed", tb.refresh())):
                out = snapmerge(t, "m", "C")
            self.assertEqual((out.category, out.rule), ("retryable", "target-moved"), out.to_json())
            self.assertGreater(out.witness["cleaned_files"], 0)
            lab.table = lab.table.refresh()
            with self.assertRaises(Exception):  # n1 references the manifests the cleanup removed
                C.scan_pyiceberg(lab.table, heads["n"])
        finally:
            lab.close()

    def test_unsettled_calls_keep_an_absent_snapshot_unknown_until_resolved(self):
        lab = self._lab()
        try:
            t = lab.branch()
            planned = C.head_id(t, "m")

            def lost_before_install(cat, table, requirements, updates):
                raise TimeoutError("request timed out; the server may still apply it")

            with mock.patch.object(SqlCatalog, "commit_table", lost_before_install), \
                    mock.patch.object(C, "SETTLED_CATALOG_CALLS", False):
                out = snapmerge(t, "m", "C")
                self.assertEqual(out.category, "commit-unknown", out.to_json())
                w = out.witness
                self.assertEqual(w["expected_head"], planned)
                self.assertTrue(w["attempt_files"] and all(_exists(w["attempt_files"])))
                self.assertEqual(emit.resolve_unknown_commit(lab.table, "m", w), ("unknown", 0))
            # once the call is known to be settled (here: the patch is gone), absence is proof
            status, removed = emit.resolve_unknown_commit(lab.table, "m", w)
            self.assertEqual((status, removed), ("not-committed", len(w["attempt_files"])))
            self.assertFalse(any(_exists(w["attempt_files"])))
            lab.table = lab.table.refresh()
            self.assertEqual(C.head_id(lab.table, "m"), planned)
            self.assertEqual(C.scan_pyiceberg(lab.table, planned).num_rows, 2)  # inputs untouched
        finally:
            lab.close()

    def test_absence_without_the_planned_head_is_not_proof(self):
        lab = self._lab()
        try:
            t = lab.branch()
            status, _ = C.attempt_status(t, "m", 123456789, expected_head=None)
            self.assertEqual(status, "unknown")
            status, _ = C.attempt_status(t, "m", 123456789, expected_head=C.head_id(t, "m"))
            self.assertEqual(status, "not-committed")
        finally:
            lab.close()


class BudgetStages(unittest.TestCase):
    def _logical_only(self):
        lab = Lab(T((1, 0), (2, 1)))
        lab.apply("A", LogicalOp("APPEND", rows=T((10, 99), (11, 100), (12, 101)), mode="cow", key_col="k"))
        lab.apply("C", LogicalOp("DELETE", filter=X.cmp("x", "=", 100), mode="cow", key_col="k"))
        return lab

    def test_insufficient_budget_on_a_true_conflict_is_an_early_stop(self):
        lab = self._logical_only()
        try:
            lab.orc()
            full = lab.merge()
            self.assertEqual(full.category, "logical-conflict", full.to_json())
            self.assertIn("H_A", full.witness)
            lab2 = self._logical_only()
            try:
                out = lab2.merge(RuleConfig(match_budget_bytes=0))
                self.assertEqual((out.category, out.rule), ("unsupported", "over-budget"), out.to_json())
                self.assertEqual(out.witness["stage"], "preflight")
                self.assertEqual(out.witness["bytes_read"], 0)
                self.assertNotIn("H_A", out.witness)      # no commutation or conflict claim
                self.assertIsNone(out.snapshot_id)
            finally:
                lab2.close()
        finally:
            lab.close()

    def _identical_upserts(self):
        """Exact-equality case: both branches upsert the identical new row (990, 7), so the logical
        comparison finds equal H sets and duplicate reconciliation is needed."""
        lab = Lab(T((1, 0)))
        lab.apply("A", LogicalOp("UPSERT", keys=[990], rows=T((990, 7)), mode="mor", key_col="k"))
        lab.apply("C", LogicalOp("UPSERT", keys=[990], rows=T((990, 7)), mode="cow", key_col="k"))
        return lab

    def test_reconciliation_stop_reports_bytes_already_read_within_the_cap(self):
        lab = self._identical_upserts()
        try:
            lab.orc()
            full = lab.merge()
            self.assertEqual(full.category, "merged", full.to_json())
            self.assertTrue(lab.both_orders(full.snapshot_id))
            ph = full.details["estimate"]["phases"]
            initial = ph["recovery"]["bytes"] + ph["logical"]["bytes"] + ph["compose"]["bytes"]
            self.assertGreater(ph["logical"]["bytes"], 0)
            self.assertGreater(ph["reconcile"]["bytes"], 0)
        finally:
            lab.close()
        lab = self._identical_upserts()
        try:
            head = C.head_id(lab.table, "A")
            out = lab.merge(RuleConfig(match_budget_bytes=initial))  # passes the preflight, not reconciliation
            self.assertEqual((out.category, out.rule), ("unsupported", "over-budget"), out.to_json())
            w, ph2 = out.witness, out.details["estimate"]["phases"]
            self.assertEqual(w["stage"], "reconcile")
            self.assertGreater(w["bytes_read"], 0)                   # recovery + logical reads already spent
            self.assertLessEqual(w["bytes_read"], initial)           # ... within the cap
            self.assertEqual(w["remaining_estimate_bytes"], ph2["compose"]["bytes"] + ph2["reconcile"]["bytes"])
            self.assertGreater(w["estimate_bytes"], initial)         # the revised total exceeds the cap
            self.assertEqual(out.stats.charged(), w["bytes_read"])
            self.assertEqual(C.head_id(lab.table.refresh(), "m"), head)  # nothing emitted
        finally:
            lab.close()

    def test_runtime_guard_refuses_an_opening_over_the_cap(self):
        lab = self._logical_only()
        try:
            def blind_estimate(lin, cfg=None, table_schema=None, arrow_schema=None, stats=None):
                zero = {"bytes": 0, "files": 0, "rows": 0}
                return {"unit": "test", "contended_partitions": 0, "contended_files": 0, "read_files": 0,
                        "read_rows": 0, "read_bytes": 0, "compose_max_partition_rows": 0,
                        "phases": {k: dict(zero) for k in ("recovery", "logical", "compose")}}

            with mock.patch.object(rules, "read_estimate", blind_estimate):
                out = lab.merge(RuleConfig(match_budget_bytes=1))
            self.assertEqual((out.category, out.rule), ("unsupported", "over-budget"), out.to_json())
            self.assertEqual(out.witness["stage"], "read-guard")
            self.assertLessEqual(out.stats.charged(), 1)
            self.assertIsNone(out.stats.read_limit)  # the cap applies to one decision only
        finally:
            lab.close()


if __name__ == "__main__":
    unittest.main()
