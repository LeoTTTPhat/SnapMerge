"""Merge-snapshot emission.

Builds the merge snapshot on the target branch from a CompositionPlan:
  * writes the plan's new file-scoped position delete files (ADDED -> they inherit the merge
    snapshot's sequence number, which is above the data sequence number of every file they
    reference, so they apply under the v2 rule data_seq <= delete_seq);
  * re-uses target-head manifests that lose no entry, rewrites the others (EXISTING / DELETED);
  * carries source-side files as EXISTING entries with their ORIGINAL data and file sequence
    numbers (re-adding them as ADDED would give data files a newer sequence number than the
    source-side delete files that reference them, silently dropping those deletes);
  * records both parents (target head = snapshot parent, source head in
    snapmerge.merge-source-head) and commits atomically with AssertRefSnapshotId(target head).
Failed attempts (2026-10-05; outcome-aware since the 2026-10-07 re-review, G3): the delete files,
manifests and manifest lists an attempt wrote are removed (best effort) only when the snapshot is
KNOWN not to be installed -- the failure happened before the catalog was called, the catalog rejected
the commit, or a status check after a catalog error shows the snapshot absent (catalog.attempt_status).
If the catalog call failed and its outcome cannot be determined, the files are RETAINED
(catalog.CommitStateUnknown): an installed snapshot may reference them.  A process crash leaves the
files of an uninstalled attempt unreferenced, for Iceberg's age-thresholded orphan-file removal.
Minor revision 2026-10-08 (M1): the status check needs positive evidence of non-installation (complete
retained ancestry from the branch head back to the planned head, settled catalog calls); a target that
moved is checked for the attempt's own snapshot first; an unknown outcome reports the planned head and
the attempt-owned files, and resolve_unknown_commit() removes those files (never input files) only once
attempt_status() proves that the snapshot was not installed.
A commit lost to another branch's commit is re-sequenced and retried by catalog.commit_snapshot;
a moved target raises catalog.TargetMoved (retryable: re-plan).
"""
from __future__ import annotations

import os

from typing import Callable

from pyiceberg.table import Table

from . import catalog as C
from .lineage import Lineage
from .rules import CompositionPlan


def merge_properties(lin: Lineage, policy: str, extra: dict | None = None) -> dict[str, str]:
    props = {
        "snapmerge.merge": "true",
        "snapmerge.merge-policy": policy,
        "snapmerge.merge-base": str(lin.base_id),
        "snapmerge.merge-target-head": str(lin.heads["A"]),
        "snapmerge.merge-source-head": str(lin.heads["C"]),
    }
    props.update(extra or {})
    return props


def remove_files(paths: list[str]) -> int:
    """Best-effort removal of files written by a failed attempt; returns how many were removed."""
    n = 0
    for p in paths:
        try:
            os.remove(C.local_path(p))
            n += 1
        except OSError:
            pass
    return n


def emit_merge(table: Table, target: str, lin: Lineage, plan: CompositionPlan, policy: str = "snapmerge",
               stats: C.IOStats | None = None, crash_hook: Callable[[str], None] | None = None,
               extra_props: dict | None = None, info: dict | None = None) -> tuple[Table, int]:
    written: list[str] = []
    if info is None:
        info = {}  # always tracked: the cleanup decision depends on info["catalog_call"]
    try:
        added = []
        for tpath in sorted(plan.new_deletes):
            target_df, positions = plan.new_deletes[tpath]
            df = C.write_position_delete_file(table, target_df, positions, stats)
            written.append(df.file_path)
            added.append(df)
        if crash_hook is not None:
            crash_hook("after_delete_files")
        props = merge_properties(lin, policy, extra_props)
        props["snapmerge.merge-new-delete-files"] = str(len(added))
        props["snapmerge.merge-carried-files"] = str(len(plan.carry))
        return C.commit_delta(table, target, lin.states["A"], remove_paths=set(plan.remove), add_files=added,
                              carry_entries=[lf.entry for lf in plan.carry], operation="overwrite",
                              properties=props, stats=stats, crash_hook=crash_hook, written=written, info=info)
    except BaseException:
        call = info.get("catalog_call")
        if call in (None, "definite-abort"):
            info["cleaned_files"] = remove_files(written)
        else:
            info["retained_files"] = len(written)  # possibly referenced by an installed snapshot
            info["attempt_files"] = list(written)  # attempt-owned: new delete files, manifests, manifest lists
            info["expected_head"] = lin.heads["A"]
        raise


def resolve_unknown_commit(table: Table, branch: str, witness: dict) -> tuple[str, int]:
    """Resolve a commit-unknown outcome later (minor revision 2026-10-08, M1).  `witness` is the outcome's
    witness (snapshot_id, expected_head, attempt_files).  Returns (status, files removed): the attempt's
    own files are removed only when catalog.attempt_status() establishes non-installation; on "committed"
    and "unknown" nothing is removed.  Input files (base, heads, carried files) are never touched."""
    status, _ = C.attempt_status(table, branch, witness["snapshot_id"], witness.get("expected_head"))
    if status != "not-committed":
        return status, 0
    return status, remove_files(list(witness.get("attempt_files") or []))
