"""SnapMerge entry point: lineage -> rules -> emit.

Outcome categories (2026-10-05, review R4 MC4), in MergeOutcome.category:
  merged            a snapshot equal to both whole-branch serial orders was committed (or no-op)
  logical-conflict  the recorded operations do not commute; witness = net-effect certificate
  unsupported       outside the supported contract: format subset (A3), missing/contradictory/
                    incomplete metadata or provenance (A1/A2 validation), missing history
  physical-conflict the logic commutes but the composition failed closed (matching, dangling delete,
                    duplicate-reconciliation shortfall)
  ablation          a disabled rule family (experiments only)
  retryable         the merge snapshot is KNOWN not to be installed: the target branch moved between
                    planning and commit, or the catalog failed and its status shows the snapshot absent;
                    the attempt's files were removed; re-plan and retry
  commit-unknown    the catalog call failed and the status check could not tell whether the snapshot
                    was installed (re-review 2026-10-07, G3): the attempt's files are RETAINED and
                    witness["snapshot_id"] identifies the attempt; resolve with
                    emit.resolve_unknown_commit(table, target, witness) before retrying or cleaning up
                    (it needs positive evidence of non-installation; minor revision 2026-10-08, M1)
Only "merged" emits a snapshot.  "logical-conflict" is the only outcome that certifies a conflict (a
net-effect witness); "unsupported" -- including an over-budget stop, whose witness reports the stage and
the bytes already read -- certifies neither commutation nor conflict (minor revision 2026-10-08, M2).
The source is PINNED: a branch name is resolved to its head once, before planning, and that
snapshot id is what is merged and recorded; later source commits are not merged and do not abort
the merge.  Commits on other branches are absorbed by re-sequencing (catalog.commit_snapshot).
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Callable

from pyiceberg.table import Table

from . import catalog as C
from .emit import emit_merge
from .lineage import compute_lineage, resolve
from .rules import Decision, RuleConfig, decide

_CATEGORY = {"logical": "logical-conflict", "structural": "unsupported", "physical": "physical-conflict",
             "ablation": "ablation", "unsupported": "unsupported", "retryable": "retryable",
             "commit-unknown": "commit-unknown"}


@dataclass
class MergeOutcome:
    policy: str
    verdict: str                      # merged | conflict
    snapshot_id: int | None = None    # head of the target branch after the merge (None on conflict)
    conflict_class: str | None = None
    rule: str | None = None
    witness: dict | None = None
    details: dict = field(default_factory=dict)
    stats: C.IOStats = field(default_factory=C.IOStats)
    latency_s: float = 0.0
    table: Table | None = None
    category: str | None = None
    source_snapshot: int | None = None

    @property
    def emitted(self) -> bool:
        return self.verdict == "merged"

    def to_json(self) -> dict:
        cat = self.category or ("merged" if self.verdict == "merged"
                                else _CATEGORY.get(self.conflict_class or "", "conflict"))
        return {"policy": self.policy, "verdict": self.verdict, "snapshot_id": self.snapshot_id,
                "conflict_class": self.conflict_class, "category": cat, "rule": self.rule, "witness": self.witness,
                "source_snapshot": self.source_snapshot,
                "details": self.details, "io": self.stats.as_dict(), "latency_ms": round(self.latency_s * 1e3, 3)}


def snapmerge(table: Table, target: str, source: str | int, cfg: RuleConfig | None = None,
              crash_hook: Callable[[str], None] | None = None, policy: str = "snapmerge") -> MergeOutcome:
    cfg = cfg or RuleConfig()
    stats = C.IOStats()
    t0 = time.perf_counter()
    out = MergeOutcome(policy=policy, verdict="conflict", stats=stats)
    out.source_snapshot = resolve(table, source)  # pinned before planning
    try:
        stats.phase = "lineage"
        lin = compute_lineage(table, target, out.source_snapshot, stats)
        stats.phase = "other"
        t_lin = time.perf_counter()
        arrow_schema = table.schema().as_arrow()
        dec: Decision = decide(lin, cfg, table.schema(), arrow_schema, stats)
    except FileNotFoundError as e:  # expired snapshot metadata or removed data/delete files
        out.conflict_class, out.rule, out.category = "unsupported", "missing-history", "unsupported"
        out.witness = {"missing": str(e)[:300]}
        out.latency_s = time.perf_counter() - t0
        out.table = table
        return out
    out.details = dec.to_json()
    out.details.setdefault("timing_ms", {})["lineage"] = round((t_lin - t0) * 1e3, 3)
    if dec.verdict == "conflict":
        out.conflict_class, out.rule, out.witness = dec.conflict_class, dec.rule, dec.witness
        out.category = _CATEGORY.get(dec.conflict_class, "conflict")
        out.latency_s = time.perf_counter() - t0
        out.table = table
        return out
    if dec.rule == "noop":
        out.verdict, out.snapshot_id, out.table, out.category = "merged", lin.heads["A"], table, "merged"
        out.latency_s = time.perf_counter() - t0
        return out
    # Retention: every file the merge snapshot newly references must still exist (a carried source
    # file may never have been read, e.g. when bounds excluded it, so a missing object would
    # otherwise only surface when readers scan the merged snapshot).
    missing = [lf.path for lf in dec.plan.carry if not os.path.exists(C.local_path(lf.path))]
    if missing:
        out.conflict_class, out.rule, out.category = "unsupported", "missing-history", "unsupported"
        out.witness = {"missing": missing[0].rsplit("/", 1)[-1], "missing_files": len(missing)}
        out.latency_s = time.perf_counter() - t0
        out.table = table
        return out
    info: dict = {}
    t_emit = time.perf_counter()
    try:
        table, sid = emit_merge(table, target, lin, dec.plan, policy=policy, stats=stats, crash_hook=crash_hook,
                                extra_props={"snapmerge.rules": cfg.tag()}, info=info)
    except C.TargetMoved as e:
        out.conflict_class, out.rule, out.category = "retryable", "target-moved", "retryable"
        out.witness = {"error": str(e)[:300], "cleaned_files": info.get("cleaned_files", 0)}
        out.details["commit"] = {k: v for k, v in info.items() if k != "snapshot_id"}
        out.latency_s = time.perf_counter() - t0
        out.table = table.refresh()
        return out
    except C.CommitStateUnknown as e:
        out.conflict_class, out.rule, out.category = "commit-unknown", "commit-state-unknown", "commit-unknown"
        out.witness = {"error": str(e)[:300], "snapshot_id": e.snapshot_id,
                       "expected_head": info.get("expected_head", lin.heads["A"]),
                       "retained_files": info.get("retained_files", 0),
                       "attempt_files": info.get("attempt_files", [])}
        out.details["commit"] = {k: v for k, v in info.items() if k != "snapshot_id"}
        out.latency_s = time.perf_counter() - t0
        out.table = table
        return out
    except C.CommitAborted as e:  # catalog error, snapshot known not to be installed
        out.conflict_class, out.rule, out.category = "retryable", "catalog-error", "retryable"
        out.witness = {"error": str(e)[:300], "cleaned_files": info.get("cleaned_files", 0)}
        out.details["commit"] = {k: v for k, v in info.items() if k != "snapshot_id"}
        out.latency_s = time.perf_counter() - t0
        out.table = table
        return out
    out.details["resequenced"] = info.get("resequenced", 0)
    if "commit_resolution" in info:  # the catalog raised, the status check found the snapshot installed
        out.details["commit"] = {k: v for k, v in info.items() if k != "snapshot_id"}
    out.details["timing_ms"]["emit"] = round((time.perf_counter() - t_emit) * 1e3, 3)
    out.verdict, out.snapshot_id, out.table, out.category = "merged", sid, table, "merged"
    out.latency_s = time.perf_counter() - t0
    return out
