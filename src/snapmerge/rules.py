"""Commutativity rules and physical composition.

Logical model.  Every recorded operation o has the form  o(T) = (T minus rows satisfying P_o) + I_o
(APPEND: P=FALSE; DELETE: P=phi, I={}; OVERWRITE: P=phi, I=R; UPSERT: P=key IN K, I=R with
keys(R)=K; REPLACE: P=FALSE, I={} -- identity on the row multiset).  For two such operations
    a(c(T)) = T|~Pa~Pc + Ic|~Pa + Ia      c(a(T)) = T|~Pa~Pc + Ia|~Pc + Ic
so a and c commute (on every state, equivalently on the given base) iff I_c|P_a = I_a|P_c as
multisets; in particular they commute when no row of I_a satisfies P_c and no row of I_c
satisfies P_a.  A sequence of such operations is again one such operation (predicate = OR of
the predicates, inserts = inserted rows not deleted by a later operation of the same side), so
SnapMerge decides the two sequences' commutation exactly from their net effects (check_logical).
The pairwise rule families used for reporting and ablation are:

  R1 APPEND/APPEND            commute
  R2 APPEND vs DELETE/OVERWRITE  inserted rows vs predicate: column bounds, then bounded read
  R3 DELETE/DELETE            commute (physical: delete-file union / COW-to-position conversion)
  R4 UPSERT/UPSERT            commute iff key sets are disjoint (witness: a shared key)
  R5 OVERWRITE/OVERWRITE      inserted rows of each side vs the other side's predicate
  R6 REPLACE vs anything      logically commute; physical rebase of positions over compaction
  R7 unknown predicate / schema or spec change / equality deletes / DVs: conflict

Physical composition (when the logic commutes).  A base data file is *contended* if one side
removed it (copy-on-write rewrite, drop, or compaction) and the other side removed it too or
added position deletes against it.  Outside contended partitions the source side's physical
delta is applied to the target head file-by-file (both sides' delete files are unioned).  In a
contended partition p one side's layout L is kept (the side that compacted p if only one did,
else the target) and the other side O is re-expressed relative to L by bounded reads:
each side's removed base rows are matched (exact full-row multiset matching) to its new rows,
which yields, per side, the base positions it deleted, the copies it made, and the rows it
inserted.  Base rows O deleted become position deletes at their location in L (the base file
itself or L's copy, e.g. inside L's compaction output); O's new files are kept only if they
contain inserted rows, with position deletes on their copy rows.  New delete files are added
in the merge snapshot (sequence number above every data file they reference); carried files
keep their original data sequence numbers.

Revision 2026-10-05 (review package).  (1) Insert discovery follows lineage.OpRecord.insert_sources:
a snapshot that removes and adds data files contributes its added rows minus the survivor copies of
the rows it removed (exact multiset difference, containment checked), so the decision no longer
depends on how a writer splits a copy-on-write operation into snapshots.  (2) Row matching is exact:
hashes only bucket rows, colliding buckets are matched on canonical values, and NULL matches NULL
(IS NOT DISTINCT FROM), so the exact-equality test is an iff and never fails closed on a collision.
(3) A logical conflict carries a net-effect witness: a value v, its multiplicities H_A(v) in
I_A|P_C and H_C(v) in I_C|P_A, the resulting difference (A;C)(v) - (C;A)(v) = H_C(v) - H_A(v), and
the inserting and deleting operations.

Re-review 2026-10-07 (G2).  The commutation criterion is independent of the base STATE once the net
inserted multisets are known; obtaining them can read data: insert-role files whose bounds may meet
the other side's predicates (phase "logical"), and for snapshots that mix survivor copies with inserts
the files they added and the files they removed -- possibly base files -- (phase "recovery", also used
to validate schema-v2 inserted-row counts).  Composition reads the contended partitions ("compose")
and duplicate reconciliation the planned files that may hold duplicated values ("reconcile").
read_estimate() predicts the data and delete bytes of every phase from manifest metadata and column
bounds alone (an upper bound under the same bounds evaluation: re-reads count again), and decide()
enforces cfg.match_budget_bytes on it BEFORE any data is read (reconciliation is estimated and
checked again once the duplicated values are known, before composition).  IOStats attributes the
bytes actually read to the same phases.

Minor revision 2026-10-08 (M2).  The budget caps the CHARGED bytes of one decision, cumulatively over the
four phases: every opening of a data or delete file is charged its recorded size, re-reads again
(IOStats.admit/charge).  Three stages enforce it: "preflight" (sum of the recovery, logical and compose
estimates; no data read yet), "reconcile" (after recovery and logical reads, once duplicated values are
known; the bytes already charged are reported) and a runtime guard that refuses any opening that would
exceed the budget ("read-guard"; never triggered in our campaigns because no phase exceeded its
estimate).  An over-budget stop is an unsupported outcome: it emits nothing and is not a logical
conflict, so the exact decision and witness statements apply to decisions that reach the comparison.
"""
from __future__ import annotations

import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import DataFile

from . import catalog as C
from . import expr as X
from .lineage import Lineage, OpRecord


@contextmanager
def _phase(stats: C.IOStats | None, name: str):
    """Attribute data/delete bytes read inside the block to merge phase `name` (re-review G2)."""
    if stats is None:
        yield
        return
    prev = stats.phase
    stats.phase = name
    try:
        yield
    finally:
        stats.phase = prev


@dataclass
class RuleConfig:
    mode: str = "recorded"         # recorded | filelevel (ignore recorded predicates)
    bounded_reads: bool = True     # rule family: bounded exact reads to resolve "maybe"
    delete_union: bool = True      # rule family: delete-file union and COW-to-position conversion
    compaction_rebase: bool = True # rule family: position rebase over compaction groups
    net_effect: bool = True        # exact net-effect commutation test (False: pairwise, sufficient only)
    discovery: str = "provenance"  # provenance | append_rule (the submitted version's literal rule)
    directions: str = "both"       # both | rebase (only the source's predicates vs the target's inserts)
    match_budget_bytes: int | None = None  # fail closed (unsupported) if the estimated data+delete bytes of ALL
                                           # read phases (read_estimate) exceed it; checked before any data read

    def tag(self) -> str:
        off = [n for n in ("bounded_reads", "delete_union", "compaction_rebase", "net_effect")
               if not getattr(self, n)]
        t = self.mode + ("" if not off else "-no_" + "-no_".join(off))
        if self.discovery != "provenance":
            t += "-" + self.discovery
        if self.directions != "both":
            t += "-" + self.directions
        return t


@dataclass
class CompositionPlan:
    remove: set[str] = field(default_factory=set)
    carry: list[C.LiveFile] = field(default_factory=list)
    new_deletes: dict[str, tuple[DataFile, set[int]]] = field(default_factory=dict)
    contended_partitions: list = field(default_factory=list)
    layouts: dict = field(default_factory=dict)
    matched_rows: int = 0
    final: dict = field(default_factory=dict)       # path -> LiveFile of the merged state
    duplicates_removed: int = 0

    def add_deletes(self, target: DataFile, positions) -> None:
        positions = {int(p) for p in positions}
        if not positions:
            return
        cur = self.new_deletes.get(target.file_path)
        if cur is None:
            self.new_deletes[target.file_path] = (target, positions)
        else:
            cur[1].update(positions)


@dataclass
class Decision:
    verdict: str                    # merge | conflict
    conflict_class: str | None = None   # logical | structural | physical | ablation
    rule: str | None = None
    witness: dict | None = None
    plan: CompositionPlan | None = None
    hits: Counter = field(default_factory=Counter)
    estimate: dict | None = None
    timing_ms: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        d = {"verdict": self.verdict, "conflict_class": self.conflict_class, "rule": self.rule,
             "witness": self.witness, "hits": dict(self.hits)}
        if self.estimate is not None:
            d["estimate"] = self.estimate
        if self.timing_ms:
            d["timing_ms"] = self.timing_ms
        if self.plan is not None:
            d["plan"] = {"removed": len(self.plan.remove), "carried": len(self.plan.carry),
                         "new_delete_files": len(self.plan.new_deletes),
                         "new_deleted_positions": sum(len(v[1]) for v in self.plan.new_deletes.values()),
                         "contended_partitions": len(self.plan.contended_partitions),
                         "layouts": sorted(set(self.plan.layouts.values())),
                         "matched_rows": self.plan.matched_rows}
        return d


class MatchError(RuntimeError):
    pass


def _conflict(cls: str, rule: str, witness: dict, hits: Counter) -> Decision:
    return Decision("conflict", cls, rule, witness, None, hits)


def pair_rule(a: OpRecord, c: OpRecord) -> str:
    kinds = {a.kind.lstrip("?"), c.kind.lstrip("?")}
    if not (a.recorded and c.recorded) and kinds - {"APPEND", "REPLACE"}:
        return "R7"
    if "REPLACE" in kinds:
        return "R6"
    if kinds == {"APPEND"}:
        return "R1"
    if kinds == {"DELETE"}:
        return "R3"
    if kinds == {"UPSERT"}:
        return "R4"
    if "APPEND" in kinds:
        return "R2"
    return "R5"


def _pred_for(op: OpRecord, cfg: RuleConfig) -> dict | None:
    if cfg.mode == "filelevel":
        if op.kind.lstrip("?") in ("APPEND", "REPLACE"):
            return dict(X.FALSE)
        return None
    return op.delete_pred()


# ============================================================================ logical rules

def _jsonable_row(row: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}


def _eval_any(preds: list[dict], rows: pa.Table) -> np.ndarray:
    mask = np.zeros(rows.num_rows, bool)
    for pr in preds:
        mask |= X.eval_arrow(pr, rows).to_numpy(zero_copy_only=False)
    return mask


def _directions(cfg: RuleConfig) -> tuple[tuple[str, str], ...]:
    # (side whose inserts are checked, side whose predicates are checked)
    return (("A", "C"),) if cfg.directions == "rebase" else (("A", "C"), ("C", "A"))


@dataclass
class _Chunk:
    """Provenance of consecutive hit rows: inserting op, the predicates that may match them, and per
    row the snapshot, file and position the row was read from (built lazily for witnesses)."""
    op: OpRecord
    maybe: list
    snapshot_ids: np.ndarray
    paths: np.ndarray
    positions: np.ndarray

    def __len__(self) -> int:
        return int(self.positions.size)

    def take(self, mask: np.ndarray) -> "_Chunk":
        return _Chunk(self.op, self.maybe, self.snapshot_ids[mask], self.paths[mask], self.positions[mask])

    def prov(self, k: int) -> dict:
        return {"op": self.op.op_id, "snapshot": int(self.snapshot_ids[k]),
                "file": str(self.paths[k]).rsplit("/", 1)[-1], "pos": int(self.positions[k])}


def _locate(chunks: list[_Chunk], k: int) -> tuple[_Chunk, int]:
    for ch in chunks:
        if k < len(ch):
            return ch, k
        k -= len(ch)
    raise IndexError(k)


def _survivors(lin: Lineage, op: OpRecord, snap, names: list[str], keep_pred, stats: C.IOStats | None,
               file_filter=None) -> pa.Table | None:
    """Live rows (at the snapshot's input state) of the data files `snap` removed that do NOT
    satisfy the operation's predicate (IS NOT TRUE) and do satisfy `keep_pred` (if given)."""
    parent = lin.state_before(snap, stats)
    p_o = op.delete_pred()
    parts = []
    for f in sorted(snap.removed_data(), key=lambda x: x.path):
        if file_filter is not None and not file_filter(f):
            continue
        rows, _ = C.live_rows(parent, parent.files.get(f.path, f), stats)
        rows = rows.select(names)
        keep = np.ones(rows.num_rows, bool)
        if p_o is not None and not X.is_false(p_o):
            keep &= ~X.eval_arrow(p_o, rows).to_numpy(zero_copy_only=False)
        if keep_pred is not None:
            keep &= keep_pred(rows)
        if keep.any():
            parts.append(rows.filter(pa.array(keep)))
    return pa.concat_tables(parts) if parts else None


def _mixed_inserts(lin: Lineage, op: OpRecord, snap, evs: list, cfg: RuleConfig, names: list[str],
                   stats: C.IOStats | None, hits: Counter):
    """Rows inserted by a mixed snapshot that may satisfy a relevant predicate, with provenance:
    (added rows satisfying Q) minus (survivor copies satisfying Q), an exact multiset difference.
    Returns None (nothing can match), a Decision (fail closed), or (rows, provenance, maybe)."""
    hits["provenance:mixed-snapshot"] += 1
    added = sorted(snap.added_data(), key=lambda f: f.path)
    may = {f.path: [(oy, pr) for oy, pr, ev in evs if ev.eval(f.data_file)] for f in added}
    files = [f for f in added if may[f.path]]
    if not files:
        hits["bounds:cannot-match"] += len(added)
        return None
    if not cfg.bounded_reads:
        return _conflict("ablation", pair_rule(op, may[files[0].path][0][0]) + ":bounds",
                         {"ops": [op.describe(), may[files[0].path][0][0].describe()],
                          "reason": "column bounds inconclusive; bounded reads disabled"}, hits)
    maybe, seen = [], set()
    for f in files:
        for oy, pr in may[f.path]:
            if id(pr) not in seen:
                seen.add(id(pr))
                maybe.append((oy, pr))
    q = [pr for _, pr in maybe]
    q_evs = [ev for oy, pr, ev in evs if id(pr) in seen]
    cand, src = [], []
    for f in files:
        rows = C.read_data_file(f, stats).select(names)
        hits["bounded-read"] += 1
        m = _eval_any(q, rows)
        if m.any():
            cand.append(rows.filter(pa.array(m)))
            src += [(f.path, int(i)) for i in np.flatnonzero(m)]
    if not cand:
        hits["bounded-read:clear"] += 1
        return None
    cand_t = pa.concat_tables(cand)
    surv = _survivors(lin, op, snap, names, lambda r: _eval_any(q, r), stats,
                      file_filter=lambda f: any(ev.eval(f.data_file) for ev in q_evs))
    keep = np.ones(cand_t.num_rows, bool)
    if surv is not None and surv.num_rows:
        li, ri = match_rows(cand_t, surv)
        if ri.size != surv.num_rows:
            return _conflict("structural", "provenance:survivor-not-copied",
                             {"op": op.describe(), "snapshot": snap.snapshot_id,
                              "uncopied_survivors": int(surv.num_rows - ri.size)}, hits)
        keep[li] = False
        hits["provenance:copies-separated"] += int(li.size)
    if not keep.any():
        return None
    rows = cand_t.filter(pa.array(keep))
    idx = np.flatnonzero(keep)
    chunk = _Chunk(op, maybe, np.full(idx.size, snap.snapshot_id, np.int64),
                   np.array([src[k][0] for k in idx.tolist()], dtype=object),
                   np.array([src[k][1] for k in idx.tolist()], np.int64))
    return rows, chunk


def mixed_insert_rows(lin: Lineage, op: OpRecord, snap, names: list[str],
                      stats: C.IOStats | None) -> tuple[bool, pa.Table | None]:
    """(ok, I_s) of a mixed snapshot (full, unpruned): its added rows minus the survivor copies, as
    an exact multiset difference.  ok=False if some survivor is not copied (A2 violated)."""
    added = [C.read_data_file(f, stats).select(names) for f in sorted(snap.added_data(), key=lambda f: f.path)]
    added_t = pa.concat_tables(added) if added else None
    surv = _survivors(lin, op, snap, names, None, stats)
    if surv is None or surv.num_rows == 0:
        return True, added_t
    if added_t is None:
        return False, None
    li, ri = match_rows(added_t, surv)
    if ri.size != surv.num_rows:
        return False, None
    keep = np.ones(added_t.num_rows, bool)
    keep[li] = False
    return True, added_t.filter(pa.array(keep))


def mixed_insert_count(lin: Lineage, op: OpRecord, snap, names: list[str], stats: C.IOStats | None) -> int | None:
    ok, rows = mixed_insert_rows(lin, op, snap, names, stats)
    if not ok:
        return None
    return 0 if rows is None else rows.num_rows


def _check_inserted_counts(lin: Lineage, cfg: RuleConfig, names: list[str], stats: C.IOStats | None,
                           hits: Counter) -> Decision | None:
    """Schema-v2 operations with mixed parts: the recorded inserted-row count must equal the rows the
    insert parts hold plus the exact inserts of the mixed parts (lineage.validate_metadata checks
    operations without mixed parts from metadata alone)."""
    if cfg.discovery != "provenance" or cfg.mode != "recorded":
        return None
    for side in ("A", "C"):
        for op in lin.ops[side]:
            if not op.recorded or op.schema_version < 2 or op.inserted_records is None:
                continue
            roles = op.snapshot_roles()
            if not any(r == "mixed" for _, r in roles):
                continue
            got = 0
            for ch, r in roles:
                if r == "insert":
                    got += sum(f.record_count for f in ch.added_data())
                elif r == "mixed":
                    n = mixed_insert_count(lin, op, ch, names, stats)
                    hits["provenance:mixed-count-read"] += 1
                    if n is None:
                        return _conflict("structural", "provenance:survivor-not-copied",
                                         {"op": op.describe(), "snapshot": ch.snapshot_id}, hits)
                    got += n
            if got != op.inserted_records:
                return _conflict("structural", "metadata:inserted-count",
                                 {"op": op.describe(), "found": got, "recorded": op.inserted_records}, hits)
    return None


def _net_witness(tabs: dict, chunks: dict) -> dict:
    """A value whose multiplicities differ between I_A|P_C and I_C|P_A, with provenance."""
    nA, nC = tabs["A"].num_rows, tabs["C"].num_rows
    if nA and nC:
        li, ri = match_rows(tabs["A"], tabs["C"])
    else:
        li, ri = np.empty(0, np.int64), np.empty(0, np.int64)
    un_a = np.setdiff1d(np.arange(nA), li)
    un_c = np.setdiff1d(np.arange(nC), ri)
    side, k = ("A", int(un_a[0])) if un_a.size else ("C", int(un_c[0]))
    other = "C" if side == "A" else "A"
    v = tabs[side].slice(k, 1)
    key = canon_rows(v)[0]
    h = {s: sum(1 for r in canon_rows(tabs[s]) if r == key) if tabs[s].num_rows else 0 for s in ("A", "C")}
    ch, kk = _locate(chunks[side], k)
    ox, maybe = ch.op, ch.maybe
    oy, pr = next(((oy, pr) for oy, pr in maybe if X.eval_arrow(pr, v).to_pylist()[0]), maybe[0])
    wit = {"kind": "net-effect", "ops": [ox.describe(), oy.describe()],
           "example_row": _jsonable_row(v.to_pylist()[0]),
           "H_A": h["A"], "H_C": h["C"],
           # (A;C)(v) - (C;A)(v) = H_C(v) - H_A(v): the base contributions cancel (Theorem 4.2)
           "order_difference": h["C"] - h["A"],
           "inserted_by": dict(ch.prov(kk), side=side),
           "deleted_by": {"side": other, "op": oy.op_id, "predicate": X.describe(pr)},
           "rows_A_matching_P_C": nA, "rows_C_matching_P_A": nC}
    if ox.kind == "UPSERT" and oy.kind == "UPSERT":
        wit["key"] = v.column(ox.key_col).to_pylist()[0]
    return wit


def check_logical(lin: Lineage, cfg: RuleConfig, table_schema, arrow_schema: pa.Schema,
                  stats: C.IOStats | None, hits: Counter) -> tuple[Decision | None, pa.Table | None]:
    """Return (conflict Decision or None, rows I_A|P_C when the exact-equality case applies).

    None means the two sides' operation sequences commute.

    Net-effect form (cfg.net_effect): side S's sequence equals one operation with predicate
    P_S = OR of its predicates and surviving inserts I_S = sum_i I_i|~(P_{i+1} or ...).  The
    sequences commute iff I_A|P_C = I_C|P_A as multisets (state-independent), which is decided
    from column bounds and bounded reads of the inserted rows only: insert-role files, and the
    added-minus-copied rows of mixed snapshots (lineage.OpRecord.insert_sources).  With
    net_effect=False the check is pairwise and only sufficient (any inserted row of one side matching
    a predicate of the other side is a conflict).  cfg.directions="rebase" checks only the target's
    inserts against the source's predicates (the condition for the designated order (A;C))."""
    for a in lin.ops["A"]:
        for c in lin.ops["C"]:
            hits["pair:" + pair_rule(a, c)] += 1
    names = arrow_schema.names
    with _phase(stats, "recovery"):
        bad = _check_inserted_counts(lin, cfg, names, stats, hits)
    if bad is not None:
        return bad, None
    preds = {s: [_pred_for(o, cfg) for o in lin.ops[s]] for s in ("A", "C")}
    rows_of: dict[str, list[pa.Table]] = {"A": [], "C": []}
    chunks: dict[str, list[_Chunk]] = {"A": [], "C": []}
    for xs, ys in _directions(cfg):
        p_y = [(oy, pr) for oy, pr in zip(lin.ops[ys], preds[ys]) if pr is None or not X.is_false(pr)]
        if not p_y:
            continue
        for i, ox in enumerate(lin.ops[xs]):
            if not ox.has_inserts(cfg.discovery):
                continue
            unknown = [oy for oy, pr in p_y if pr is None]
            if unknown:
                return _conflict("logical", "R7-unknown-predicate",
                                 {"ops": [ox.describe(), unknown[0].describe()]}, hits), None
            relevant = []
            for oy, pr in p_y:
                if (ox.kind == "UPSERT" and oy.kind == "UPSERT" and ox.recorded and oy.recorded
                        and cfg.mode == "recorded"):
                    hits["R4:key-check"] += 1
                    if not set(ox.keys or []) & set(oy.keys or []):
                        continue
                relevant.append((oy, pr))
            if not relevant:
                continue
            evs = [(oy, pr, _InclusiveMetricsEvaluator(table_schema, X.to_iceberg(pr, arrow_schema)))
                   for oy, pr in relevant]
            later = preds[xs][i + 1:]
            for kind, src in ox.insert_sources(cfg.discovery):
                if kind == "file":
                    f = src
                    maybe = [(oy, pr) for oy, pr, ev in evs if ev.eval(f.data_file)]
                    if not maybe:
                        hits["bounds:cannot-match"] += 1
                        continue
                    if not cfg.bounded_reads:
                        rule = pair_rule(ox, maybe[0][0])
                        return _conflict("ablation", rule + ":bounds",
                                         {"ops": [ox.describe(), maybe[0][0].describe()],
                                          "file": f.path.rsplit("/", 1)[-1],
                                          "reason": "column bounds inconclusive; bounded reads disabled"}, hits), None
                    rows = C.read_data_file(f, stats).select(names)
                    hits["bounded-read"] += 1
                    n = rows.num_rows
                    chunk = _Chunk(ox, maybe, np.full(n, f.snapshot_id or -1, np.int64),
                                   np.full(n, f.path, dtype=object), np.arange(n, dtype=np.int64))
                    mask = _eval_any([pr for _, pr in maybe], rows)
                else:
                    with _phase(stats, "recovery"):
                        res = _mixed_inserts(lin, ox, src, evs, cfg, names, stats, hits)
                    if res is None:
                        continue
                    if isinstance(res, Decision):
                        return res, None
                    rows, chunk = res
                    maybe = chunk.maybe
                    mask = np.ones(rows.num_rows, bool)
                if not mask.any():
                    hits["bounded-read:clear"] += 1
                    continue
                if cfg.net_effect:
                    if all(pr is not None for pr in later):
                        for pr in later:
                            if not X.is_false(pr):
                                mask &= ~X.eval_arrow(pr, rows).to_numpy(zero_copy_only=False)
                    if not mask.any():
                        hits["net:masked-by-later-op"] += 1
                        continue
                hit_rows = rows.filter(pa.array(mask))
                hit_chunk = chunk.take(mask)
                if not cfg.net_effect:
                    first = hit_rows.slice(0, 1)
                    oy, pr = next((oy, pr) for oy, pr in maybe if X.eval_arrow(pr, first).to_pylist()[0])
                    return _conflict("logical", pair_rule(ox, oy),
                                     {"kind": "pairwise", "ops": [ox.describe(), oy.describe()],
                                      "example_row": _jsonable_row(first.to_pylist()[0]),
                                      "inserted_by": dict(hit_chunk.prov(0), side=xs),
                                      "deleted_by": {"side": ys, "op": oy.op_id, "predicate": X.describe(pr)},
                                      "matching_rows": int(mask.sum())}, hits), None
                rows_of[xs].append(hit_rows)
                chunks[xs].append(hit_chunk)
    if not rows_of["A"] and not rows_of["C"]:
        return None, None
    # exact condition: I_A|P_C == I_C|P_A as multisets
    tabs = {s: (pa.concat_tables(rows_of[s]) if rows_of[s] else arrow_schema.empty_table()) for s in ("A", "C")}
    if tabs["A"].num_rows == tabs["C"].num_rows and tabs["A"].num_rows:
        li, _ = match_rows(tabs["A"], tabs["C"])
        if li.size == tabs["A"].num_rows:
            hits["net:exact-equal"] += 1
            return None, tabs["A"]
    wit = _net_witness(tabs, chunks)
    side = wit["inserted_by"]["side"]
    ox = next(o for o in lin.ops[side] if o.op_id == wit["inserted_by"]["op"])
    oy = next(o for o in lin.ops["C" if side == "A" else "A"] if o.op_id == wit["deleted_by"]["op"])
    return _conflict("logical", pair_rule(ox, oy), wit, hits), None


# ============================================================================ physical composition

def _fill_value(typ: pa.DataType):
    if pa.types.is_integer(typ):
        return pa.scalar(0, typ)
    if pa.types.is_floating(typ):
        return pa.scalar(0.0, typ)
    if pa.types.is_boolean(typ):
        return pa.scalar(False, typ)
    if pa.types.is_string(typ) or pa.types.is_large_string(typ):
        return pa.scalar("", typ)
    if pa.types.is_temporal(typ):
        return pa.scalar(0, pa.int64()).cast(typ) if not pa.types.is_date32(typ) else pa.scalar(0, pa.int32()).cast(typ)
    return None


def _row_hashes(t: pa.Table, nullable: set[str] | None = None) -> np.ndarray:
    """64-bit hash of every row.  Columns in `nullable` are hashed as (NULL flag, value with NULL
    replaced) so that both sides of a comparison use the same pandas dtypes; -0.0 hashes as 0.0.
    Hashes only bucket rows: match_rows verifies every pair and resolves collisions exactly."""
    nullable = nullable if nullable is not None else {n for n in t.schema.names if t.column(n).null_count}
    cols = {}
    for name in t.schema.names:
        col = t.column(name)
        if name in nullable:
            fill = _fill_value(col.type)
            if fill is None:
                col = pc.cast(col, pa.string())
                fill = pa.scalar("", pa.string())
            cols["__null_" + name] = pc.is_null(col)
            col = pc.fill_null(col, fill)
        cols[name] = col
    df = pa.table(cols).to_pandas(date_as_object=False)
    for c in df.columns:  # -0.0 and 0.0 are equal under SQL/Arrow comparison but hash differently
        if pd.api.types.is_float_dtype(df[c].dtype):
            df[c] = df[c] + 0.0
    return pd.util.hash_pandas_object(df, index=False).to_numpy()


def _canon_value(v):
    if v is None:
        return ("<null>",)
    if isinstance(v, float):
        if v != v:
            return ("<nan>",)
        if v == 0.0:
            return 0.0
    return v


def canon_rows(t: pa.Table) -> list[tuple]:
    """Exact, hashable row values: NULL equals NULL, NaN equals NaN, -0.0 equals 0.0."""
    cols = [t.column(n).to_pylist() for n in t.schema.names]
    return [tuple(_canon_value(c[i]) for c in cols) for i in range(t.num_rows)]


def _pairs_equal(lt: pa.Table, rt: pa.Table) -> np.ndarray:
    """Per aligned row pair: equal on every column under IS NOT DISTINCT FROM (and NaN = NaN)."""
    ok = np.ones(lt.num_rows, bool)
    for name in lt.schema.names:
        a, b = lt.column(name), rt.column(name)
        e = pc.or_(pc.fill_null(pc.equal(a, b), False), pc.and_(pc.is_null(a), pc.is_null(b)))
        if pa.types.is_floating(a.type):
            e = pc.or_(e, pc.fill_null(pc.and_(pc.is_nan(a), pc.is_nan(b)), False))
        ok &= e.to_numpy(zero_copy_only=False)
    return ok


def match_rows(left: pa.Table, right: pa.Table) -> tuple[np.ndarray, np.ndarray]:
    """Exact maximum multiset matching of identical rows (IS NOT DISTINCT FROM on every column).
    Returns index pairs (li, ri).  Rows are bucketed by a 64-bit hash; every candidate pair is
    verified, and any bucket that holds a collision is re-matched on canonical values, so the result
    is exact for every input (2026-10-05; the submitted version failed closed on collisions)."""
    if left.num_rows == 0 or right.num_rows == 0:
        return np.empty(0, np.int64), np.empty(0, np.int64)
    right = right.select(left.schema.names).cast(left.schema)
    nullable = {n for n in left.schema.names if left.column(n).null_count or right.column(n).null_count}
    h_left, h_right = _row_hashes(left, nullable), _row_hashes(right, nullable)
    hl = pd.DataFrame({"h": h_left, "i": np.arange(left.num_rows)})
    hr = pd.DataFrame({"h": h_right, "j": np.arange(right.num_rows)})
    hl["r"] = hl.groupby("h").cumcount()
    hr["r"] = hr.groupby("h").cumcount()
    m = hl.merge(hr, on=["h", "r"], how="inner")
    li = m["i"].to_numpy(np.int64)
    ri = m["j"].to_numpy(np.int64)
    if li.size:
        ok = _pairs_equal(left.take(pa.array(li)), right.take(pa.array(ri)))
        if not ok.all():
            bad = np.unique(h_left[li[~ok]])
            keep = ~np.isin(h_left[li], bad)
            li, ri = li[keep], ri[keep]
            lb = np.flatnonzero(np.isin(h_left, bad))
            rb = np.flatnonzero(np.isin(h_right, bad))
            pool: dict[tuple, list[int]] = defaultdict(list)
            for j, key in zip(rb.tolist(), canon_rows(right.take(pa.array(rb)))):
                pool[key].append(j)
            el, er = [], []
            for i, key in zip(lb.tolist(), canon_rows(left.take(pa.array(lb)))):
                if pool.get(key):
                    el.append(i)
                    er.append(pool[key].pop())
            li = np.concatenate([li, np.array(el, np.int64)])
            ri = np.concatenate([ri, np.array(er, np.int64)])
    return li, ri


@dataclass
class _Fate:
    removed: set[str]
    copies: dict[str, dict[int, tuple[str, int]]]  # base path -> {base pos -> (new path, new pos)}
    deleted: dict[str, np.ndarray]                 # base path -> base-live positions deleted by the side
    new_files: list[C.LiveFile]
    inserted: dict[str, np.ndarray]                # new path -> live positions holding inserted rows
    copy_positions: dict[str, np.ndarray]          # new path -> live positions holding copies


def _side_fate(lin: Lineage, side: str, p: tuple, base_files_p: dict[str, C.LiveFile], names: list[str],
               stats: C.IOStats | None) -> tuple[_Fate, int]:
    st = lin.states[side]
    d = lin.deltas[side]
    removed = sorted(x for x in d.removed_data if x in base_files_p)
    new_files = sorted([lf for lf in d.added_data.values() if lf.partition_key == p], key=lambda f: f.path)
    fate = _Fate(set(removed), {}, {}, new_files, {}, {})
    # base files still live at the side's head: newly deleted positions
    for path, bf in base_files_p.items():
        if path in fate.removed:
            continue
        head_f = st.files.get(path)
        if head_f is None:
            continue
        ds_head = st.deletes_for(head_f)
        ds_base = lin.base.deletes_for(bf)
        if {x.path for x in ds_head} == {x.path for x in ds_base}:
            continue
        now = C.read_deleted_positions(ds_head, path, stats)
        before = C.read_deleted_positions(ds_base, path, stats)
        newly = np.setdiff1d(now, before)
        if newly.size:
            fate.deleted[path] = newly
    if not removed:
        for g in new_files:
            _, pos = C.live_rows(st, g, stats, columns=[names[0]])
            fate.inserted[g.path] = pos
            fate.copy_positions[g.path] = np.empty(0, np.int64)
        return fate, 0
    # removed base rows (live in base) vs live rows of the side's new files in p
    lrows, lsrc = [], []
    for path in removed:
        rows, pos = C.live_rows(lin.base, base_files_p[path], stats)
        lrows.append(rows.select(names))
        lsrc.extend((path, int(i)) for i in pos)
    rrows, rsrc = [], []
    for g in new_files:
        rows, pos = C.live_rows(st, g, stats)
        rrows.append(rows.select(names))
        rsrc.extend((g.path, int(i)) for i in pos)
    left = pa.concat_tables(lrows) if lrows else None
    right = pa.concat_tables(rrows) if rrows else None
    if left is not None and right is not None:
        right = right.cast(left.schema)
        li, ri = match_rows(left, right)
    else:
        li, ri = np.empty(0, np.int64), np.empty(0, np.int64)
    matched_l = np.zeros(len(lsrc), bool)
    matched_r = np.zeros(len(rsrc), bool)
    matched_l[li] = True
    matched_r[ri] = True
    for a, b in zip(li.tolist(), ri.tolist()):
        bp, bi = lsrc[a]
        fate.copies.setdefault(bp, {})[bi] = rsrc[b]
    dele: dict[str, list[int]] = {}
    for k, (bp, bi) in enumerate(lsrc):
        if not matched_l[k]:
            dele.setdefault(bp, []).append(bi)
    for bp, lst in dele.items():
        fate.deleted[bp] = np.array(sorted(lst), np.int64)
    ins: dict[str, list[int]] = {g.path: [] for g in new_files}
    cop: dict[str, list[int]] = {g.path: [] for g in new_files}
    for k, (gp, gi) in enumerate(rsrc):
        (cop if matched_r[k] else ins)[gp].append(gi)
    fate.inserted = {k: np.array(v, np.int64) for k, v in ins.items()}
    fate.copy_positions = {k: np.array(v, np.int64) for k, v in cop.items()}
    return fate, int(li.size)


def _compose_partition(lin: Lineage, p: tuple, layout: str, names: list[str], plan: CompositionPlan,
                       stats: C.IOStats | None) -> dict[str, tuple[C.LiveFile, str]]:
    other = "C" if layout == "A" else "A"
    base_files_p = {path: f for path, f in lin.base.files.items() if f.content == "data" and f.partition_key == p}
    fl, ml = _side_fate(lin, layout, p, base_files_p, names, stats)
    fo, mo = _side_fate(lin, other, p, base_files_p, names, stats)
    plan.matched_rows += ml + mo
    st_l, st_o = lin.states[layout], lin.states[other]
    desired = {path: (lf, layout) for path, lf in st_l.files.items() if lf.partition_key == p}
    # base rows deleted by O, located in L's layout
    for bpath, positions in fo.deleted.items():
        l_del = set(fl.deleted.get(bpath, np.empty(0, np.int64)).tolist())
        if bpath in fl.removed:
            copies = fl.copies.get(bpath, {})
            per_target: dict[str, list[int]] = {}
            for i in positions.tolist():
                if i in l_del:
                    continue
                loc = copies.get(i)
                if loc is None:
                    continue  # L deleted it as well (unmatched in L)
                per_target.setdefault(loc[0], []).append(loc[1])
            for tpath, plist in per_target.items():
                plan.add_deletes(st_l.files[tpath].data_file, plist)
        else:
            keep = [i for i in positions.tolist() if i not in l_del]
            if keep:
                plan.add_deletes(st_l.files[bpath].data_file, keep)
    # O's new files: keep those holding inserted rows; delete their copy rows
    for g in fo.new_files:
        ins = fo.inserted.get(g.path, np.empty(0, np.int64))
        if ins.size == 0:
            continue
        desired[g.path] = (g, other)
        for dfile in st_o.deletes_for(g):
            desired[dfile.path] = (dfile, other)
        cps = fo.copy_positions.get(g.path, np.empty(0, np.int64))
        if cps.size:
            plan.add_deletes(g.data_file, cps.tolist())
    return desired


def plan_composition(lin: Lineage, cfg: RuleConfig, arrow_schema: pa.Schema, stats: C.IOStats | None,
                     hits: Counter) -> Decision:
    base = lin.base
    dA, dC = lin.deltas["A"], lin.deltas["C"]
    base_data = {p for p, f in base.files.items() if f.content == "data"}
    remA, remC = set(dA.removed_data), set(dC.removed_data)
    tgtA, tgtC = dA.deleted_targets() & base_data, dC.deleted_targets() & base_data
    contended = (remA & remC) | (remA & tgtC) | (remC & tgtA)
    union_files = (tgtA & tgtC) - contended
    if union_files:
        hits["R3:delete-file-union"] += len(union_files)
        if not cfg.delete_union:
            return _conflict("ablation", "R3:delete-union-disabled",
                             {"file": sorted(union_files)[0].rsplit("/", 1)[-1]}, hits)
    replaced = {s: {f.path for o in lin.ops[s] if o.kind.lstrip("?") == "REPLACE" for f in
                    (x for sn in o.snaps for x in sn.removed) if f.content == "data"} for s in ("A", "C")}
    if contended:
        via_replace = contended & (replaced["A"] | replaced["C"])
        other = contended - via_replace
        hits["contended-files"] += len(contended)
        if via_replace:
            hits["R6:compaction-rebase"] += len(via_replace)
            if not cfg.compaction_rebase:
                return _conflict("ablation", "R6:compaction-rebase-disabled",
                                 {"file": sorted(via_replace)[0].rsplit("/", 1)[-1]}, hits)
        if other:
            hits["R3:cow-to-position"] += len(other)
            if not cfg.delete_union:
                return _conflict("ablation", "R3:cow-conversion-disabled",
                                 {"file": sorted(other)[0].rsplit("/", 1)[-1]}, hits)
        if not cfg.bounded_reads:
            return _conflict("ablation", "bounded-reads-disabled",
                             {"file": sorted(contended)[0].rsplit("/", 1)[-1]}, hits)
    P = sorted({base.files[p].partition_key for p in contended}, key=repr)
    Pset = set(P)
    live_A = lin.states["A"].files
    desired: dict[str, tuple[C.LiveFile, str]] = {}
    for path, lf in live_A.items():
        if lf.partition_key not in Pset:
            desired[path] = (lf, "A")
    for path in list(dC.removed_data) + list(dC.removed_deletes):
        f = base.files[path]
        if f.partition_key not in Pset:
            desired.pop(path, None)
    for path, lf in list(dC.added_data.items()) + list(dC.added_deletes.items()):
        if lf.partition_key not in Pset:
            desired[path] = (lf, "C")
    plan = CompositionPlan(contended_partitions=P)
    names = arrow_schema.names
    for p in P:
        p_removed_by = {s: {x for x in lin.deltas[s].removed_data if base.files[x].partition_key == p}
                        for s in ("A", "C")}
        a_comp = bool(p_removed_by["A"] & replaced["A"])
        c_comp = bool(p_removed_by["C"] & replaced["C"])
        layout = "C" if (c_comp and not a_comp) else "A"
        plan.layouts[repr(p)] = layout
        try:
            desired.update(_compose_partition(lin, p, layout, names, plan, stats))
        except MatchError as e:
            return _conflict("physical", "match-failure", {"partition": repr(p), "error": str(e)}, hits)
    plan.final = {path: lf for path, (lf, _) in desired.items()}
    plan.remove = set(live_A) - set(desired)
    plan.carry = [lf for path, (lf, src) in sorted(desired.items()) if path not in live_A]
    # sanity: every new delete targets a file that stays live
    for tpath in plan.new_deletes:
        if tpath not in desired:
            return _conflict("physical", "dangling-delete", {"file": tpath.rsplit("/", 1)[-1]}, hits)
    return Decision("merge", None, None, None, plan, hits)


def reconcile_duplicates(lin: Lineage, plan: CompositionPlan, dup: pa.Table, table_schema,
                         arrow_schema: pa.Schema, stats: C.IOStats | None, hits: Counter) -> Decision | None:
    """Exact-equality case (I_A|P_C = I_C|P_A = M, non-empty).  Every row value v of M satisfies
    both predicates, so its logical count in the merge is count(M, v) (only one side's copies of
    the inserts survive), while the composition can contain more instances.  Count the live
    instances of each v in the planned merged state (bounded: files whose key bounds may hold v)
    and add position deletes for the excess.  Any instance may be deleted: rows are equal."""
    names = arrow_schema.names
    dup = dup.select(names)
    key = names[0]
    state = C.TableState(None, 0, files=dict(plan.final))
    key_vals = set(dup.column(key).to_pylist())
    pred = X.isin(key, sorted(key_vals, key=repr)) if None not in key_vals else dict(X.TRUE)
    ev = _InclusiveMetricsEvaluator(table_schema, X.to_iceberg(pred, arrow_schema))
    rows_l, src = [], []
    for lf in sorted((f for f in state.files.values() if f.content == "data"), key=lambda f: f.path):
        if not ev.eval(lf.data_file):
            continue
        rows, pos = C.live_rows(state, lf, stats)
        extra = plan.new_deletes.get(lf.path)
        if extra:
            keep = ~np.isin(pos, np.fromiter(extra[1], np.int64))
            rows, pos = rows.filter(pa.array(keep)), pos[keep]
        sel = X.eval_arrow(pred, rows).to_numpy(zero_copy_only=False)
        if sel.any():
            rows_l.append(rows.filter(pa.array(sel)).select(names))
            src.extend((lf.path, int(p)) for p in pos[sel])
    if not rows_l:
        return _conflict("physical", "duplicate-reconcile", {"reason": "no live instance found"}, hits)
    live = pa.concat_tables(rows_l).cast(dup.schema)
    li, di = match_rows(live, dup)
    if li.size != dup.num_rows:
        return _conflict("physical", "duplicate-reconcile", {"reason": "fewer live instances than expected"}, hits)
    kept = np.zeros(live.num_rows, bool)
    kept[li] = True
    # unmatched live rows that equal some row of `dup` are the excess instances (exact values)
    removed = 0
    rest = np.flatnonzero(~kept)
    if rest.size:
        dup_keys = set(canon_rows(dup))
        per_target: dict[str, list[int]] = {}
        for k, key in zip(rest.tolist(), canon_rows(live.take(pa.array(rest)))):
            if key in dup_keys:
                per_target.setdefault(src[k][0], []).append(src[k][1])
                removed += 1
        for tpath, plist in per_target.items():
            plan.add_deletes(state.files[tpath].data_file, plist)
    plan.duplicates_removed = removed
    hits["net:duplicates-removed"] += removed
    return None


def _contended(lin: Lineage) -> set[str]:
    dA, dC = lin.deltas["A"], lin.deltas["C"]
    base_data = {p for p, f in lin.base.files.items() if f.content == "data"}
    remA, remC = set(dA.removed_data), set(dC.removed_data)
    tgtA, tgtC = dA.deleted_targets() & base_data, dC.deleted_targets() & base_data
    return (remA & remC) | (remA & tgtC) | (remC & tgtA)


class _Reads:
    """Files a phase will read, with multiplicity (a file read twice counts twice)."""

    def __init__(self):
        self.bytes = 0
        self.files = 0
        self.rows = 0

    def add(self, f: C.LiveFile) -> None:
        self.bytes += int(f.size or 0)
        self.files += 1
        if f.content == "data":
            self.rows += int(f.record_count or 0)

    def as_dict(self) -> dict:
        return {"bytes": self.bytes, "files": self.files, "rows": self.rows}


def read_estimate(lin: Lineage, cfg: RuleConfig | None = None, table_schema=None,
                  arrow_schema: pa.Schema | None = None, stats: C.IOStats | None = None) -> dict:
    """Preflight from manifest metadata and column bounds only (no data read): the data and delete
    bytes each phase of decide() may read (module doc).  read_bytes is their sum; read_rows the data
    rows; compose_max_partition_rows the rows of the largest contended partition (both sides),
    which composition holds in memory at once (partitions are composed one at a time).  Without
    schemas only the composition phase is estimated (the 2026-10-05 composition_estimate)."""
    cfg = cfg or RuleConfig()
    ph = {k: _Reads() for k in ("recovery", "logical", "compose")}
    if table_schema is not None and arrow_schema is not None and cfg.mode == "recorded":
        # recovery: schema-v2 inserted-count validation reads every mixed part in full
        if cfg.discovery == "provenance":
            for side in ("A", "C"):
                for op in lin.ops[side]:
                    if not op.recorded or op.schema_version < 2 or op.inserted_records is None:
                        continue
                    roles = op.snapshot_roles()
                    if not any(r == "mixed" for _, r in roles):
                        continue
                    for ch, r in roles:
                        if r != "mixed":
                            continue
                        parent = lin.state_before(ch, stats)  # metadata only (cached for the decision)
                        for f in ch.added_data():
                            ph["recovery"].add(f)
                        for f in ch.removed_data():
                            pf = parent.files.get(f.path, f)
                            ph["recovery"].add(pf)
                            for d in parent.deletes_for(pf):
                                ph["recovery"].add(d)
        preds = {s_: [_pred_for(o, cfg) for o in lin.ops[s_]] for s_ in ("A", "C")}
        for xs, ys in _directions(cfg):
            p_y = [(oy, pr) for oy, pr in zip(lin.ops[ys], preds[ys]) if pr is not None and not X.is_false(pr)]
            if not p_y:
                continue
            for ox in lin.ops[xs]:
                if not ox.has_inserts(cfg.discovery):
                    continue
                relevant = []
                for oy, pr in p_y:
                    if (ox.kind == "UPSERT" and oy.kind == "UPSERT" and ox.recorded and oy.recorded
                            and not set(ox.keys or []) & set(oy.keys or [])):
                        continue
                    relevant.append((oy, pr))
                if not relevant:
                    continue
                evs = [_InclusiveMetricsEvaluator(table_schema, X.to_iceberg(pr, arrow_schema)) for _, pr in relevant]
                for kind, src in ox.insert_sources(cfg.discovery):
                    if kind == "file":
                        if any(ev.eval(src.data_file) for ev in evs):
                            ph["logical"].add(src)
                        continue
                    added = [f for f in src.added_data() if any(ev.eval(f.data_file) for ev in evs)]
                    if not added:
                        continue
                    q_evs = [ev for ev in evs if any(ev.eval(f.data_file) for f in added)]
                    parent = lin.state_before(src, stats)
                    for f in added:
                        ph["recovery"].add(f)
                    for f in src.removed_data():
                        if any(ev.eval(f.data_file) for ev in q_evs):
                            pf = parent.files.get(f.path, f)
                            ph["recovery"].add(pf)
                            for d in parent.deletes_for(pf):
                                ph["recovery"].add(d)
    # composition: per contended partition and per side, as _side_fate reads it
    contended = _contended(lin)
    parts = {lin.base.files[p].partition_key for p in contended}
    max_rows = 0
    for p in parts:
        base_files_p = {path: f for path, f in lin.base.files.items() if f.content == "data" and f.partition_key == p}
        part_rows = 0
        for side in ("A", "C"):
            st, d = lin.states[side], lin.deltas[side]
            removed = [x for x in d.removed_data if x in base_files_p]
            new_files = [lf for lf in d.added_data.values() if lf.partition_key == p]
            for path, bf in base_files_p.items():
                if path in removed or path not in st.files:
                    continue
                ds_head, ds_base = st.deletes_for(st.files[path]), lin.base.deletes_for(bf)
                if {x.path for x in ds_head} != {x.path for x in ds_base}:
                    for x in list(ds_head) + list(ds_base):
                        ph["compose"].add(x)
            for path in removed:
                ph["compose"].add(base_files_p[path])
                part_rows += int(base_files_p[path].record_count or 0)
                for x in lin.base.deletes_for(base_files_p[path]):
                    ph["compose"].add(x)
            for g in new_files:
                ph["compose"].add(g)
                part_rows += int(g.record_count or 0)
                for x in st.deletes_for(g):
                    ph["compose"].add(x)
        max_rows = max(max_rows, part_rows)
    tot = _Reads()
    for r in ph.values():
        tot.bytes, tot.files, tot.rows = tot.bytes + r.bytes, tot.files + r.files, tot.rows + r.rows
    return {"unit": "data+delete bytes read (upper bound under the same bounds evaluation)",
            "contended_partitions": len(parts), "contended_files": len(contended),
            "read_files": tot.files, "read_rows": tot.rows, "read_bytes": tot.bytes,
            "compose_max_partition_rows": max_rows,
            "phases": {k: v.as_dict() for k, v in ph.items()}}


def composition_estimate(lin: Lineage) -> dict:
    """Composition phase only (kept for callers of the 2026-10-05 interface)."""
    return read_estimate(lin)


def reconcile_estimate(lin: Lineage, dup: pa.Table, table_schema, arrow_schema: pa.Schema) -> dict:
    """Upper bound on the bytes duplicate reconciliation reads: data files live at either head whose key
    bounds may hold a duplicated value, and the delete files that reference them (metadata only)."""
    names = arrow_schema.names
    key = names[0]
    key_vals = set(dup.select(names).column(key).to_pylist())
    pred = X.isin(key, sorted(key_vals, key=repr)) if None not in key_vals else dict(X.TRUE)
    ev = _InclusiveMetricsEvaluator(table_schema, X.to_iceberg(pred, arrow_schema))
    r = _Reads()
    seen = set()
    for side in ("A", "C"):
        st = lin.states[side]
        for f in st.files.values():
            if f.content != "data" or f.path in seen or not ev.eval(f.data_file):
                continue
            seen.add(f.path)
            r.add(f)
            for d in list(lin.states["A"].deletes_for(f)) + list(lin.states["C"].deletes_for(f)):
                r.add(d)
    return r.as_dict()


def decide(lin: Lineage, cfg: RuleConfig, table_schema, arrow_schema: pa.Schema,
           stats: C.IOStats | None = None) -> Decision:
    hits: Counter = Counter()
    if lin.structural:
        return _conflict("structural", "R7", {"reasons": lin.structural}, hits)
    if lin.deltas["C"].is_empty() and not lin.ops["C"]:
        return Decision("merge", None, "noop", None, CompositionPlan(), hits)
    est = read_estimate(lin, cfg, table_schema, arrow_schema, stats)
    budget = cfg.match_budget_bytes
    spent = (lambda: stats.charged()) if stats is not None else (lambda: None)

    def over_budget(stage: str, remaining: int | None) -> Decision:
        # Minor revision 2026-10-08 (M2): an over-budget stop is an EARLY outcome (unsupported): it emits no
        # snapshot and certifies neither commutation nor conflict.  It reports where it stopped and the bytes
        # already charged, which never exceed the budget.
        dec = _conflict("unsupported", "over-budget", {
            "stage": stage, "estimate_bytes": est["read_bytes"], "budget_bytes": budget, "bytes_read": spent(),
            "remaining_estimate_bytes": remaining, "phases": est["phases"]}, hits)
        dec.estimate = est
        return dec

    if budget is not None and est["read_bytes"] > budget:  # stage "preflight": before any data is read
        return over_budget("preflight", est["read_bytes"])
    if budget is not None and stats is not None:
        stats.read_limit = budget  # runtime cap: no opening may push the charged bytes over the budget
    try:
        t0 = time.perf_counter()
        with _phase(stats, "logical"):
            d, dup = check_logical(lin, cfg, table_schema, arrow_schema, stats, hits)
        t_logical = round((time.perf_counter() - t0) * 1e3, 3)
        if d is not None:
            d.estimate, d.timing_ms = est, {"logical": t_logical}
            return d
        if dup is not None and dup.num_rows:
            est["phases"]["reconcile"] = reconcile_estimate(lin, dup, table_schema, arrow_schema)
            est["read_bytes"] += est["phases"]["reconcile"]["bytes"]
            est["read_files"] += est["phases"]["reconcile"]["files"]
            est["read_rows"] += est["phases"]["reconcile"]["rows"]
            if budget is not None and est["read_bytes"] > budget:
                # stage "reconcile": the duplicated values are known only after recovery and logical reads, so
                # this check runs after them and before composition; it stops with those bytes already charged
                dec = over_budget("reconcile", est["phases"]["compose"]["bytes"] + est["phases"]["reconcile"]["bytes"])
                dec.timing_ms = {"logical": t_logical}
                return dec
        t1 = time.perf_counter()
        with _phase(stats, "compose"):
            dec = plan_composition(lin, cfg, arrow_schema, stats, hits)
        t_compose = time.perf_counter()
        if dec.verdict == "merge" and dup is not None and dup.num_rows:
            with _phase(stats, "reconcile"):
                bad = reconcile_duplicates(lin, dec.plan, dup, table_schema, arrow_schema, stats, hits)
            if bad is not None:
                dec = bad
        dec.estimate = est
        dec.timing_ms = {"logical": t_logical, "compose": round((t_compose - t1) * 1e3, 3),
                         "reconcile": round((time.perf_counter() - t_compose) * 1e3, 3)}
        return dec
    except C.ReadBudgetExceeded:
        # stage "read-guard": a phase would have read more than its estimate predicted (never observed);
        # the opening was refused before the read
        return over_budget("read-guard", None)
    finally:
        if stats is not None:
            stats.read_limit = None
    t1 = time.perf_counter()
    dec = plan_composition(lin, cfg, arrow_schema, stats, hits)
    if dec.verdict == "merge" and dup is not None and dup.num_rows:
        bad = reconcile_duplicates(lin, dec.plan, dup, table_schema, arrow_schema, stats, hits)
        if bad is not None:
            dec = bad
    dec.estimate = est
    dec.timing_ms = {"logical": t_logical, "compose": round((time.perf_counter() - t1) * 1e3, 3)}
    return dec
