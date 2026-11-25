"""Provenance-aware writer check (harness; 2026-10-05, review R2 MC2 / F7).

The value-level writer check (oracle.check_snapshot of a branch head against the serial application
of that side's operations) tests A1's extensional part only: deleting a base row and re-inserting an
identical one leaves the same multiset.  This module checks the PHYSICAL obligations of A2 for every
operation, snapshot by snapshot, against the generator's intended operation o = (P_o, I_o) -- not
against the recorded metadata, so a recording error is caught too.  For each snapshot s of o, with
input state S and output state S':
  (a) every position newly deleted (by a position delete file) on a data file live in S and S' holds
      a row satisfying P_o (P_o = predicate IS TRUE); no deleted position is resurrected;
  (b) a delete file is dropped only together with the data file it references;
  (c) the live rows of every data file s removes that do not satisfy P_o reappear EXACTLY ONCE in
      data files that s itself adds, in the same partition (survivor copies, per snapshot -- the
      premise of SnapMerge's mixed-snapshot difference);
  (d) the remaining rows of s's added files are s's inserts I_s; a snapshot declared `delete` or
      `replace` (schema v2) has none, one declared `insert` removes no data file.
For the whole operation: (e) the union of the I_s equals I_o as a multiset, (f) no instance live
before o that satisfies P_o is live in place after it, and (g) (re-review 2026-10-07: A2 is a deletion
phase followed by an insertion phase) no snapshot of o deletes a row in, or removes, a data file that an
earlier snapshot of o added -- o never deletes rows it inserted.
The module imports only catalog and expr (no lineage, rules, merge or baselines code) and matches
rows with its own canonical-value Counter, independent of rules.match_rows.
"""
from __future__ import annotations

from collections import Counter, defaultdict

import numpy as np
import pyarrow as pa

from . import catalog as C
from . import expr as X


def _canon(v):
    if v is None:
        return ("<null>",)
    if isinstance(v, float):
        if v != v:
            return ("<nan>",)
        if v == 0.0:
            return 0.0
    return v


def _keys(t: pa.Table) -> list[tuple]:
    cols = [t.column(n).to_pylist() for n in t.schema.names]
    return [tuple(_canon(c[i]) for c in cols) for i in range(t.num_rows)]


def _chain(table, before: int | None, after: int | None) -> list[int]:
    out, sid = [], after
    while sid is not None and sid != before:
        out.append(sid)
        snap = table.metadata.snapshot_by_id(sid)
        sid = snap.parent_snapshot_id if snap else None
    return list(reversed(out))


def _deleted(state: C.TableState, f: C.LiveFile) -> set[int]:
    return set(C.read_deleted_positions(state.deletes_for(f), f.path).tolist())


def _rows(state: C.TableState, f: C.LiveFile, names: list[str]) -> tuple[pa.Table, np.ndarray]:
    rows, pos = C.live_rows(state, f)
    return rows.select(names), pos


def check_op(table, before: int | None, after: int | None, op, names: list[str]) -> dict:
    """Check one logical operation (writer.LogicalOp) whose snapshots lie between `before` and
    `after` on one branch.  Returns {"ok", "snapshots", "roles", "n_violations", "violations"}."""
    snaps = _chain(table, before, after)
    states = [C.load_state(table, before)] + [C.load_state(table, s) for s in snaps]
    pred = op.delete_pred()
    has_pred = pred is not None and not X.is_false(pred)

    def sat(rows: pa.Table) -> np.ndarray:
        if not has_pred or rows.num_rows == 0:
            return np.zeros(rows.num_rows, bool)
        return X.eval_arrow(pred, rows).to_numpy(zero_copy_only=False)

    bad: list[str] = []
    inserted: Counter = Counter()
    roles = []
    op_added: set[str] = set()
    for j, sid in enumerate(snaps):
        S, S2 = states[j], states[j + 1]
        props = C.summary_props(table.metadata.snapshot_by_id(sid))
        role = props.get("snapmerge.part")
        roles.append(role)
        d1 = {p: f for p, f in S.files.items() if f.content == "data"}
        d2 = {p: f for p, f in S2.files.items() if f.content == "data"}
        removed = [d1[p] for p in sorted(d1) if p not in d2]
        added = [d2[p] for p in sorted(d2) if p not in d1]
        for p in sorted(set(d1) & set(d2)):                                   # (a)
            was, now = _deleted(S, d1[p]), _deleted(S2, d2[p])
            if was - now:
                bad.append(f"s{j}: deleted positions resurrected in {p.rsplit('/', 1)[-1]}")
            newly = sorted(now - was)
            if newly and p in op_added:                                       # (g)
                bad.append(f"s{j}: deletes rows the operation itself inserted in {p.rsplit('/', 1)[-1]}")
            if newly:
                rows = C.read_data_file(d2[p]).select(names).take(pa.array(newly, pa.int64()))
                if not sat(rows).all():
                    bad.append(f"s{j}: position delete of a row not satisfying the predicate")
        for p, f in S.files.items():                                          # (b)
            if f.content != "data" and p not in S2.files and f.ref_path is not None and f.ref_path in d2:
                bad.append(f"s{j}: delete file dropped while its data file stays live")
        surv: dict = defaultdict(Counter)                                     # (c)
        for f in removed:
            rows, _ = _rows(S, f, names)
            keep = ~sat(rows)
            for k in _keys(rows.filter(pa.array(keep))):
                surv[f.partition_key][k] += 1
        new: dict = defaultdict(Counter)
        for g in added:
            rows, _ = _rows(S2, g, names)
            for k in _keys(rows):
                new[g.partition_key][k] += 1
        ins_s: Counter = Counter()
        for part in set(surv) | set(new):
            missing = surv[part] - new[part]
            if missing:
                bad.append(f"s{j}: {sum(missing.values())} survivor row(s) not copied by the same snapshot")
            ins_s += new[part] - surv[part]
        if role in ("delete", "replace") and ins_s:                          # (d)
            bad.append(f"s{j}: snapshot declared {role} inserts {sum(ins_s.values())} row(s)")
        if role == "insert" and removed:
            bad.append(f"s{j}: snapshot declared insert removes data files")
        if any(f.path in op_added for f in removed):                          # (g)
            bad.append(f"s{j}: removes a data file the operation itself added")
        op_added |= {g.path for g in added}
        inserted += ins_s
    exp = op.insert_rows()
    want = Counter(_keys(exp.select(names))) if exp is not None and exp.num_rows else Counter()
    if inserted != want:                                                      # (e)
        bad.append(f"inserted rows differ from the operation's rows (+{sum((inserted - want).values())} "
                   f"-{sum((want - inserted).values())})")
    if has_pred and snaps:                                                    # (f)
        S0, Sk = states[0], states[-1]
        for p, f in S0.files.items():
            if f.content == "data" and p in Sk.files:
                rows, _ = _rows(Sk, Sk.files[p], names)
                if sat(rows).any():
                    bad.append(f"matching row still live in place in {p.rsplit('/', 1)[-1]}")
                    break
    return {"ok": not bad, "snapshots": len(snaps), "roles": roles, "n_violations": len(bad), "violations": bad[:5]}
