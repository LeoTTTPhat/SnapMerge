"""Information-matched controls (2026-10-05, review R1 MC1 / R3 MC1 / G2).

Every policy here receives exactly SnapMerge's information: the recorded operations (kind, predicate,
upsert keys) and the same insertion provenance (lineage.OpRecord.insert_sources), and it may read
inserted files exactly as SnapMerge's decision does (column bounds, then bounded reads).  They differ
from SnapMerge in what they decide and what they can emit:

  replay_intent        LOGICAL: pairwise validation in both directions -- conflict if any row one
                       side inserted satisfies a recorded predicate of the other side (no masking,
                       no exact-equality case).  PHYSICAL: replay of the source's snapshots onto the
                       target (cherry-pick semantics, one transaction, one guarded catalog commit).
  replay_intent_exact  LOGICAL: SnapMerge's exact net-effect criterion.  PHYSICAL: replay; the
                       exact-equality case needs duplicate reconciliation, which replay cannot emit,
                       so it is a physical conflict.
  rebase_occ           LOGICAL: one direction only -- the source's predicates against the target's
                       inserts, as Iceberg's OverwriteFiles conflictDetectionFilter validates a
                       rebased commit (here with exact bounded reads; Iceberg itself uses bounds
                       only).  PHYSICAL: replay.  Its contract is the designated order (A;C), not
                       order independence; the analysis scores it against that contract.

Physical replay can express only what re-adding the source's files expresses.  It reports a
physical conflict (never an unsound merge) when
  * a data file the source removed is no longer live on the target (the target rewrote or
    compacted it: replaying the removal would fail or drop the target's copies),
  * a source delete file references a data file that is not live on the target (it would dangle),
  * a target delete file references a data file the source removed (replaying the source's
    rewrite would resurrect rows the target deleted),
  * the exact-equality case is reached (two copies of identical inserts; replay cannot reconcile).
These are exactly the compositions SnapMerge's emitter performs (delete-file union across a
rewrite, copy-on-write-to-position conversion, compaction rebase, duplicate reconciliation).  With
snapmerge[no_net_effect] (pairwise + composition) and snapmerge (exact + composition) the four
policies form a 2x2 {pairwise, exact} x {replay, composition} on the same information.

recompute            Contract-matched reference: materialise rows(B) in full, apply both recorded
                     orders (predicates and the provenance-derived inserted rows), and if they agree
                     rewrite the whole table with the result.  Sound and complete under A1-A2 but
                     reads and writes everything (cost reference, not a competitor).
"""
from __future__ import annotations

import os
import time
from collections import Counter

import pyarrow as pa

from pyiceberg.table import Table

from . import catalog as C
from . import expr as X
from .baselines import _conflict, _done, _pointer, _replay_commit, ROWLEVEL_WRITE_CHUNK_ROWS, _rowlevel_order
from .emit import merge_properties
from .lineage import Lineage, compute_lineage
from .merge import MergeOutcome
from .rules import RuleConfig, check_logical, Decision


def _physical_replay_conflict(lin: Lineage) -> dict | None:
    """What physical replay cannot express (module docstring)."""
    tgt_live = set(lin.states["A"].files)
    for s in lin.snaps["C"]:
        for f in s.removed:
            if f.path in lin.base.files and f.path not in tgt_live:
                return {"reason": "source removed a file the target no longer has", "file": f.path.rsplit("/", 1)[-1]}
        for f in s.added:
            if f.content == "pos" and f.ref_path is not None and f.ref_path in lin.base.files \
                    and f.ref_path not in tgt_live:
                return {"reason": "source delete file references a file the target removed",
                        "file": f.ref_path.rsplit("/", 1)[-1]}
    src_removed = set(lin.deltas["C"].removed_data)
    for d in lin.deltas["A"].added_deletes.values():
        if d.ref_path is not None and d.ref_path in src_removed:
            return {"reason": "target delete file references a file the source removed",
                    "file": d.ref_path.rsplit("/", 1)[-1]}
    return None


def _intent_replay(table: Table, target: str, source: str, policy: str, cfg: RuleConfig) -> MergeOutcome:
    out = MergeOutcome(policy=policy, verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.structural:
        return _conflict(out, "structural", {"reasons": lin.structural}, t0, "structural")
    hits: Counter = Counter()
    dec, dup = check_logical(lin, cfg, table.schema(), table.schema().as_arrow(), out.stats, hits)
    out.details["hits"] = dict(hits)
    if dec is not None:
        out.verdict, out.conflict_class, out.rule, out.witness = "conflict", dec.conflict_class, dec.rule, dec.witness
        return _done(out, t0)
    if dup is not None and dup.num_rows:
        return _conflict(out, "physical:duplicate-reconciliation", {"rows": dup.num_rows}, t0, "physical")
    bad = _physical_replay_conflict(lin)
    if bad is not None:
        return _conflict(out, "physical:replay", bad, t0, "physical")
    todo = [s for s in lin.snaps["C"] if not s.is_marker]
    if not todo:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "nothing to replay")
    try:
        table, sid = _replay_commit(table, target, lin, todo, policy, out)
    except LookupError as e:
        return _conflict(out, "physical:missing-file", {"file": str(e).rsplit("/", 1)[-1]}, t0, "physical")
    out.verdict, out.snapshot_id, out.table = "merged", sid, table
    return _done(out, t0)


def replay_intent(table: Table, target: str, source: str) -> MergeOutcome:
    return _intent_replay(table, target, source, "replay_intent", RuleConfig(net_effect=False))


def replay_intent_exact(table: Table, target: str, source: str) -> MergeOutcome:
    return _intent_replay(table, target, source, "replay_intent_exact", RuleConfig())


def rebase_occ(table: Table, target: str, source: str) -> MergeOutcome:
    return _intent_replay(table, target, source, "rebase_occ", RuleConfig(net_effect=False, directions="rebase"))


# ============================================================================ recompute reference

def _inserted_rows(lin: Lineage, side: str, names: list[str], stats: C.IOStats) -> list[pa.Table | None]:
    """I_o of every operation of a side, from the same provenance SnapMerge uses (full reads)."""
    from .rules import mixed_insert_rows

    out = []
    for op in lin.ops[side]:
        parts = []
        for kind, src in op.insert_sources():
            if kind == "file":
                parts.append(C.read_data_file(src, stats).select(names))
            else:
                ok, rows = mixed_insert_rows(lin, op, src, names, stats)
                if not ok:
                    raise LookupError("provenance: survivor not copied")
                if rows is not None:
                    parts.append(rows)
        out.append(pa.concat_tables(parts) if parts else None)
    return out


def recompute(table: Table, target: str, source: str, con=None) -> MergeOutcome:
    out = MergeOutcome(policy="recompute", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.structural:
        return _conflict(out, "structural", {"reasons": lin.structural}, t0, "structural")
    if any(not o.recorded for s in ("A", "C") for o in lin.ops[s]):
        return _conflict(out, "unknown-predicate", None, t0, "logical")
    own = con is None
    con = con or C.duckdb_connect()
    arrow_schema = table.schema().as_arrow()
    names = arrow_schema.names
    cols = ", ".join(f'"{c}"' for c in names)
    scale = float(os.environ.get("MEMGUARD_BATCH_SCALE", "1") or 1)
    read_batch = max(10_000, int(250_000 * scale))
    new_files = []
    try:
        try:
            ins = {s: _inserted_rows(lin, s, names, out.stats) for s in ("A", "C")}
        except LookupError as e:
            return _conflict(out, "provenance", {"error": str(e)}, t0, "structural")
        for f in lin.base.files.values():
            if f.content == "data":
                out.stats.data_bytes_read += f.size
                out.stats.data_files_read += 1
            else:
                out.stats.delete_bytes_read += f.size
                out.stats.delete_files_read += 1
        for order, seq in (("ac", ("A", "C")), ("ca", ("C", "A"))):
            con.execute(f"CREATE OR REPLACE TEMP TABLE rc_{order} AS SELECT {cols} FROM iceberg_scan("
                        f"'{C.metadata_location(table)}', snapshot_from_id={int(lin.base_id)})")
            for side in seq:
                for op, rows in zip(lin.ops[side], ins[side]):
                    pred = op.delete_pred()
                    if pred is not None and not X.is_false(pred):
                        con.execute(f"DELETE FROM rc_{order} WHERE {X.to_sql(pred, arrow_schema)}")
                    if rows is not None and rows.num_rows:
                        con.register("_rc_ins", rows.cast(arrow_schema))
                        con.execute(f"INSERT INTO rc_{order} SELECT {cols} FROM _rc_ins")
                        con.unregister("_rc_ins")
        a = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM rc_ac EXCEPT ALL SELECT {cols} FROM rc_ca)").fetchone()[0]
        b = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM rc_ca EXCEPT ALL SELECT {cols} FROM rc_ac)").fetchone()[0]
        if a or b:
            return _conflict(out, "orders-differ", {"only_ac": a, "only_ca": b}, t0, "logical")
        order = ", ".join(f'"{c}"' for c in _rowlevel_order(table, "rid" if "rid" in names else names[0]))
        reader = con.execute(f"SELECT {cols} FROM rc_ac ORDER BY {order}").to_arrow_reader(read_batch)
        buf, n_buf = [], 0
        for batch in reader:
            buf.append(batch)
            n_buf += batch.num_rows
            while n_buf >= ROWLEVEL_WRITE_CHUNK_ROWS:
                t = pa.Table.from_batches(buf)
                new_files.extend(C.write_data_files(table, t.slice(0, ROWLEVEL_WRITE_CHUNK_ROWS).cast(arrow_schema),
                                                    out.stats))
                rest = t.slice(ROWLEVEL_WRITE_CHUNK_ROWS)
                buf, n_buf = rest.to_batches(), rest.num_rows
        if n_buf:
            new_files.extend(C.write_data_files(table, pa.Table.from_batches(buf).cast(arrow_schema), out.stats))
    finally:
        for t in ("rc_ac", "rc_ca"):
            try:
                con.execute(f"DROP TABLE IF EXISTS {t}")
            except Exception:
                pass
        if own:
            con.close()
    state = lin.states["A"]
    table, sid = C.commit_delta(table, target, state, remove_paths=set(state.files), add_files=new_files,
                                operation="overwrite", properties=merge_properties(lin, "recompute"), stats=out.stats)
    out.details["new_data_files"] = len(new_files)
    out.verdict, out.snapshot_id, out.table = "merged", sid, table
    return _done(out, t0)


INTENT_POLICIES = {
    "replay_intent": replay_intent,
    "replay_intent_exact": replay_intent_exact,
    "rebase_occ": rebase_occ,
    "recompute": recompute,
}
