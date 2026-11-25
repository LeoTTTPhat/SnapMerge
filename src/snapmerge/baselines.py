"""Baseline merge policies (all merge `source` into `target`; the LCA is computed as for SnapMerge).

(a) replay        Partition-level replay per the Iceberg community proposal #13959 ("More Flexible
                  Branch Merge", closed not planned; linked proposal document verified 2026-10-02):
                  find the LCA, replay the source snapshots (LCA, source head] in order onto the target
                  with cherry-pick semantics (added files are re-added with new sequence numbers,
                  removed files must still be live) INTO ONE TRANSACTION -- every replayed snapshot is
                  staged with its own sequence number and the chain is published with a single catalog
                  commit guarded by the target head -- and detect conflicts at partition granularity:
                    append                      -> never conflicts
                    overwrite / delete          -> conflict if the target added data, removed data or
                                                   added delete files in any partition the snapshot touched
                    replace (compaction)        -> conflict if the target removed data or added delete
                                                   files in a touched partition
                  A snapshot that touched no partition (a delete matching nothing) is replayed as a
                  no-op.  Validation runs before any commit, so a conflict leaves the target unchanged.
(b) lakefs        lakeFS-style table-level policy: merge only if one side made no data change
                  (compaction counts as no data change); if both sides only compacted, keep the target.
(c) nessie_normal Nessie MERGE (NORMAL): conflict if the table changed on both branches, else take
                  the changed side.
    nessie_force  Nessie MERGE (FORCE): the source content wins (silent loss of target changes).
(d) fast_forward  only if the target head is the LCA (or the source made no change).
(e) cherry_pick   iterated Iceberg cherry-pick: append snapshots are re-added; replace-partitions
                  (dynamic) overwrites are allowed if the target did not modify those partitions since
                  the LCA; every other operation is unsupported -> conflict.  Iterated = one
                  cherry-pick call (one catalog commit) per picked snapshot.
(f) rowlevel      Decibel/Dolt-style row-level three-way merge by primary key in DuckDB: full read of
                  base, target and source; per key take the changed side; conflict if both changed it
                  differently; result written as a full overwrite.  The merged rows are streamed out
                  of DuckDB ordered by (partition source column, key) and written in fixed chunks of
                  ROWLEVEL_WRITE_CHUNK_ROWS rows (never materialised as one Arrow table); one commit
                  removes every live file and adds the new ones.  NOT a sound upper bound: per-key
                  merging ignores predicate semantics (see the differential tests).
"""
from __future__ import annotations

import os
import time

import pyarrow as pa

from pyiceberg.table import Table

from . import catalog as C
from .emit import merge_properties
from .lineage import Lineage, SnapChange, compute_lineage
from .merge import MergeOutcome


def _done(out: MergeOutcome, t0: float) -> MergeOutcome:
    out.latency_s = time.perf_counter() - t0
    return out


def _conflict(out: MergeOutcome, rule: str, witness: dict | None, t0: float, cls: str = "policy") -> MergeOutcome:
    out.verdict, out.conflict_class, out.rule, out.witness = "conflict", cls, rule, witness
    return _done(out, t0)


def _pointer(table: Table, target: str, lin: Lineage, snapshot_id: int, out: MergeOutcome, t0: float,
             note: str) -> MergeOutcome:
    if snapshot_id != lin.heads["A"]:
        table = C.set_branch(table, target, snapshot_id, lin.heads["A"])
    out.verdict, out.snapshot_id, out.table = "merged", snapshot_id, table
    out.details["resolution"] = note
    return _done(out, t0)


def _data_change(lin: Lineage, side: str) -> bool:
    """lakeFS sees table metadata only: any non-replace snapshot counts as a data change."""
    return any(s.operation != "replace" for s in lin.snaps[side])


def _touched(snaps: list[SnapChange]) -> dict[str, set]:
    out = {"added_data": set(), "removed_or_deletes": set()}
    for s in snaps:
        for f in s.added:
            (out["added_data"] if f.content == "data" else out["removed_or_deletes"]).add(f.partition_key)
        for f in s.removed:
            out["removed_or_deletes"].add(f.partition_key)
    return out


def _replay_commit(table: Table, target: str, lin: Lineage, snaps: list[SnapChange], policy: str,
                   out: MergeOutcome, single_transaction: bool = True) -> tuple[Table, int]:
    """Re-apply `snaps` on the target.  single_transaction=True stages every replayed snapshot in one
    transaction and publishes the chain with ONE catalog commit (#13959); False commits each
    snapshot separately (iterated cherry-pick)."""
    state = lin.states["A"]
    sid = lin.heads["A"]
    live = set(state.files)
    for s in snaps:  # validate file availability before committing anything
        missing = {f.path for f in s.removed} - live
        if missing:
            raise LookupError(sorted(missing)[0])
        live -= {f.path for f in s.removed}
        live |= {f.path for f in s.added}
    tx = C.stage_transaction(table) if single_transaction else None
    for i, s in enumerate(snaps):
        props = {"snapmerge.replayed-from": str(s.snapshot_id)}
        if i == len(snaps) - 1:
            props.update(merge_properties(lin, policy))
        table, sid = C.commit_delta(table, target, state, remove_paths={f.path for f in s.removed},
                                    add_files=[f.data_file for f in s.added], operation=s.operation,
                                    properties=props, stats=out.stats, tx=tx)
        state = C.load_state(table, sid, out.stats, metadata=tx.table_metadata if tx is not None else None)
    if tx is not None:
        table = C.commit_staged(tx)
    out.details["commits"] = 1 if single_transaction else len(snaps)
    out.details["replayed_snapshots"] = len(snaps)
    return table, sid


def replay(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="replay", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.structural:
        return _conflict(out, "structural", {"reasons": lin.structural}, t0, "structural")
    ta = _touched(lin.snaps["A"])
    todo = []
    for s in lin.snaps["C"]:
        if s.is_marker:
            continue
        parts = s.partitions()
        if s.operation == "append":
            bad = set()
        elif s.operation == "replace":
            bad = parts & ta["removed_or_deletes"]
        else:
            bad = parts & (ta["added_data"] | ta["removed_or_deletes"])
        if bad:
            return _conflict(out, f"partition-overlap:{s.operation}",
                             {"snapshot": s.snapshot_id, "partition": repr(sorted(bad, key=repr)[0]),
                              "op": s.props.get("snapmerge.op")}, t0)
        todo.append(s)
    if not todo:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "nothing to replay")
    try:
        table, sid = _replay_commit(table, target, lin, todo, "replay", out)
    except LookupError as e:
        return _conflict(out, "missing-file", {"file": str(e).rsplit("/", 1)[-1]}, t0)
    out.verdict, out.snapshot_id, out.table = "merged", sid, table
    return _done(out, t0)


def cherry_pick(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="cherry_pick", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.structural:
        return _conflict(out, "structural", {"reasons": lin.structural}, t0, "structural")
    ta = _touched(lin.snaps["A"])
    a_parts = ta["added_data"] | ta["removed_or_deletes"]
    todo = []
    for op in lin.ops["C"]:
        if op.dynamic and op.kind.lstrip("?") == "OVERWRITE":
            parts = op.partitions()
            if parts & a_parts:
                return _conflict(out, "replace-partitions-changed",
                                 {"op": op.describe(), "partition": repr(sorted(parts & a_parts, key=repr)[0])}, t0)
            todo.extend(s for s in op.snaps if not s.is_marker)
            continue
        for s in op.snaps:
            if s.operation != "append":
                return _conflict(out, f"unsupported:{s.operation}", {"op": op.describe()}, t0)
            if not s.is_marker:
                todo.append(s)
    if not todo:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "nothing to pick")
    try:
        table, sid = _replay_commit(table, target, lin, todo, "cherry_pick", out, single_transaction=False)
    except LookupError as e:
        return _conflict(out, "missing-file", {"file": str(e).rsplit("/", 1)[-1]}, t0)
    out.verdict, out.snapshot_id, out.table = "merged", sid, table
    return _done(out, t0)


def lakefs(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="lakefs", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    da, dc = _data_change(lin, "A"), _data_change(lin, "C")
    if not dc:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "source has no data change")
    if not da:
        return _pointer(table, target, lin, lin.heads["C"], out, t0, "target has no data change")
    return _conflict(out, "table-changed-on-both", None, t0)


def nessie_normal(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="nessie_normal", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.heads["C"] == lin.base_id:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "source unchanged")
    if lin.heads["A"] == lin.base_id:
        return _pointer(table, target, lin, lin.heads["C"], out, t0, "target unchanged")
    return _conflict(out, "content-changed-on-both", None, t0)


def nessie_force(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="nessie_force", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    return _pointer(table, target, lin, lin.heads["C"], out, t0, "source wins")


def fast_forward(table: Table, target: str, source: str) -> MergeOutcome:
    out = MergeOutcome(policy="fast_forward", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    if lin.heads["A"] == lin.base_id:
        return _pointer(table, target, lin, lin.heads["C"], out, t0, "fast-forward")
    if lin.heads["C"] == lin.base_id:
        return _pointer(table, target, lin, lin.heads["A"], out, t0, "already up to date")
    return _conflict(out, "not-fast-forward", None, t0)


def _full_read_cost(state: C.TableState, stats: C.IOStats) -> None:
    for f in state.files.values():
        if f.content == "data":
            stats.data_bytes_read += f.size
            stats.data_files_read += 1
        else:
            stats.delete_bytes_read += f.size
            stats.delete_files_read += 1


ROWLEVEL_WRITE_CHUNK_ROWS = 1_000_000  # fixed (part of the baseline's output layout, never MEMGUARD-scaled)


def _rowlevel_order(table: Table, key: str) -> list[str]:
    """Partition source columns, then the key: chunks of the sorted stream cover few partitions."""
    schema = table.schema()
    cols = [schema.find_column_name(f.source_id) for f in table.spec().fields]
    return [c for c in cols if c] + [key]


def rowlevel(table: Table, target: str, source: str, con=None, key: str = "rid") -> MergeOutcome:
    out = MergeOutcome(policy="rowlevel", verdict="conflict")
    t0 = time.perf_counter()
    lin = compute_lineage(table, target, source, out.stats)
    own = con is None
    con = con or C.duckdb_connect()
    arrow_schema = table.schema().as_arrow()
    names = arrow_schema.names
    scale = float(os.environ.get("MEMGUARD_BATCH_SCALE", "1") or 1)
    read_batch = max(10_000, int(250_000 * scale))  # streaming granularity only (layout is fixed)
    new_files = []
    try:
        cols = ", ".join(f'"{c}"' for c in names)
        for tag, sid in (("b", lin.base_id), ("a", lin.heads["A"]), ("c", lin.heads["C"])):
            con.execute(f"CREATE OR REPLACE TEMP TABLE rl_{tag} AS SELECT {cols} FROM iceberg_scan("
                        f"'{C.metadata_location(table)}', snapshot_from_id={int(sid)})")
        for st in (lin.base, lin.states["A"], lin.states["C"]):
            _full_read_cost(st, out.stats)
        for tag in ("b", "a", "c"):
            dup = con.execute(f'SELECT "{key}" FROM rl_{tag} GROUP BY 1 HAVING count(*) > 1 LIMIT 1').fetchone()
            if dup is not None:
                return _conflict(out, "duplicate-key", {"key": dup[0], "version": tag}, t0)
        packs = {t: f"struct_pack({', '.join(f'{c} := {t}.{c}' for c in [chr(34) + n + chr(34) for n in names])})"
                 for t in ("b", "a", "c")}
        con.execute(f"""CREATE OR REPLACE TEMP TABLE rl_m AS
            WITH k AS (SELECT "{key}" FROM rl_b UNION SELECT "{key}" FROM rl_a UNION SELECT "{key}" FROM rl_c),
            j AS (SELECT k."{key}" AS k,
                         CASE WHEN b."{key}" IS NULL THEN NULL ELSE {packs['b']} END AS rb,
                         CASE WHEN a."{key}" IS NULL THEN NULL ELSE {packs['a']} END AS ra,
                         CASE WHEN c."{key}" IS NULL THEN NULL ELSE {packs['c']} END AS rc
                  FROM k LEFT JOIN rl_b b USING ("{key}") LEFT JOIN rl_a a USING ("{key}")
                         LEFT JOIN rl_c c USING ("{key}"))
            SELECT k, rb, ra, rc,
                   (ra IS DISTINCT FROM rb AND rc IS DISTINCT FROM rb AND ra IS DISTINCT FROM rc) AS conflict,
                   CASE WHEN ra IS NOT DISTINCT FROM rb THEN rc ELSE ra END AS r
            FROM j""")
        for tag in ("b", "a", "c"):  # free memory before the write
            con.execute(f"DROP TABLE IF EXISTS rl_{tag}")
        bad = con.execute("SELECT k FROM rl_m WHERE conflict ORDER BY k LIMIT 1").fetchone()
        if bad is not None:
            n = con.execute("SELECT count(*) FROM rl_m WHERE conflict").fetchone()[0]
            return _conflict(out, "row-conflict", {"key": bad[0], "conflicting_keys": n}, t0)
        order = ", ".join(f'"{c}"' for c in _rowlevel_order(table, key))
        reader = con.execute(f"SELECT {cols} FROM (SELECT unnest(r) FROM rl_m WHERE r IS NOT NULL) "
                             f"ORDER BY {order}").to_arrow_reader(read_batch)
        buf: list[pa.RecordBatch] = []
        n_buf = 0
        chunk = ROWLEVEL_WRITE_CHUNK_ROWS

        def flush(t: pa.Table) -> None:
            new_files.extend(C.write_data_files(table, t.select(names).cast(arrow_schema), out.stats))

        for batch in reader:
            buf.append(batch)
            n_buf += batch.num_rows
            while n_buf >= chunk:
                t = pa.Table.from_batches(buf)
                flush(t.slice(0, chunk))
                rest = t.slice(chunk)
                buf, n_buf = rest.to_batches(), rest.num_rows
        if n_buf:
            flush(pa.Table.from_batches(buf))
        del buf
    finally:
        for tag in ("b", "a", "c", "m"):
            try:
                con.execute(f"DROP TABLE IF EXISTS rl_{tag}")
            except Exception:
                pass
        if own:
            con.close()
    state = lin.states["A"]
    props = merge_properties(lin, "rowlevel")
    props["snapmerge.rowlevel-write-chunk-rows"] = str(ROWLEVEL_WRITE_CHUNK_ROWS)
    table, sid = C.commit_delta(table, target, state, remove_paths=set(state.files), add_files=new_files,
                                operation="overwrite", properties=props, stats=out.stats)
    out.details["new_data_files"] = len(new_files)
    out.verdict, out.snapshot_id, out.table = "merged", sid, table
    return _done(out, t0)


POLICIES = {
    "replay": replay,
    "lakefs": lakefs,
    "nessie_normal": nessie_normal,
    "nessie_force": nessie_force,
    "fast_forward": fast_forward,
    "cherry_pick": cherry_pick,
    "rowlevel": rowlevel,
}
