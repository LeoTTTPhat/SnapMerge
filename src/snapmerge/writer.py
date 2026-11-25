"""Logical-operation writer.

Every logical operation is committed to a branch and recorded in snapshot summary properties:
  snapmerge.op         APPEND | OVERWRITE | DELETE | UPSERT | REPLACE
  snapmerge.op-id      groups the 1..n snapshots one logical operation produced
  snapmerge.filter     serialized filter (OVERWRITE, DELETE; REPLACE file-selection filter)
  snapmerge.keys       JSON list of upsert keys; snapmerge.key-col names the key column
  snapmerge.mode       cow | mor
  snapmerge.dynamic    true for a partition-aligned (replace-partitions) overwrite
  snapmerge.replace-group  JSON list of data files replaced by a compaction

Copy-on-write operations use PyIceberg's public API (append, delete, overwrite,
dynamic_partition_overwrite).  Merge-on-read operations (position deletes, optionally with
inserted rows in the same snapshot) and REPLACE/compaction snapshots are hand-authored through
catalog.commit_delta.  A logical operation whose physical effect is empty (e.g. a delete that
matches no row) still commits a marker snapshot so the operation is recorded.

With record=False the writer behaves like an engine that records nothing beyond Iceberg's own
summary `operation` field (the file-level fallback of the plan).

Metadata schema v2 (2026-10-05, review F1/F10; schema_version=1 reproduces the submitted writer):
  snapmerge.schema-version  2
  snapmerge.part            insert | delete | mixed | replace | marker  (role of THIS snapshot)
  snapmerge.op-end          true on the last snapshot of the operation (completion marker)
  snapmerge.inserted-records  |I_o|, the number of rows the operation inserts
  snapmerge.filter-fields   JSON {column: Iceberg field id} binding the predicate to the schema
  snapmerge.semantics       fixed-rows: the operation deletes rows satisfying the predicate and inserts
                            the FIXED rows it wrote (an UPDATE x = x + 1 is recorded as its result rows,
                            not as a statement to re-execute); readers reject any other value
Copy-on-write operations through PyIceberg are committed as ONE transaction (delete part, then
insert part) so a branch never exposes half an operation; the delete part is handed
expr.is_true_form(predicate), whose PyIceberg semantics equal "predicate IS TRUE" on NULLs.

writer="single" emulates a foreign cooperating writer that commits a copy-on-write DELETE,
OVERWRITE or UPSERT as ONE snapshot: every data file with a matching live row is removed, and its
survivors are written together with the inserted rows of the same partition into new files
(part=mixed).  The review's counterexample is this shape.
"""
from __future__ import annotations

import json
import warnings
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pyarrow as pa

from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.table import Table

from . import catalog as C
from . import expr as X

KINDS = ("APPEND", "OVERWRITE", "DELETE", "UPSERT", "REPLACE")


@dataclass
class LogicalOp:
    kind: str
    filter: dict | None = None          # OVERWRITE / DELETE predicate; REPLACE file selection
    rows: pa.Table | None = None        # APPEND / OVERWRITE / UPSERT inserted rows
    keys: list | None = None            # UPSERT key set (= key values of rows)
    mode: str = "cow"                   # cow | mor (DELETE / OVERWRITE / UPSERT)
    dynamic: bool = False               # OVERWRITE via dynamic partition overwrite
    sort_by: str | None = None          # REPLACE: rewrite order (changes row positions)
    key_col: str = "rid"

    def __post_init__(self):
        if self.kind not in KINDS:
            raise ValueError(self.kind)

    # ---- logical semantics: op(X) = (X minus rows satisfying delete_pred) + insert_rows
    def delete_pred(self) -> dict:
        if self.kind in ("DELETE", "OVERWRITE"):
            return self.filter
        if self.kind == "UPSERT":
            return X.isin(self.key_col, self.keys)
        return dict(X.FALSE)

    def insert_rows(self) -> pa.Table | None:
        if self.kind in ("APPEND", "OVERWRITE", "UPSERT"):
            return self.rows
        return None

    def summary(self) -> dict:
        d: dict[str, Any] = {"kind": self.kind, "mode": self.mode}
        if self.filter is not None:
            d["filter"] = X.describe(self.filter)
        if self.rows is not None:
            d["rows"] = self.rows.num_rows
        if self.keys is not None:
            d["keys"] = len(self.keys)
        if self.dynamic:
            d["dynamic"] = True
        if self.sort_by:
            d["sort_by"] = self.sort_by
        return d


SCHEMA_VERSION = 2


def op_properties(op: LogicalOp, op_id: str, record: bool = True, *, schema_version: int = SCHEMA_VERSION,
                  part: str | None = None, op_end: bool = True, table: Table | None = None) -> dict[str, str]:
    if not record:
        return {}
    p = {"snapmerge.op": op.kind, "snapmerge.op-id": op_id, "snapmerge.mode": op.mode,
         "snapmerge.key-col": op.key_col}
    if op.filter is not None:
        p["snapmerge.filter"] = X.dumps(op.filter)
    if op.kind == "UPSERT":
        p["snapmerge.keys"] = json.dumps(sorted(op.keys))
    if op.dynamic:
        p["snapmerge.dynamic"] = "true"
    if schema_version >= 2:
        p["snapmerge.schema-version"] = str(schema_version)
        p["snapmerge.semantics"] = "fixed-rows"
        if part is not None:
            p["snapmerge.part"] = part
        p["snapmerge.op-end"] = "true" if op_end else "false"
        ins = op.insert_rows()
        p["snapmerge.inserted-records"] = str(0 if ins is None else ins.num_rows)
        cols = set(X.columns(op.filter)) if op.filter is not None and op.kind != "REPLACE" else set()
        if op.kind == "UPSERT":
            cols.add(op.key_col)
        if cols and table is not None:
            sch = table.schema()
            p["snapmerge.filter-fields"] = json.dumps({c: sch.find_field(c).field_id for c in sorted(cols)},
                                                      sort_keys=True)
    return p


def _with(props: dict, **kw) -> dict:
    """Copy of v2 props with part / op-end replaced (no-op for v1 or unrecorded props)."""
    if "snapmerge.schema-version" not in props:
        return props
    out = dict(props)
    if "part" in kw:
        out["snapmerge.part"] = kw["part"]
    if "op_end" in kw:
        out["snapmerge.op-end"] = "true" if kw["op_end"] else "false"
    return out


def _arrow_schema(table: Table) -> pa.Schema:
    return table.schema().as_arrow()


def _conform(table: Table, rows: pa.Table) -> pa.Table:
    return rows.select(_arrow_schema(table).names).cast(_arrow_schema(table))


def _marker(table: Table, branch: str, operation: str, props: dict, stats) -> Table:
    st = C.load_state(table, C.head_id(table, branch))
    props = _with(dict(props), part="marker", op_end=True)
    props["snapmerge.marker"] = "true"
    table, _ = C.commit_delta(table, branch, st, remove_paths=set(), add_files=[], operation=operation,
                              properties=props, stats=stats)
    return table


def _mor_row_delta(table: Table, branch: str, pred: dict, insert: pa.Table | None, props: dict,
                   stats: C.IOStats | None) -> Table:
    """Hand-authored merge-on-read row delta: file-scoped position deletes for rows matching
    `pred` plus (optionally) new data files, in ONE snapshot."""
    st = C.load_state(table, C.head_id(table, branch), stats)
    schema = _arrow_schema(table)
    ev = _InclusiveMetricsEvaluator(table.schema(), X.to_iceberg(pred, schema))
    added = []
    for lf in sorted(st.data_files(), key=lambda f: f.path):
        if not ev.eval(lf.data_file):
            continue
        rows, positions = C.live_rows(st, lf, stats)
        mask = X.eval_arrow(pred, rows).to_numpy(zero_copy_only=False)
        hit = positions[mask]
        if hit.size:
            added.append(C.write_position_delete_file(table, lf.data_file, hit.tolist(), stats))
    if insert is not None and insert.num_rows:
        added += C.write_data_files(table, _conform(table, insert), stats)
    has_data = any(d.content.value == 0 for d in added)
    operation = "overwrite" if has_data else "delete"
    props = _with(props, part="insert" if has_data else "delete", op_end=True)
    table, _ = C.commit_delta(table, branch, st, remove_paths=set(), add_files=added, operation=operation,
                              properties=props, stats=stats)
    return table


def _cow_single(table: Table, branch: str, pred: dict, insert: pa.Table | None, props: dict,
                stats: C.IOStats | None) -> Table:
    """Foreign-writer emulation: a copy-on-write delete/overwrite/upsert in ONE snapshot.  Files with
    a matching live row are removed (with the position deletes that referenced them); their
    survivors and the inserted rows are written together, partition by partition."""
    st = C.load_state(table, C.head_id(table, branch), stats)
    schema = _arrow_schema(table)
    ev = _InclusiveMetricsEvaluator(table.schema(), X.to_iceberg(pred, schema))
    removed, keep = set(), []
    for lf in sorted(st.data_files(), key=lambda f: f.path):
        if not ev.eval(lf.data_file):
            continue
        rows, _ = C.live_rows(st, lf, stats)
        mask = X.eval_arrow(pred, rows).to_numpy(zero_copy_only=False)
        if mask.any():
            removed.add(lf.path)
            if (~mask).any():
                keep.append(rows.filter(pa.array(~mask)))
    if insert is not None and insert.num_rows:
        keep.append(_conform(table, insert))
    if not removed and not (insert is not None and insert.num_rows):
        return table
    new_files = C.write_data_files(table, _conform(table, pa.concat_tables(keep)), stats) if keep else []
    drop = {d.path for d in st.delete_files() if d.ref_path is not None and d.ref_path in removed}
    has_ins = insert is not None and insert.num_rows > 0
    part = "mixed" if (removed and has_ins) else ("delete" if removed else "insert")
    props = _with(props, part=part, op_end=True)
    table, _ = C.commit_delta(table, branch, st, remove_paths=removed | drop, add_files=new_files,
                              operation="overwrite" if new_files else "delete", properties=props, stats=stats)
    return table


def _cow_tx(table: Table, branch: str, pred: dict | None, rows: pa.Table | None, props: dict,
            isolation=None) -> Table:
    """Copy-on-write delete part (PyIceberg, with the two-valued IS TRUE form of the predicate) and
    insert part (append) in ONE transaction, i.e. one catalog commit."""
    schema = _arrow_schema(table)
    has_rows = rows is not None and rows.num_rows > 0
    tx = table.transaction()
    if pred is not None and not X.is_false(pred):
        kw = {"_isolation_operation": isolation} if isolation is not None else {}
        tx.delete(X.to_iceberg(X.is_true_form(pred), schema), snapshot_properties=_with(props, part="delete",
                  op_end=not has_rows), branch=branch, **kw)
    if has_rows:
        tx.append(_conform(table, rows), snapshot_properties=_with(props, part="insert", op_end=True), branch=branch)
    tx.commit_transaction()
    return table


def _replace(table: Table, branch: str, op: LogicalOp, props: dict, stats: C.IOStats | None) -> Table:
    """Hand-authored compaction: rewrite all live rows of the selected data files (and drop the
    file-scoped position deletes that applied to them) in one REPLACE snapshot."""
    st = C.load_state(table, C.head_id(table, branch), stats)
    schema = _arrow_schema(table)
    sel = op.filter if op.filter is not None else dict(X.TRUE)
    ev = _InclusiveMetricsEvaluator(table.schema(), X.to_iceberg(sel, schema))
    group = sorted([lf for lf in st.data_files() if ev.eval(lf.data_file)], key=lambda f: f.path)
    if not group:
        return _marker(table, branch, "replace", props, stats)
    parts = [C.live_rows(st, lf, stats)[0] for lf in group]
    rows = pa.concat_tables(parts)
    if op.sort_by:
        rows = rows.sort_by([(op.sort_by, "ascending"), (op.key_col, "descending")])
    group_paths = {lf.path for lf in group}
    drop_deletes = {d.path for d in st.delete_files() if d.ref_path is not None and d.ref_path in group_paths}
    new_files = C.write_data_files(table, _conform(table, rows), stats)
    props = _with(dict(props), part="replace", op_end=True)
    if props:
        props["snapmerge.replace-group"] = json.dumps(sorted(group_paths))
    table, _ = C.commit_delta(table, branch, st, remove_paths=group_paths | drop_deletes, add_files=new_files,
                              operation="replace", properties=props, stats=stats)
    return table


def apply_op(table: Table, branch: str, op: LogicalOp, op_id: str, record: bool = True,
             stats: C.IOStats | None = None, *, writer: str = "split",
             schema_version: int = SCHEMA_VERSION) -> Table:
    """Commit one logical operation on `branch`; returns the refreshed table.

    writer="split" (default): PyIceberg copy-on-write (delete part + insert part in one transaction);
    writer="single": one hand-authored snapshot per copy-on-write operation (foreign-writer emulation).
    schema_version=1 writes the submitted version's properties (no roles, counts or markers) and uses
    table.overwrite / table.delete exactly as the submitted writer did."""
    from pyiceberg.table.snapshots import Operation

    props = op_properties(op, op_id, record, schema_version=schema_version, table=table)
    before = C.head_id(table, branch)
    schema = _arrow_schema(table)
    legacy = schema_version < 2
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        if op.kind == "APPEND":
            if op.rows is not None and op.rows.num_rows:
                table.append(_conform(table, op.rows), snapshot_properties=_with(props, part="insert", op_end=True),
                             branch=branch)
            marker_op = "append"
        elif op.kind == "DELETE":
            if op.mode == "mor":
                table = _mor_row_delta(table, branch, op.filter, None, props, stats)
            elif writer == "single":
                table = _cow_single(table, branch, op.filter, None, props, stats)
            elif legacy:
                table.delete(X.to_iceberg(op.filter, schema), snapshot_properties=props, branch=branch)
            else:
                table = _cow_tx(table, branch, op.filter, None, props)
            marker_op = "delete"
        elif op.kind == "OVERWRITE":
            if op.mode == "mor":
                table = _mor_row_delta(table, branch, op.filter, op.rows, props, stats)
            elif writer == "single":
                table = _cow_single(table, branch, op.filter, op.rows, props, stats)
            elif legacy:
                # Dynamic partition overwrite is expressed as an overwrite whose filter is the
                # partition-aligned predicate (PyIceberg 0.12.0's dynamic_partition_overwrite only
                # supports identity transforms); snapmerge.dynamic marks it as replace-partitions.
                table.overwrite(_conform(table, op.rows), overwrite_filter=X.to_iceberg(op.filter, schema),
                                snapshot_properties=props, branch=branch)
            else:
                table = _cow_tx(table, branch, op.filter, op.rows, props, isolation=Operation.OVERWRITE)
            marker_op = "overwrite"
        elif op.kind == "UPSERT":
            pred = op.delete_pred()
            if op.mode == "mor":
                table = _mor_row_delta(table, branch, pred, op.rows, props, stats)
            elif writer == "single":
                table = _cow_single(table, branch, pred, op.rows, props, stats)
            elif legacy:
                tx = table.transaction()
                if op.keys:
                    tx.delete(X.to_iceberg(pred, schema), snapshot_properties=props, branch=branch)
                if op.rows is not None and op.rows.num_rows:
                    tx.append(_conform(table, op.rows), snapshot_properties=props, branch=branch)
                tx.commit_transaction()
            else:
                table = _cow_tx(table, branch, pred if op.keys else None, op.rows, props)
            marker_op = "overwrite"
        elif op.kind == "REPLACE":
            table = _replace(table, branch, op, props, stats)
            marker_op = "replace"
        else:
            raise ValueError(op.kind)
    table = table.refresh()
    if C.head_id(table, branch) == before:
        table = _marker(table, branch, marker_op, props, stats)
    return table.refresh()


def write_base(table: Table, rows: pa.Table, chunks: int = 2, branch: str = "main",
               record: bool = True) -> Table:
    """Write the base table in `chunks` appends so each partition holds several data files."""
    rows = _conform(table, rows)
    n = rows.num_rows
    bounds = np.linspace(0, n, chunks + 1).astype(int)
    for i in range(chunks):
        part = rows.slice(bounds[i], bounds[i + 1] - bounds[i])
        if part.num_rows:
            props = {"snapmerge.op": "BASE", "snapmerge.op-id": f"base{i}"} if record else {}
            table.append(part, snapshot_properties=props, branch=branch)
    return table.refresh()
