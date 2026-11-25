"""Lineage: lowest common ancestor, per-side logical operations and physical deltas.

A side's operations are reconstructed from its snapshots (base, head]:
  * physical changes per snapshot come from manifest entries the snapshot itself wrote
    (status ADDED / DELETED with that snapshot id);
  * logical metadata comes from the recorded summary properties (writer.op_properties);
    snapshots without them become one operation each, typed by Iceberg's summary `operation`
    (append / delete / overwrite / replace) with an unknown delete predicate.
A side's net physical delta compares the live file sets of base and head.

Insertion provenance (2026-10-05, review F1).  Every snapshot of a recorded operation has a ROLE:
  insert   adds data files holding only inserted rows and removes no data file;
  delete   removes data files and adds at most survivor copies (a copy-on-write rewrite);
  mixed    removes data files and adds survivor copies AND inserted rows in the same files;
  replace  compaction;   marker  no physical change.
Schema-v2 writers declare the role (snapmerge.part); otherwise it is inferred from the snapshot's
shape: adds data only -> insert, removes and adds data -> mixed, removes only -> delete.  The
inserted rows of a mixed snapshot are its added rows minus the survivor copies, i.e. the live rows
of the files it removed that do not satisfy the operation's predicate (A2), as an exact multiset
difference (rules.py).  A declared `delete` part is trusted only together with the operation's
recorded inserted-row count, which validate_metadata() checks against the insert parts, so rows
hidden in a rewrite are detected.  validate_metadata() also rejects incomplete, contradictory and
mis-bound records with explicit reasons (structural conflicts prefixed "metadata:").

Re-review 2026-10-07.  (G1, physical identity) validate_identity() rejects content files whose
identity is not fresh: a data or delete file path added after the base on BOTH branches, or added by a
branch although it was live at the base or already added or removed earlier on that branch.  The
composition keys files by path, so an alias would otherwise be emitted once although the recorded
operations insert its rows twice.  (G1, phases) A recorded operation is a deletion phase followed by an
insertion phase (A2); validate_metadata() rejects a schema-v2 operation with a delete or mixed part
after an insert part, and one whose position deletes target a data file the same operation added.
"""
from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass, field

from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.table import Table

from . import catalog as C
from . import expr as X


@dataclass
class SnapChange:
    snapshot_id: int
    operation: str            # Iceberg summary operation
    props: dict
    added: list[C.LiveFile] = field(default_factory=list)    # data + delete files added
    removed: list[C.LiveFile] = field(default_factory=list)  # data + delete files removed

    @property
    def is_marker(self) -> bool:
        return not self.added and not self.removed

    def added_data(self) -> list[C.LiveFile]:
        return [f for f in self.added if f.content == "data"]

    def removed_data(self) -> list[C.LiveFile]:
        return [f for f in self.removed if f.content == "data"]

    def partitions(self) -> set[tuple]:
        return {f.partition_key for f in self.added} | {f.partition_key for f in self.removed}


@dataclass
class OpRecord:
    op_id: str
    side: str
    kind: str                  # APPEND OVERWRITE DELETE UPSERT REPLACE or ?APPEND ?DELETE ?OVERWRITE ?REPLACE
    recorded: bool
    filter: dict | None
    keys: list | None
    key_col: str
    mode: str
    dynamic: bool
    snaps: list[SnapChange] = field(default_factory=list)
    schema_version: int = 1
    inserted_records: int | None = None   # v2: |I_o| as recorded by the writer

    # -- physical roles ---------------------------------------------------------------
    def added_data(self) -> list[C.LiveFile]:
        return [f for s in self.snaps for f in s.added if f.content == "data"]

    def snapshot_roles(self) -> list[tuple[SnapChange, str]]:
        """(snapshot, role) with role in insert | delete | mixed | replace | marker (module doc)."""
        out = []
        for s in self.snaps:
            add, rem = bool(s.added_data()), bool(s.removed_data())
            declared = s.props.get("snapmerge.part") if self.schema_version >= 2 else None
            if self.kind.lstrip("?") == "REPLACE":
                role = "replace" if (add or rem) else "marker"
            elif not add:
                role = "delete" if (rem or s.added or s.removed) else "marker"
            elif declared in ("insert", "delete", "mixed"):
                role = declared
            else:
                role = "mixed" if rem else "insert"
            out.append((s, role))
        return out

    def insert_sources(self, discovery: str = "provenance") -> list[tuple[str, object]]:
        """Where this operation's inserted rows I_o are: ("file", LiveFile) for a data file all of
        whose rows are inserts, ("mixed", SnapChange) for a snapshot whose inserts must be separated
        from survivor copies by an exact multiset difference.

        discovery="append_rule" reproduces the submitted version's literal rule (copy-on-write
        inserts only from snapshots whose Iceberg operation is `append`), used only by the
        snapmerge[append_rule] variant that measures the review's counterexample class."""
        if self.kind.lstrip("?") in ("REPLACE", "DELETE"):
            return []
        if not self.recorded:
            return [("file", f) for f in self.added_data()]  # unknown provenance; R7 guards it
        if discovery == "append_rule":
            if self.mode == "mor":
                return [("file", f) for f in self.added_data()]
            return [("file", f) for s in self.snaps if s.operation == "append" for f in s.added_data()]
        out: list[tuple[str, object]] = []
        for s, role in self.snapshot_roles():
            if role == "insert":
                out += [("file", f) for f in s.added_data()]
            elif role == "mixed":
                out.append(("mixed", s))
        return out

    def insert_files(self) -> list[C.LiveFile]:
        """Data files consisting only of inserted rows (insert-role parts)."""
        return [f for k, f in self.insert_sources() if k == "file"]

    def has_inserts(self, discovery: str = "provenance") -> bool:
        if self.kind in ("APPEND", "OVERWRITE", "UPSERT", "?APPEND", "?OVERWRITE"):
            return bool(self.insert_sources(discovery))
        return False

    def delete_pred(self) -> dict | None:
        """Logical delete predicate; FALSE if the op deletes nothing; None if unknown."""
        if self.kind in ("APPEND", "REPLACE", "?APPEND", "?REPLACE"):
            return dict(X.FALSE)
        if not self.recorded:
            return None
        if self.kind in ("DELETE", "OVERWRITE"):
            return self.filter
        if self.kind == "UPSERT":
            return X.isin(self.key_col, self.keys or [])
        return None

    def partitions(self) -> set[tuple]:
        out: set[tuple] = set()
        for s in self.snaps:
            out |= s.partitions()
        return out

    def describe(self) -> dict:
        d = {"op_id": self.op_id, "side": self.side, "kind": self.kind, "mode": self.mode}
        if self.filter is not None:
            d["filter"] = X.describe(self.filter)
        if self.keys is not None:
            d["n_keys"] = len(self.keys)
        return d


@dataclass
class SideDelta:
    removed_data: dict[str, C.LiveFile]     # base data files not live at head
    added_data: dict[str, C.LiveFile]       # data files live at head, not in base
    added_deletes: dict[str, C.LiveFile]    # delete files live at head, not in base
    removed_deletes: dict[str, C.LiveFile]  # base delete files not live at head

    def deleted_targets(self) -> set[str]:
        return {d.ref_path for d in self.added_deletes.values() if d.ref_path is not None}

    def is_empty(self) -> bool:
        return not (self.removed_data or self.added_data or self.added_deletes or self.removed_deletes)


@dataclass
class Lineage:
    base_id: int | None
    heads: dict[str, int | None]
    base: C.TableState
    states: dict[str, C.TableState]
    ops: dict[str, list[OpRecord]]
    deltas: dict[str, SideDelta]
    snaps: dict[str, list[SnapChange]]
    structural: list[str] = field(default_factory=list)
    table: Table | None = None
    _parent_states: dict = field(default_factory=dict)

    def state_before(self, snap: SnapChange, stats: C.IOStats | None = None) -> C.TableState:
        """Live state of the parent of `snap` (the input state of that snapshot)."""
        parent = self.table.metadata.snapshot_by_id(snap.snapshot_id).parent_snapshot_id
        if parent not in self._parent_states:
            self._parent_states[parent] = C.load_state(self.table, parent, stats)
        return self._parent_states[parent]


def merge_parents(snap) -> list[int]:
    props = C.summary_props(snap)
    out = []
    for k in ("snapmerge.merge-source-head",):
        if k in props:
            try:
                out.append(int(props[k]))
            except ValueError:
                pass
    return out


def _all_ancestors(table: Table, sid: int | None) -> list[int]:
    """Ancestors including recorded merge parents (BFS order)."""
    seen: OrderedDict[int, None] = OrderedDict()
    frontier = [sid] if sid is not None else []
    while frontier:
        nxt = []
        for s in frontier:
            if s is None or s in seen:
                continue
            snap = table.metadata.snapshot_by_id(s)
            if snap is None:
                continue
            seen[s] = None
            nxt.append(snap.parent_snapshot_id)
            nxt.extend(merge_parents(snap))
        frontier = nxt
    return list(seen)


def lca(table: Table, a: int | None, c: int | None) -> int | None:
    """Lowest common ancestor: the newest (highest sequence number) common ancestor."""
    if a is None or c is None:
        return None
    anc_a = set(_all_ancestors(table, a))
    common = [s for s in _all_ancestors(table, c) if s in anc_a]
    if not common:
        return None
    return max(common, key=lambda s: table.metadata.snapshot_by_id(s).sequence_number)


def first_parent_path(table: Table, base: int | None, head: int | None) -> list[int]:
    """Snapshots on the first-parent chain from head back to (excluding) base, oldest first."""
    out = []
    sid = head
    while sid is not None and sid != base:
        out.append(sid)
        snap = table.metadata.snapshot_by_id(sid)
        sid = snap.parent_snapshot_id if snap else None
    if base is not None and sid != base:
        raise ValueError("base is not a first-parent ancestor of head")
    return list(reversed(out))


def snapshot_changes(table: Table, sid: int, stats: C.IOStats | None = None) -> SnapChange:
    snap = table.metadata.snapshot_by_id(sid)
    props = C.summary_props(snap)
    ch = SnapChange(sid, props.get("operation", "overwrite"), props)
    if stats is not None:
        try:
            import os
            stats.metadata_bytes_read += os.path.getsize(C.local_path(snap.manifest_list))
        except OSError:
            pass
    for m in snap.manifests(table.io):
        if m.added_snapshot_id != sid:
            continue
        for e in C._manifest_entries(table, m, stats):
            if e.snapshot_id != sid:
                continue
            df = e.data_file
            content = {0: "data", 1: "pos", 2: "eq"}[int(df.content)]
            lf = C.LiveFile(path=df.file_path, content=content, entry=e, partition_key=C._pkey(e),
                            spec_id=df.spec_id, data_seq=e.sequence_number or 0, file_seq=e.file_sequence_number,
                            snapshot_id=e.snapshot_id, record_count=df.record_count, size=df.file_size_in_bytes,
                            ref_path=C._ref_path(df) if content == "pos" else None)
            if e.status == ManifestEntryStatus.ADDED:
                ch.added.append(lf)
            elif e.status == ManifestEntryStatus.DELETED:
                ch.removed.append(lf)
    return ch


def _ops_from_snaps(side: str, snaps: list[SnapChange]) -> list[OpRecord]:
    ops: list[OpRecord] = []
    by_id: dict[str, OpRecord] = {}
    for ch in snaps:
        p = ch.props
        if "snapmerge.op" in p and "snapmerge.op-id" in p:
            oid = p["snapmerge.op-id"]
            rec = by_id.get(oid)
            if rec is None:
                keys = json.loads(p["snapmerge.keys"]) if "snapmerge.keys" in p else None
                rec = OpRecord(op_id=oid, side=side, kind=p["snapmerge.op"], recorded=True,
                               filter=X.loads(p.get("snapmerge.filter")), keys=keys,
                               key_col=p.get("snapmerge.key-col", "rid"), mode=p.get("snapmerge.mode", "cow"),
                               dynamic=p.get("snapmerge.dynamic") == "true",
                               schema_version=int(p.get("snapmerge.schema-version", "1")),
                               inserted_records=(int(p["snapmerge.inserted-records"])
                                                 if "snapmerge.inserted-records" in p else None))
                by_id[oid] = rec
                ops.append(rec)
            rec.snaps.append(ch)
        else:
            kind = {"append": "?APPEND", "delete": "?DELETE", "overwrite": "?OVERWRITE",
                    "replace": "?REPLACE"}.get(ch.operation, "?OVERWRITE")
            rec = OpRecord(op_id=f"{side}-snap-{ch.snapshot_id}", side=side, kind=kind, recorded=False,
                           filter=None, keys=None, key_col="rid", mode="unknown",
                           dynamic=ch.props.get("replace-partitions") == "true", snaps=[ch])
            ops.append(rec)
    return ops


def side_delta(base: C.TableState, head: C.TableState) -> SideDelta:
    b, h = base.files, head.files
    return SideDelta(
        removed_data={p: f for p, f in b.items() if f.content == "data" and p not in h},
        added_data={p: f for p, f in h.items() if f.content == "data" and p not in b},
        added_deletes={p: f for p, f in h.items() if f.content != "data" and p not in b},
        removed_deletes={p: f for p, f in b.items() if f.content != "data" and p not in h},
    )


def resolve(table: Table, ref: str | int) -> int | None:
    """A branch name is resolved to its current head once; a snapshot id is taken as given (pinned)."""
    return int(ref) if isinstance(ref, int) else C.head_id(table, ref)


def compute_lineage(table: Table, target: str | int, source: str | int, stats: C.IOStats | None = None) -> Lineage:
    a = resolve(table, target)
    c = resolve(table, source)
    base_id = lca(table, a, c)
    base = C.load_state(table, base_id, stats)
    states = {"A": C.load_state(table, a, stats), "C": C.load_state(table, c, stats)}
    lin = Lineage(base_id=base_id, heads={"A": a, "C": c}, base=base, states=states, ops={}, deltas={}, snaps={},
                  table=table)
    if base_id is None:
        lin.structural.append("no-common-ancestor")
    for side, hid in (("A", a), ("C", c)):
        try:
            path = first_parent_path(table, base_id, hid)
        except ValueError:
            lin.structural.append(f"{side}: base not on first-parent chain")
            path = []
        snaps = [snapshot_changes(table, s, stats) for s in path]
        lin.snaps[side] = snaps
        lin.ops[side] = _ops_from_snaps(side, snaps)
        lin.deltas[side] = side_delta(base, states[side])
        st = states[side]
        if st.schema_id != base.schema_id and base_id is not None:
            lin.structural.append(f"{side}: schema change")
        base_specs = {f.spec_id for f in base.files.values()} or {0}
        if any(f.spec_id not in base_specs for f in st.files.values()):
            lin.structural.append(f"{side}: partition spec change")
        if st.has_unsupported_deletes():
            lin.structural.append(f"{side}: equality deletes or deletion vectors")
        for ch in snaps:
            if any(f.content in ("eq", "dv") for f in ch.added):
                lin.structural.append(f"{side}: equality deletes or deletion vectors")
                break
        d = lin.deltas[side]
        for p, df in d.removed_deletes.items():
            if df.ref_path is not None and df.ref_path in st.files:
                lin.structural.append(f"{side}: delete file removed while its data file stays live")
                break
            if df.ref_path is None:
                lin.structural.append(f"{side}: partition-scoped delete file removed")
                break
        if any(df.ref_path is None for df in d.added_deletes.values()):
            lin.structural.append(f"{side}: partition-scoped position deletes")
    lin.structural += validate_metadata(lin)
    lin.structural += validate_identity(lin)
    lin.structural = sorted(set(lin.structural))
    return lin


def validate_identity(lin: Lineage) -> list[str]:
    """Fresh post-base content-file identities (A3): every data or delete file a branch adds after the
    base has a path that was not live at the base, was not added or removed earlier on that branch,
    and is not added by the other branch.  Shared BASE files are expected and unaffected."""
    out: list[str] = []
    base_paths = set(lin.base.files)
    added: dict[str, set[str]] = {}
    for side in ("A", "C"):
        seen = set(base_paths)
        mine: set[str] = set()
        for ch in lin.snaps.get(side, []):
            for f in ch.added:
                if f.path in seen:
                    out.append(f"identity: {side} re-registers content file {f.path.rsplit('/', 1)[-1]} "
                               "(live at the base, or added or removed earlier on this branch)")
                seen.add(f.path)
                mine.add(f.path)
            for f in ch.removed:
                seen.add(f.path)
        added[side] = mine
    for path in sorted(added["A"] & added["C"]):
        out.append(f"identity: content file {path.rsplit('/', 1)[-1]} added after the base on both branches")
    return out


METADATA_SCHEMA_VERSION = 2


def _nan_columns(lin: Lineage, cols: set[str]) -> list[str]:
    """Predicate columns holding NaN in some data file of the base or of a side's new files."""
    if not cols or lin.table is None:
        return []
    schema = lin.table.schema()
    ids = {}
    for c in cols:
        try:
            ids[schema.find_field(c).field_id] = c
        except ValueError:
            pass
    files = [f for f in lin.base.files.values() if f.content == "data"]
    for side in ("A", "C"):
        files += [f for ch in lin.snaps.get(side, []) for f in ch.added_data()]
    bad = set()
    for f in files:
        for fid, n in (f.data_file.nan_value_counts or {}).items():
            if n and fid in ids:
                bad.add(ids[fid])
    return sorted(bad)


def validate_metadata(lin: Lineage) -> list[str]:
    """Explicit rejection reasons for recorded metadata that SnapMerge cannot rely on (A1).

    missing       a predicate or key column the table schema does not have
    contradictory a role or count that contradicts the operation kind or the snapshot's shape
    incomplete    a schema-v2 operation whose last visible snapshot lacks the op-end marker, or
                  whose snapshots are interleaved with another operation's
    inconsistent  a schema-v2 inserted-row count that the insert parts do not account for
                  (mixed parts are counted in rules.py after the exact difference)
    Schema-v1 records (the submitted writer) carry no roles or counts; their rewrite-bearing
    snapshots are treated as mixed, which needs only A2."""
    out: list[str] = []
    pred_cols: set[str] = set()
    names = set(lin.table.schema().column_names) if lin.table is not None else None
    for side in ("A", "C"):
        order = [ch.snapshot_id for ch in lin.snaps.get(side, [])]
        pos = {sid: i for i, sid in enumerate(order)}
        for op in lin.ops.get(side, []):
            if not op.recorded:
                continue
            tag = f"metadata: {side} {op.op_id}"
            kind = op.kind
            if op.filter is not None:
                cols = X.columns(op.filter)
                pred_cols |= cols
                if names is not None and cols - names:
                    out.append(f"{tag}: missing predicate column {sorted(cols - names)[0]}")
            if kind == "UPSERT":
                pred_cols.add(op.key_col)
                if names is not None and op.key_col not in names:
                    out.append(f"{tag}: missing key column {op.key_col}")
            idx = [pos[ch.snapshot_id] for ch in op.snaps if ch.snapshot_id in pos]
            if idx and idx != list(range(idx[0], idx[0] + len(idx))):
                out.append(f"{tag}: incomplete (snapshots interleaved with another operation)")
            roles = op.snapshot_roles()
            if kind == "APPEND" and any(ch.removed for ch, _ in roles):
                out.append(f"{tag}: contradictory (APPEND removes files)")
            if kind == "DELETE" and any(r == "insert" for _, r in roles):
                out.append(f"{tag}: contradictory (DELETE with an insert part)")
            if op.schema_version < 2:
                continue
            sem = {ch.props.get("snapmerge.semantics", "fixed-rows") for ch in op.snaps}
            if sem != {"fixed-rows"}:
                out.append(f"{tag}: unsupported semantics {sorted(sem)} (only fixed-rows is supported)")
            for ch, r in roles:
                if ch.props.get("snapmerge.part") == "insert" and ch.removed_data():
                    out.append(f"{tag}: contradictory (insert part removes data files)")
            seen_insert = False
            for ch, r in roles:  # A2 phases: deletion before insertion
                if r == "insert":
                    seen_insert = True
                elif r in ("delete", "mixed") and seen_insert:
                    out.append(f"{tag}: contradictory (deletion part after an insertion part)")
                    break
            own = {f.path for ch in op.snaps for f in ch.added_data()}
            if any(f.ref_path in own for ch in op.snaps for f in ch.added if f.content != "data"):
                out.append(f"{tag}: contradictory (position deletes on a data file the operation added)")
            if op.snaps and op.snaps[-1].props.get("snapmerge.op-end") != "true":
                out.append(f"{tag}: incomplete (last snapshot lacks op-end)")
            if op.inserted_records is not None:
                if kind in ("DELETE", "REPLACE") and op.inserted_records:
                    out.append(f"{tag}: contradictory ({kind} records inserted rows)")
                if not any(r == "mixed" for _, r in roles):
                    got = sum(f.record_count for ch, r in roles if r == "insert" for f in ch.added_data())
                    if kind in ("APPEND", "OVERWRITE", "UPSERT") and got != op.inserted_records:
                        out.append(f"{tag}: inconsistent (insert parts hold {got} rows, "
                                   f"{op.inserted_records} recorded)")
    for c in _nan_columns(lin, pred_cols):
        out.append(f"metadata: NaN values in predicate column {c} (unsupported value domain)")
    return out
