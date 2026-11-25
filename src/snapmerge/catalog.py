"""PyIceberg 0.12.0 helpers.

This is the ONLY module that touches PyIceberg internals (manifest writers, snapshot objects,
transaction staging).  Everything else goes through the functions here.

Physical model (Iceberg v2): a snapshot's live state is the set of manifest entries with status
ADDED or EXISTING.  Data rows of a live data file are visible unless a live position-delete file
applies to them: same partition (or file-scoped to the path) and delete data-sequence-number >=
data-file data-sequence-number (Iceberg spec, "Scan Planning").
"""
from __future__ import annotations

import os
import time
import uuid
import warnings
from collections import defaultdict
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import to_bytes
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestFile,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import AddSnapshotUpdate, AssertRefSnapshotId, SetSnapshotRefUpdate
from pyiceberg.transforms import DayTransform, IdentityTransform, MonthTransform, YearTransform
from pyiceberg.types import LongType

PATH_FIELD_ID = 2147483546
POS_FIELD_ID = 2147483545
AVRO_CODEC = "gzip"  # DuckDB's Avro reader rejects zstd-coded manifests; gzip(deflate) is PyIceberg's default

OPS = {
    "append": Operation.APPEND,
    "overwrite": Operation.OVERWRITE,
    "delete": Operation.DELETE,
    "replace": Operation.REPLACE,
}


# ============================================================================ cost accounting

@dataclass
class IOStats:
    data_bytes_read: int = 0
    delete_bytes_read: int = 0
    metadata_bytes_read: int = 0
    data_files_read: int = 0
    delete_files_read: int = 0
    manifests_read: int = 0
    data_bytes_written: int = 0
    delete_bytes_written: int = 0
    metadata_bytes_written: int = 0
    data_files_written: int = 0
    delete_files_written: int = 0
    manifests_written: int = 0
    # Re-review 2026-10-07 (G2): data + delete bytes read, attributed to the merge phase that read them
    # (lineage, recovery = insertion recovery from mixed snapshots and inserted-count validation,
    # logical = predicate evaluation on inserted files, compose, reconcile; "other" outside SnapMerge).
    phase: str = "other"
    phase_bytes: dict = field(default_factory=dict)
    # Minor revision 2026-10-08 (M2): the read budget is a cap on the CHARGED bytes of one decision.  Every
    # opening of a data or delete file is charged the file's recorded size (manifest file_size_in_bytes),
    # whatever columns are read, and a re-read is charged again (no cache across openings).  decide() sets
    # read_limit to the budget; admit() refuses an opening that would exceed it, BEFORE the file is read.
    read_limit: int | None = None

    def charge(self, nbytes: int) -> None:
        self.phase_bytes[self.phase] = self.phase_bytes.get(self.phase, 0) + int(nbytes)

    def charged(self) -> int:
        """Data and delete bytes charged so far."""
        return self.data_bytes_read + self.delete_bytes_read

    def admit(self, nbytes: int) -> None:
        if self.read_limit is not None and self.charged() + int(nbytes) > self.read_limit:
            raise ReadBudgetExceeded(self.charged(), int(nbytes), self.read_limit, self.phase)

    def add(self, other: "IOStats") -> None:
        for f in fields(self):
            if f.name in ("phase", "read_limit"):
                continue
            if f.name == "phase_bytes":
                for k, v in other.phase_bytes.items():
                    self.phase_bytes[k] = self.phase_bytes.get(k, 0) + v
                continue
            setattr(self, f.name, getattr(self, f.name) + getattr(other, f.name))

    def as_dict(self) -> dict:
        d = {f.name: getattr(self, f.name) for f in fields(self) if f.name not in ("phase", "phase_bytes", "read_limit")}
        d["read_bytes_by_phase"] = dict(sorted(self.phase_bytes.items()))
        return d


# ============================================================================ catalog / tables

class ReadBudgetExceeded(RuntimeError):
    """A data or delete file opening would push the charged bytes of a decision over its read budget
    (minor revision 2026-10-08, M2).  Raised before the file is read; decide() turns it into an
    unsupported (over-budget) outcome, so the charged bytes never exceed the budget."""

    def __init__(self, charged: int, nbytes: int, limit: int, phase: str):
        super().__init__(f"read budget {limit} B: {charged} B charged, opening {nbytes} B in phase {phase}")
        self.charged, self.nbytes, self.limit, self.phase = charged, nbytes, limit, phase


def open_catalog(root: str | os.PathLike, name: str = "snapmerge") -> SqlCatalog:
    root = Path(root).resolve()
    (root / "warehouse").mkdir(parents=True, exist_ok=True)
    return SqlCatalog(name, uri=f"sqlite:///{root}/catalog.db", warehouse=f"file://{root}/warehouse")


_TRANSFORMS = {"identity": IdentityTransform, "month": MonthTransform, "day": DayTransform, "year": YearTransform}


def create_table(catalog: SqlCatalog, ident: str, arrow_schema: pa.Schema,
                 partition: tuple[str, str] | None, properties: dict[str, str] | None = None) -> Table:
    """Create a v2 table.  `partition` = (source column, transform name) or None."""
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.table.name_mapping import MappedField, NameMapping

    ns = ident.split(".")[0]
    try:
        catalog.create_namespace(ns)
    except Exception:  # already exists
        pass
    mapping = NameMapping([MappedField(field_id=i + 1, names=[f.name]) for i, f in enumerate(arrow_schema)])
    schema = pyarrow_to_schema(arrow_schema, name_mapping=mapping)
    spec = PartitionSpec()
    if partition is not None:
        col, tname = partition
        src = schema.find_field(col)
        spec = PartitionSpec(PartitionField(source_id=src.field_id, field_id=1000,
                                            transform=_TRANSFORMS[tname](), name=f"{col}_{tname}"))
    props = {"format-version": "2", "write.delete.mode": "copy-on-write"}
    props.update(properties or {})
    return catalog.create_table(ident, schema=schema, partition_spec=spec, properties=props)


def head(table: Table, ref: str) -> Snapshot | None:
    return table.metadata.snapshot_by_name(ref)


def head_id(table: Table, ref: str) -> int | None:
    s = head(table, ref)
    return None if s is None else s.snapshot_id


def create_branch(table: Table, name: str, snapshot_id: int) -> Table:
    table.manage_snapshots().create_branch(snapshot_id, name).commit()
    return table.refresh()


def ancestors(table: Table, snapshot_id: int | None) -> list[Snapshot]:
    """Snapshot and its ancestors, newest first."""
    out = []
    sid = snapshot_id
    while sid is not None:
        s = table.metadata.snapshot_by_id(sid)
        if s is None:
            break
        out.append(s)
        sid = s.parent_snapshot_id
    return out


def summary_props(snap: Snapshot) -> dict[str, str]:
    s = snap.summary
    props = dict(s._additional_properties) if s is not None else {}
    props["operation"] = s.operation.value if s is not None else "overwrite"
    return props


def local_path(uri: str) -> str:
    return uri[len("file://"):] if uri.startswith("file://") else uri


def metadata_location(table: Table) -> str:
    return table.metadata_location


# ============================================================================ live state

@dataclass
class LiveFile:
    path: str
    content: str  # "data" | "pos" | "eq" | "dv"
    entry: ManifestEntry
    partition_key: tuple
    spec_id: int
    data_seq: int
    file_seq: int | None
    snapshot_id: int | None  # snapshot that added the file
    record_count: int
    size: int
    ref_path: str | None = None  # file-scoped position deletes: referenced data file

    @property
    def data_file(self) -> DataFile:
        return self.entry.data_file


_MANIFEST_CACHE: dict[str, tuple] = {}


def clear_metadata_caches() -> None:
    """Drop every in-process metadata cache (this module's manifest-entry cache and PyIceberg's
    ManifestFile cache), so that a timed merge does not profit from a policy that ran before it."""
    from pyiceberg.manifest import clear_manifest_cache

    _MANIFEST_CACHE.clear()
    clear_manifest_cache()


def _manifest_entries(table: Table, m: ManifestFile, stats: IOStats | None) -> tuple:
    if stats is not None:
        stats.metadata_bytes_read += m.manifest_length
        stats.manifests_read += 1
    key = m.manifest_path
    hit = _MANIFEST_CACHE.get(key)
    if hit is None:
        hit = tuple(m.fetch_manifest_entry(table.io, discard_deleted=False))
        if len(_MANIFEST_CACHE) > 20000:
            _MANIFEST_CACHE.clear()
        _MANIFEST_CACHE[key] = hit
    return hit


def _pkey(entry: ManifestEntry) -> tuple:
    p = entry.data_file.partition
    vals = tuple(p[i] for i in range(len(p))) if p is not None else ()
    return (entry.data_file.spec_id,) + vals


def _ref_path(df: DataFile) -> str | None:
    lo = (df.lower_bounds or {}).get(PATH_FIELD_ID)
    hi = (df.upper_bounds or {}).get(PATH_FIELD_ID)
    if lo is not None and hi is not None and lo == hi:
        try:
            return lo.decode("utf-8")
        except Exception:
            return None
    return None


@dataclass
class TableState:
    snapshot_id: int | None
    sequence_number: int
    files: dict[str, LiveFile] = field(default_factory=dict)
    manifests: list[ManifestFile] = field(default_factory=list)
    manifest_paths_of: dict[str, str] = field(default_factory=dict)  # file path -> manifest path
    schema_id: int | None = None

    def data_files(self) -> list[LiveFile]:
        return [f for f in self.files.values() if f.content == "data"]

    def delete_files(self) -> list[LiveFile]:
        return [f for f in self.files.values() if f.content != "data"]

    def has_unsupported_deletes(self) -> bool:
        return any(f.content in ("eq", "dv") for f in self.files.values())

    def deletes_for(self, data: LiveFile) -> list[LiveFile]:
        """Position-delete files that apply to `data` under the v2 sequence-number rule."""
        out = []
        for d in self.files.values():
            if d.content != "pos":
                continue
            if d.data_seq < data.data_seq:
                continue
            if d.ref_path is not None:
                if d.ref_path == data.path:
                    out.append(d)
            elif d.partition_key == data.partition_key:
                out.append(d)
        return out

    def partitions(self) -> set[tuple]:
        return {f.partition_key for f in self.files.values()}

    def total_rows_upper(self) -> int:
        return sum(f.record_count for f in self.data_files())


def load_state(table: Table, snapshot_id: int | None, stats: IOStats | None = None,
               metadata=None) -> TableState:
    """Live state of a snapshot.  `metadata` (e.g. a transaction's staged metadata) defaults to
    table.metadata; staged snapshots are readable because their manifest lists are already written."""
    if snapshot_id is None:
        return TableState(None, 0)
    snap = (metadata or table.metadata).snapshot_by_id(snapshot_id)
    if stats is not None:
        try:
            stats.metadata_bytes_read += os.path.getsize(local_path(snap.manifest_list))
        except OSError:
            pass
    manifests = snap.manifests(table.io)
    st = TableState(snapshot_id, snap.sequence_number, manifests=list(manifests), schema_id=snap.schema_id)
    for m in manifests:
        for e in _manifest_entries(table, m, stats):
            if e.status == ManifestEntryStatus.DELETED:
                continue
            df = e.data_file
            content = {DataFileContent.DATA: "data", DataFileContent.POSITION_DELETES: "pos",
                       DataFileContent.EQUALITY_DELETES: "eq"}[df.content]
            if content == "pos" and df.file_format == FileFormat.PUFFIN:
                content = "dv"
            lf = LiveFile(path=df.file_path, content=content, entry=e, partition_key=_pkey(e),
                          spec_id=df.spec_id, data_seq=e.sequence_number or 0, file_seq=e.file_sequence_number,
                          snapshot_id=e.snapshot_id, record_count=df.record_count, size=df.file_size_in_bytes,
                          ref_path=_ref_path(df) if content == "pos" else None)
            st.files[lf.path] = lf
            st.manifest_paths_of[lf.path] = m.manifest_path
    return st


# ============================================================================ file reads

def read_data_file(lf: LiveFile, stats: IOStats | None = None, columns: list[str] | None = None) -> pa.Table:
    """Rows of a data file in physical order (position i = row i)."""
    if stats is not None:
        stats.admit(lf.size)
    t = pq.read_table(local_path(lf.path), columns=columns)
    if stats is not None:
        stats.data_bytes_read += lf.size
        stats.data_files_read += 1
        stats.charge(lf.size)
    return t


def read_deleted_positions(dfs: Iterable[LiveFile], target_path: str, stats: IOStats | None = None) -> np.ndarray:
    pos: list[np.ndarray] = []
    for d in dfs:
        if stats is not None:
            stats.admit(d.size)
        t = pq.read_table(local_path(d.path))
        if stats is not None:
            stats.delete_bytes_read += d.size
            stats.delete_files_read += 1
            stats.charge(d.size)
        paths = t.column("file_path").to_numpy(zero_copy_only=False)
        p = t.column("pos").to_numpy()
        pos.append(p[paths == target_path])
    if not pos:
        return np.empty(0, dtype=np.int64)
    return np.unique(np.concatenate(pos).astype(np.int64))


def live_rows(state: TableState, lf: LiveFile, stats: IOStats | None = None,
              columns: list[str] | None = None) -> tuple[pa.Table, np.ndarray]:
    """(live rows, their physical positions) of a data file in `state`."""
    t = read_data_file(lf, stats, columns)
    deleted = read_deleted_positions(state.deletes_for(lf), lf.path, stats)
    positions = np.arange(t.num_rows, dtype=np.int64)
    if deleted.size:
        keep = ~np.isin(positions, deleted)
        positions = positions[keep]
        t = t.filter(pa.array(keep))
    return t, positions


# ============================================================================ file writes

def write_data_files(table: Table, rows: pa.Table, stats: IOStats | None = None) -> list[DataFile]:
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    if rows.num_rows == 0:
        return []
    out = list(_dataframe_to_data_files(table_metadata=table.metadata, df=rows, io=table.io,
                                        write_uuid=uuid.uuid4()))
    for df in out:
        try:
            df.spec_id
        except AttributeError:
            df.spec_id = table.metadata.default_spec_id
        if stats is not None:
            stats.data_bytes_written += df.file_size_in_bytes
            stats.data_files_written += 1
    return out


_POS_SCHEMA = pa.schema([
    pa.field("file_path", pa.string(), nullable=False, metadata={"PARQUET:field_id": str(PATH_FIELD_ID)}),
    pa.field("pos", pa.int64(), nullable=False, metadata={"PARQUET:field_id": str(POS_FIELD_ID)}),
])


def write_position_delete_file(table: Table, target: DataFile, positions: Iterable[int],
                               stats: IOStats | None = None) -> DataFile:
    """Hand-authored, file-scoped v2 position delete file (path bounds lower == upper)."""
    positions = sorted({int(p) for p in positions})
    if not positions:
        raise ValueError("empty position delete file")
    loc = table.location_provider().new_data_location(f"pos-del-{uuid.uuid4()}.parquet")
    fpath = local_path(loc)
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    tb = pa.table({"file_path": [target.file_path] * len(positions), "pos": positions}, schema=_POS_SCHEMA)
    pq.write_table(tb, fpath)
    size = os.path.getsize(fpath)
    pb = target.file_path.encode("utf-8")
    n = len(positions)
    df = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES, file_path=loc, file_format=FileFormat.PARQUET,
        partition=target.partition, record_count=n, file_size_in_bytes=size, column_sizes={},
        value_counts={PATH_FIELD_ID: n, POS_FIELD_ID: n}, null_value_counts={PATH_FIELD_ID: 0, POS_FIELD_ID: 0},
        nan_value_counts={},
        lower_bounds={PATH_FIELD_ID: pb, POS_FIELD_ID: to_bytes(LongType(), positions[0])},
        upper_bounds={PATH_FIELD_ID: pb, POS_FIELD_ID: to_bytes(LongType(), positions[-1])},
        key_metadata=None, split_offsets=None, equality_ids=None, sort_order_id=None,
    )
    df.spec_id = target.spec_id
    if stats is not None:
        stats.delete_bytes_written += size
        stats.delete_files_written += 1
    return df


# ============================================================================ snapshot commit

class _DeleteManifestWriterV2(ManifestWriterV2):
    """PyIceberg 0.12.0 only writes DATA manifests; delete manifests differ only in content."""

    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


class CommitAborted(RuntimeError):
    pass


from pyiceberg.exceptions import CommitFailedException  # noqa: E402


class TargetMoved(CommitAborted, CommitFailedException):
    """The branch no longer points to the head the snapshot was planned on: re-plan (retryable)."""


from pyiceberg.exceptions import CommitStateUnknownException  # noqa: E402


class CommitStateUnknown(RuntimeError):
    """The catalog call failed in a way that does not tell whether the snapshot was installed, and
    the follow-up status check could not decide it either (re-review 2026-10-07, G3).  The attempt's
    files are RETAINED: they may be referenced by an installed snapshot.  Resolve later with
    emit.resolve_unknown_commit(table, branch, outcome.witness), which removes the attempt's own files only
    on positive evidence of non-installation (attempt_status with the planned head; M1, 2026-10-08)."""

    def __init__(self, message: str, snapshot_id: int):
        super().__init__(message)
        self.snapshot_id = snapshot_id


#: Settled-call premise of the tested catalog wrapper (minor revision 2026-10-08, M1).  PyIceberg's
#: SqlCatalog on SQLite runs a commit in the calling process as one database transaction, so when the call
#: has returned or raised, the transaction has committed or rolled back and nothing applies it later.
#: CommitFailedException (a failed requirement such as "branch still points to A", or the conditional
#: UPDATE of the metadata pointer matching no row) and the stale-sequence ValueError are raised before the
#: transaction commits: they prove that THIS call installed nothing.  Any other exception (e.g. an I/O error
#: during or after the database commit, or a lost confirmation) leaves the outcome to attempt_status().
#: Set False for a catalog that may apply a request after the client gave up (e.g. a REST call whose
#: response timed out): absence of the snapshot is then never proof, and unresolved attempts stay unknown.
SETTLED_CATALOG_CALLS = True


def attempt_status(table: Table, branch: str, snapshot_id: int, expected_head: int | None = None) -> tuple[str, Table | None]:
    """Status of a commit attempt identified by its snapshot id, chosen once per attempt and re-used by its
    re-sequenced retries: ("committed" | "not-committed" | "unknown", refreshed table or None).

    committed      the snapshot is retained in the table metadata (a later commit on the branch, or a reset
                   of the branch, cannot hide it while it is retained) or reachable from the branch head;
    not-committed  only with positive evidence: the snapshot is absent, the walk from the branch head reaches
                   the planned head `expected_head` through retained snapshots only (complete ancestry), and
                   calls are settled.  An installed attempt is a child of expected_head (its commit asserts
                   that head), so it would lie on that path unless the branch was reset; resetting the
                   target or expiring its snapshots newer than expected_head before the outcome is resolved
                   is outside the supported deployment (A5 extends through resolution);
    unknown        otherwise: the table cannot be read, the walk meets a snapshot that is no longer retained
                   (incomplete ancestry) or ends without meeting expected_head (e.g. after a reset), no
                   expected head is given, or calls are not settled.  The attempt's files are then kept.
    Minor revision 2026-10-08 (M1): before, a walk that ended early also counted as not-committed."""
    try:
        t = table.refresh()
    except Exception:
        return "unknown", None
    if t.metadata.snapshot_by_id(snapshot_id) is not None:
        return "committed", t
    sid, seen, complete = head_id(t, branch), set(), False
    while sid is not None and sid not in seen:
        if sid == snapshot_id:
            return "committed", t
        if expected_head is not None and sid == expected_head:
            complete = True
            break
        seen.add(sid)
        snap = t.metadata.snapshot_by_id(sid)
        if snap is None:  # no longer retained: the ancestry below this point cannot be checked
            break
        sid = snap.parent_snapshot_id
    if complete and SETTLED_CATALOG_CALLS:
        return "not-committed", t
    return "unknown", t


RESEQUENCE_ATTEMPTS = 8  # commits lost to OTHER branches' commits are re-sequenced and retried


def _stale_sequence(e: Exception) -> bool:
    return isinstance(e, ValueError) and "older than last sequence number" in str(e)


def _manifest_totals(manifests: list[ManifestFile]) -> dict[str, int]:
    tot = defaultdict(int)
    for m in manifests:
        files = (m.added_files_count or 0) + (m.existing_files_count or 0)
        rows = (m.added_rows_count or 0) + (m.existing_rows_count or 0)
        if m.content == ManifestContent.DATA:
            tot["total-data-files"] += files
            tot["total-records"] += rows
        else:
            tot["total-delete-files"] += files
            tot["total-position-deletes"] += rows
    return tot


def commit_snapshot(
    table: Table,
    branch: str,
    expected_head: int | None,
    *,
    keep_manifests: list[ManifestFile],
    existing_entries: list[ManifestEntry],
    added_files: list[DataFile],
    deleted_entries: list[ManifestEntry],
    operation: str,
    properties: dict[str, str],
    stats: IOStats | None = None,
    crash_hook: Callable[[str], None] | None = None,
    tx=None,
    written: list[str] | None = None,
    info: dict | None = None,
) -> tuple[Table, int]:
    """Write manifests + manifest list and atomically move `branch` from `expected_head`.

    keep_manifests      : manifests reused verbatim (sequence numbers inherited as recorded)
    existing_entries    : live entries re-written as EXISTING (original data/file sequence numbers kept)
    added_files         : new data or delete files (ADDED; inherit this snapshot's sequence number)
    deleted_entries     : entries recorded as DELETED in this snapshot
    The catalog commit is guarded by AssertRefSnapshotId(branch == expected_head); a crash before it
    leaves only unreferenced files (no partial merge is visible).
    tx                  : if given (a Transaction from stage_transaction()), the snapshot is only STAGED
                          in it (sequence number and parent from the staged metadata) and the caller
                          commits all staged snapshots with ONE catalog commit (commit_staged()).  Only the
                          first staged snapshot's AssertRefSnapshotId is kept, i.e. the single catalog
                          commit asserts the branch head the chain started from.
    written             : if given, every metadata file this call writes is appended (cleanup of
                          failed attempts).
    info                : if given, receives {"resequenced": number of re-sequenced retries}.
    Concurrency (2026-10-05): if another branch committed after `table` was loaded, the catalog
    rejects the stale sequence number; the snapshot is then re-sequenced (a new manifest list with
    the next sequence number; the manifests are reused because their ADDED entries inherit it) and
    the commit is retried, as Iceberg's Java SnapshotProducer does.  If `branch` itself moved, the
    commit raises TargetMoved and the caller must re-plan.  Staged commits (tx) are not retried.
    """
    md = tx.table_metadata if tx is not None else table.metadata
    sid = md.new_snapshot_id()
    seq = md.next_sequence_number()
    spec = md.spec()
    schema = md.schema()
    commit_uuid = uuid.uuid4()
    counter = iter(range(10**6))
    loc = table.location_provider()

    def new_out():
        p = loc.new_metadata_location(f"{commit_uuid}-m{next(counter)}.avro")
        if written is not None:
            written.append(p)
        return table.io.new_output(p)

    groups = {"data": ([], [], []), "deletes": ([], [], [])}
    for e in existing_entries:
        groups["data" if e.data_file.content == DataFileContent.DATA else "deletes"][0].append(e)
    for df in added_files:
        groups["data" if df.content == DataFileContent.DATA else "deletes"][1].append(df)
    for e in deleted_entries:
        groups["data" if e.data_file.content == DataFileContent.DATA else "deletes"][2].append(e)

    new_manifests: list[ManifestFile] = []
    for kind, (ex, add, rem) in groups.items():
        if not (ex or add or rem):
            continue
        # one manifest per partition spec id
        by_spec: dict[int, list] = defaultdict(lambda: ([], [], []))
        for e in ex:
            by_spec[e.data_file.spec_id][0].append(e)
        for d in add:
            by_spec[d.spec_id][1].append(d)
        for e in rem:
            by_spec[e.data_file.spec_id][2].append(e)
        for spec_id, (ex2, add2, rem2) in by_spec.items():
            wcls = ManifestWriterV2 if kind == "data" else _DeleteManifestWriterV2
            w = wcls(md.specs()[spec_id], schema, new_out(), sid, AVRO_CODEC)
            with w:
                for e in ex2:
                    if e.sequence_number is None:
                        raise ValueError("existing entry without data sequence number")
                    w.existing(e)
                for d in add2:
                    w.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=sid,
                                                  sequence_number=None, file_sequence_number=None, data_file=d))
                for e in rem2:
                    w.delete(e)
            mf = w.to_manifest_file()
            new_manifests.append(mf)
            if stats is not None:
                stats.metadata_bytes_written += mf.manifest_length
                stats.manifests_written += 1

    manifests = new_manifests + list(keep_manifests)

    def write_list(seq_no: int, attempt: int) -> str:
        ml_path = loc.new_metadata_location(f"snap-{sid}-{attempt}-{commit_uuid}.avro")
        if written is not None:
            written.append(ml_path)
        with write_manifest_list(format_version=2, output_file=table.io.new_output(ml_path), snapshot_id=sid,
                                 parent_snapshot_id=expected_head, sequence_number=seq_no,
                                 avro_compression=AVRO_CODEC) as mlw:
            mlw.add_manifests(manifests)
        if stats is not None:
            stats.metadata_bytes_written += os.path.getsize(local_path(ml_path))
        return ml_path

    ml_path = write_list(seq, 0)
    if crash_hook is not None:
        crash_hook("after_manifest_list")  # may raise to simulate a crash before the catalog commit

    props = {k: str(v) for k, v in properties.items()}
    totals = _manifest_totals(manifests)
    for k, v in totals.items():
        props.setdefault(k, str(v))
    added_data = sum(1 for d in added_files if d.content == DataFileContent.DATA)
    added_del = len(added_files) - added_data
    props.setdefault("added-data-files", str(added_data))
    props.setdefault("added-delete-files", str(added_del))
    props.setdefault("deleted-data-files", str(sum(1 for e in deleted_entries
                                                   if e.data_file.content == DataFileContent.DATA)))

    def make_tx(md_table, seq_no: int, path: str):
        snap = Snapshot(snapshot_id=sid, parent_snapshot_id=expected_head, sequence_number=seq_no,
                        timestamp_ms=int(time.time() * 1000), manifest_list=path,
                        summary=Summary(operation=OPS[operation], **props), schema_id=md.current_schema_id)
        t = md_table.transaction() if tx is None else tx
        t._apply(
            (AddSnapshotUpdate(snapshot=snap),
             SetSnapshotRefUpdate(snapshot_id=sid, ref_name=branch, type=SnapshotRefType.BRANCH)),
            (AssertRefSnapshotId(snapshot_id=expected_head, ref=branch),),
        )
        return t

    if tx is not None:
        make_tx(table, seq, ml_path)
        return table, sid
    cur = make_tx(table, seq, ml_path)
    if crash_hook is not None:
        crash_hook("before_catalog_commit")
    # Commit outcomes (re-review 2026-10-07, G3; M1 2026-10-08).  info["catalog_call"] tells the caller's
    # cleanup what is known: None = the catalog was never called (files are unreferenced); "definite-abort" =
    # the catalog rejected every call of the attempt, or attempt_status found positive evidence that the
    # snapshot is not installed; "committed"; "started" (set while a call is in flight and left in place if
    # the outcome stays unknown) or "unresolved" -> keep the files.
    for attempt in range(1, RESEQUENCE_ATTEMPTS + 1):
        if info is not None:
            info["catalog_call"] = "started"
            info["snapshot_id"] = sid
        try:
            if crash_hook is not None:
                crash_hook("catalog_commit")  # may install the snapshot and then raise (lost confirmation)
            new_table = cur.commit_transaction()
            if info is not None:
                info["resequenced"] = attempt - 1
                info["catalog_call"] = "committed"
            return new_table, sid
        except (ValueError, CommitFailedException) as e:
            if isinstance(e, ValueError) and not _stale_sequence(e):
                status, t = attempt_status(table, branch, sid, expected_head)  # unexpected: resolve, as below
                if info is not None:
                    info["commit_resolution"] = status
                if status == "committed":
                    if info is not None:
                        info["resequenced"] = attempt - 1
                        info["catalog_call"] = "committed"
                    return t, sid
                if status == "unknown":
                    raise CommitStateUnknown(f"commit outcome unknown for snapshot {sid}: {e}", sid) from e
                if info is not None:
                    info["catalog_call"] = "definite-abort"
                raise
            # a rejected CAS / stale sequence number: the catalog did not install THIS call (settled premise)
            if info is not None:
                info["catalog_call"] = "definite-abort"
            table = table.refresh()
            if head_id(table, branch) != expected_head:
                # Own snapshot first (minor revision 2026-10-08, M1): the target may have moved because this
                # attempt was installed and a later commit followed.  Every earlier call of the attempt was
                # rejected too, so this check cannot find it under the settled premise; it is kept as a guard,
                # and only positive evidence of non-installation lets the caller clean up.
                status, t = attempt_status(table, branch, sid, expected_head)
                if info is not None:
                    info["commit_resolution"] = status
                if status == "committed":
                    if info is not None:
                        info["resequenced"] = attempt - 1
                        info["catalog_call"] = "committed"
                    return t, sid
                if status == "unknown":
                    if info is not None:
                        info["catalog_call"] = "unresolved"
                    raise CommitStateUnknown(f"target {branch} moved and the outcome of snapshot {sid} cannot "
                                             f"be established: {e}", sid) from e
                raise TargetMoved(f"branch {branch} moved from {expected_head} to {head_id(table, branch)}") from e
            if attempt == RESEQUENCE_ATTEMPTS:
                raise
            seq = table.metadata.next_sequence_number()
            cur = make_tx(table, seq, write_list(seq, attempt))
        except Exception as e:  # incl. CommitStateUnknownException: the snapshot may or may not be installed
            status, t = attempt_status(table, branch, sid, expected_head)
            if info is not None:
                info["commit_resolution"] = status
            if status == "committed":
                if info is not None:
                    info["resequenced"] = attempt - 1
                    info["catalog_call"] = "committed"
                return t, sid
            if status == "not-committed":
                if info is not None:
                    info["catalog_call"] = "definite-abort"
                raise CommitAborted(f"catalog error, snapshot {sid} not installed: {e}") from e
            raise CommitStateUnknown(f"commit outcome unknown for snapshot {sid}: {e}", sid) from e
    raise AssertionError("unreachable")


def stage_transaction(table: Table):
    """A transaction into which several snapshots can be staged (commit_delta(..., tx=tx))."""
    return table.transaction()


def commit_staged(tx, crash_hook: Callable[[str], None] | None = None) -> Table:
    """ONE catalog commit for every snapshot staged in `tx`."""
    if crash_hook is not None:
        crash_hook("before_catalog_commit")
    return tx.commit_transaction()


def live_manifest_entries(table: Table, m: ManifestFile) -> list[ManifestEntry]:
    return [e for e in _manifest_entries(table, m, None) if e.status != ManifestEntryStatus.DELETED]


def commit_delta(
    table: Table,
    branch: str,
    state: TableState,
    *,
    remove_paths: set[str],
    add_files: list[DataFile],
    carry_entries: list[ManifestEntry] | None = None,
    operation: str,
    properties: dict[str, str],
    stats: IOStats | None = None,
    crash_hook: Callable[[str], None] | None = None,
    tx=None,
    written: list[str] | None = None,
    info: dict | None = None,
) -> tuple[Table, int]:
    """Commit `state` minus `remove_paths` plus `add_files` (ADDED, new sequence number) plus
    `carry_entries` (EXISTING, original sequence numbers) on top of branch head `state.snapshot_id`.
    Manifests without removed entries are reused verbatim.  With `tx`, the snapshot is only staged."""
    keep, existing, deleted = [], [], []
    for m in state.manifests:
        live = live_manifest_entries(table, m)
        if any(e.data_file.file_path in remove_paths for e in live):
            for e in live:
                (deleted if e.data_file.file_path in remove_paths else existing).append(e)
        else:
            keep.append(m)
    missing = remove_paths - set(state.files)
    if missing:
        raise ValueError(f"cannot remove files that are not live: {sorted(missing)[:3]}")
    return commit_snapshot(table, branch, state.snapshot_id, keep_manifests=keep,
                           existing_entries=existing + list(carry_entries or []), added_files=add_files,
                           deleted_entries=deleted, operation=operation, properties=properties, stats=stats,
                           crash_hook=crash_hook, tx=tx, written=written, info=info)


def set_branch(table: Table, branch: str, snapshot_id: int, expected_head: int | None) -> Table:
    """Pointer-only update of a branch (used by pointer-level baselines)."""
    tx = table.transaction()
    tx._apply((SetSnapshotRefUpdate(snapshot_id=snapshot_id, ref_name=branch, type=SnapshotRefType.BRANCH),),
              (AssertRefSnapshotId(snapshot_id=expected_head, ref=branch),))
    return tx.commit_transaction()


# ============================================================================ readers

def scan_pyiceberg(table: Table, snapshot_id: int) -> pa.Table:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return table.scan(snapshot_id=snapshot_id).to_arrow()


def pyiceberg_reader_groups(table: Table, snapshot_id: int, tasks_per_group: int):
    """PyIceberg scan of a snapshot as a sequence of Arrow RecordBatchReaders, each over at most
    `tasks_per_group` planned FileScanTasks.  Same planning and per-task delete application as
    table.scan(...).to_arrow_batch_reader(); only the read concurrency is bounded (PyIceberg 0.12.0
    submits every task of one reader to its thread pool at once, so a single reader over a large
    table buffers most of the table in memory)."""
    from pyiceberg.table import _to_arrow_batch_reader_via_file_scan_tasks

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        scan = table.scan(snapshot_id=snapshot_id)
        tasks = list(scan.plan_files())
        schema = scan.projection()
    n = max(1, int(tasks_per_group))
    for i in range(0, len(tasks), n):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield _to_arrow_batch_reader_via_file_scan_tasks(scan, schema, tasks[i:i + n])


def duckdb_connect(memory_limit_mb: int | None = None):
    import duckdb

    con = duckdb.connect()
    con.execute("LOAD iceberg")
    con.execute("SET threads=2")
    if memory_limit_mb:
        con.execute(f"SET memory_limit='{int(memory_limit_mb)}MB'")
    return con


def scan_duckdb(con, table: Table, snapshot_id: int) -> pa.Table:
    q = f"SELECT * FROM iceberg_scan('{metadata_location(table)}', snapshot_from_id={int(snapshot_id)})"
    return con.execute(q).to_arrow_table()
