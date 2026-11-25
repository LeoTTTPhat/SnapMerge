"""Shared fixtures for the SnapMerge test-suite (small synthetic tables, temp catalogs)."""
from __future__ import annotations

import datetime as dt
import functools
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.rules import RuleConfig  # noqa: E402
from snapmerge.writer import LogicalOp, apply_op, write_base  # noqa: E402

SCHEMA = pa.schema([("rid", pa.int64()), ("d", pa.date32()), ("q", pa.float64()), ("s", pa.string())])


def rows(n: int, off: int = 0, q: float | None = None, month: int | None = None) -> pa.Table:
    """n rows; rid = off..off+n-1; d spread over Jan-Mar 2024 (or one month); q = i % 50."""
    return pa.table({
        "rid": pa.array(range(off, off + n), pa.int64()),
        "d": pa.array([dt.date(2024, month or 1 + (i % 3), 1 + i % 28) for i in range(n)], pa.date32()),
        "q": pa.array([float(q) if q is not None else float(i % 50) for i in range(n)]),
        "s": pa.array([f"x{i % 7}" for i in range(n)]),
    }, schema=SCHEMA)


def month_pred(m: int) -> dict:
    return X.and_(X.cmp("d", ">=", dt.date(2024, m, 1)), X.cmp("d", "<", dt.date(2024, m + 1, 1)))


class Env:
    """A temp catalog with one month-partitioned table, a base and branches A and C."""

    def __init__(self, base: pa.Table | None = None, chunks: int = 2, partition=("d", "month"), record=True):
        self.dir = Path(tempfile.mkdtemp(prefix="snapmerge-test-"))
        self.cat = C.open_catalog(self.dir)
        self.base = base if base is not None else rows(200)
        self.record = record
        self.table = C.create_table(self.cat, "t.t", self.base.schema, partition)
        self.table = write_base(self.table, self.base, chunks=chunks, record=record)
        self.base_id = C.head_id(self.table, "main")
        self.table = C.create_branch(self.table, "A", self.base_id)
        self.table = C.create_branch(self.table, "C", self.base_id)
        self.ops = {"A": [], "C": []}
        self.oracle = Oracle()

    def apply(self, side: str, *ops: LogicalOp) -> "Env":
        for op in ops:
            self.table = apply_op(self.table, side, op, f"{side}{len(self.ops[side])}", record=self.record)
            self.ops[side].append(op)
        return self

    def serial(self) -> dict:
        return self.oracle.serial(self.base, self.ops["A"], self.ops["C"])

    def merge(self, cfg: RuleConfig | None = None, target: str = "m", crash_hook=None):
        if target not in self.table.metadata.refs:
            self.table = C.create_branch(self.table, target, C.head_id(self.table, "A"))
        out = snapmerge(self.table, target, "C", cfg, crash_hook=crash_hook)
        if out.table is not None:
            self.table = out.table.refresh()
        return out

    def check(self, snapshot_id: int, expected=("o_ac", "o_ca")) -> dict:
        return self.oracle.check_snapshot(self.table, snapshot_id, list(expected))

    def close(self):
        self.oracle.close()
        shutil.rmtree(self.dir, ignore_errors=True)


@functools.lru_cache(maxsize=4)
def tpch_lineitem(sf: float = 0.01) -> pa.Table:
    from snapmerge.sources import generate_tpch

    return generate_tpch(sf, "lineitem")


def nyc_like(n: int = 6000, seed: int = 3) -> pa.Table:
    """Synthetic table with the NYC source schema (timestamps, day partitions) for tests only."""
    rng = np.random.default_rng(seed)
    start = dt.datetime(2023, 1, 1)
    secs = np.sort(rng.integers(0, 12 * 86400, n))
    pick = [start + dt.timedelta(seconds=int(s)) for s in secs]
    drop = [p + dt.timedelta(seconds=int(x)) for p, x in zip(pick, rng.integers(60, 3600, n))]
    fare = np.round(rng.uniform(3, 80, n), 2)
    tip = np.round(rng.uniform(0, 15, n), 2)
    return pa.table({
        "rid": pa.array(np.arange(n, dtype=np.int64) + 2_000_000_000),
        "VendorID": pa.array(rng.integers(1, 3, n).astype(np.int64)),
        "tpep_pickup_datetime": pa.array(pick, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(drop, pa.timestamp("us")),
        "trip_distance": pa.array(np.round(rng.uniform(0.1, 20, n), 2)),
        "PULocationID": pa.array(rng.integers(1, 265, n).astype(np.int64)),
        "DOLocationID": pa.array(rng.integers(1, 265, n).astype(np.int64)),
        "payment_type": pa.array(rng.integers(1, 5, n).astype(np.int64)),
        "fare_amount": pa.array(fare),
        "tip_amount": pa.array(tip),
        "total_amount": pa.array(np.round(fare + tip + 1.0, 2)),
    })
