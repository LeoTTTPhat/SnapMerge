"""Data sources: TPC-H (generated locally with DuckDB's tpch extension) and NYC TLC yellow taxi.

Prepared files (written by scripts/prepare_tpch.py and scripts/fetch_nyc_tlc.py):
  data/tpch/sf{SF}/lineitem.parquet   rid = l_orderkey*8 + l_linenumber; decimals cast to DOUBLE
  data/tpch/sf{SF}/orders.parquet     rid = o_orderkey
  data/nyc/yellow_tripdata_{YYYY-MM}.clean.parquet   rid = month_index*10^8 + row number
Every filterable column is NOT NULL (asserted at preparation), so SQL, Arrow and Iceberg agree on
predicate semantics.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[2]
DATA = PROJECT / "data"


@dataclass(frozen=True)
class SourceSpec:
    name: str
    partition: tuple[str, str]                 # (time column, transform: month | day)
    key: str
    numeric: tuple[str, ...]                   # range-predicate columns
    categorical: tuple[str, ...]               # equality-predicate columns
    mutable_numeric: tuple[str, ...]           # columns corrected by OVERWRITE / UPSERT
    sort_cols: tuple[str, ...]                 # compaction sort orders

    @property
    def time_col(self) -> str:
        return self.partition[0]


LINEITEM = SourceSpec(
    name="tpch_lineitem", partition=("l_shipdate", "month"), key="rid",
    numeric=("l_quantity", "l_discount", "l_extendedprice", "l_tax", "l_partkey"),
    categorical=("l_returnflag", "l_linestatus", "l_shipmode", "l_shipinstruct"),
    mutable_numeric=("l_discount", "l_tax", "l_quantity", "l_extendedprice"),
    sort_cols=("l_partkey", "l_quantity", "l_extendedprice", "l_receiptdate"),
)

ORDERS = SourceSpec(
    name="tpch_orders", partition=("o_orderdate", "month"), key="rid",
    numeric=("o_totalprice", "o_custkey", "o_shippriority"),
    categorical=("o_orderstatus", "o_orderpriority"),
    mutable_numeric=("o_totalprice",),
    sort_cols=("o_custkey", "o_totalprice"),
)

NYC = SourceSpec(
    name="nyc_yellow", partition=("tpep_pickup_datetime", "day"), key="rid",
    numeric=("trip_distance", "fare_amount", "tip_amount", "total_amount", "PULocationID", "DOLocationID"),
    categorical=("VendorID", "payment_type"),
    mutable_numeric=("fare_amount", "tip_amount", "total_amount"),
    sort_cols=("PULocationID", "fare_amount", "trip_distance"),
)

SPECS = {"tpch_lineitem": LINEITEM, "tpch_orders": ORDERS, "nyc_yellow": NYC}

LINEITEM_SQL = """SELECT CAST(l_orderkey AS BIGINT) * 8 + l_linenumber AS rid,
  CAST(l_orderkey AS BIGINT) AS l_orderkey, CAST(l_partkey AS BIGINT) AS l_partkey,
  CAST(l_suppkey AS BIGINT) AS l_suppkey, CAST(l_linenumber AS BIGINT) AS l_linenumber,
  CAST(l_quantity AS DOUBLE) AS l_quantity, CAST(l_extendedprice AS DOUBLE) AS l_extendedprice,
  CAST(l_discount AS DOUBLE) AS l_discount, CAST(l_tax AS DOUBLE) AS l_tax,
  l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode
FROM lineitem ORDER BY rid"""

ORDERS_SQL = """SELECT CAST(o_orderkey AS BIGINT) AS rid, CAST(o_custkey AS BIGINT) AS o_custkey,
  o_orderstatus, CAST(o_totalprice AS DOUBLE) AS o_totalprice, o_orderdate, o_orderpriority, o_clerk,
  CAST(o_shippriority AS BIGINT) AS o_shippriority
FROM orders ORDER BY rid"""


def _not_null(t: pa.Table) -> pa.Table:
    """Drop Arrow-level nullability flags (Iceberg optional fields) but assert no NULL values."""
    for name in t.schema.names:
        if t.column(name).null_count:
            raise ValueError(f"column {name} contains NULLs")
    return t


def generate_tpch(sf: float, table: str = "lineitem") -> pa.Table:
    """Generate TPC-H in memory with DuckDB's tpch extension (dbgen)."""
    import duckdb

    con = duckdb.connect()
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")
    sql = LINEITEM_SQL if table == "lineitem" else ORDERS_SQL
    t = con.execute(sql).to_arrow_table()
    con.close()
    return _not_null(t)


def tpch_path(sf: float, table: str = "lineitem", data_dir: Path = DATA) -> Path:
    return Path(data_dir) / "tpch" / f"sf{sf:g}" / f"{table}.parquet"


def nyc_path(month: str, data_dir: Path = DATA) -> Path:
    return Path(data_dir) / "nyc" / f"yellow_tripdata_{month}.clean.parquet"


def source_paths(source: str, sf: float = 0.1, months: tuple[str, ...] = ("2023-01",),
                 data_dir: Path = DATA) -> list[Path]:
    """The prepared files a source reads (for provenance / fingerprints)."""
    if source.startswith("tpch"):
        return [tpch_path(sf, source.split("_", 1)[1], data_dir)]
    return [nyc_path(m, data_dir) for m in months]


def load_source(source: str, data_dir: Path = DATA, sf: float = 0.1, months: tuple[str, ...] = ("2023-01",),
                allow_generate: bool = False) -> tuple[SourceSpec, pa.Table]:
    """Load a prepared source table.  `source` in tpch_lineitem | tpch_orders | nyc_yellow."""
    spec = SPECS[source]
    if source.startswith("tpch"):
        table = source.split("_", 1)[1]
        p = tpch_path(sf, table, data_dir)
        if p.exists():
            return spec, _not_null(pq.read_table(p))
        if not allow_generate:
            raise FileNotFoundError(f"{p} missing; run scripts/prepare_tpch.py --sf {sf:g}")
        return spec, generate_tpch(sf, table)
    parts = []
    for m in months:
        p = nyc_path(m, data_dir)
        if not p.exists():
            raise FileNotFoundError(f"{p} missing; run scripts/fetch_nyc_tlc.py --months {m}")
        parts.append(pq.read_table(p))
    return spec, _not_null(pa.concat_tables(parts))


# ----------------------------------------------------------------------------- partitions

def partition_ids(spec: SourceSpec, t: pa.Table) -> np.ndarray:
    col, transform = spec.partition
    arr = t.column(col)
    if transform == "month":
        y = pc.year(arr).to_numpy(zero_copy_only=False).astype(np.int64)
        m = pc.month(arr).to_numpy(zero_copy_only=False).astype(np.int64)
        return (y - 1970) * 12 + (m - 1)
    if transform == "day":
        a = arr.cast(pa.timestamp("us")) if pa.types.is_timestamp(arr.type) else arr
        d = pc.cast(pc.cast(a, pa.timestamp("us")), pa.int64()).to_numpy(zero_copy_only=False) // 86_400_000_000
        return d.astype(np.int64)
    raise ValueError(transform)


def partition_bounds(spec: SourceSpec, pid: int, arrow_type: pa.DataType):
    """[lo, hi) of the time column for partition id `pid`, as Python values of the column type."""
    _, transform = spec.partition
    if transform == "month":
        y, m = divmod(int(pid), 12)
        lo = dt.date(1970 + y, m + 1, 1)
        hi = dt.date(1970 + y + (m + 1) // 12, (m + 1) % 12 + 1, 1)
    else:
        lo = dt.date(1970, 1, 1) + dt.timedelta(days=int(pid))
        hi = lo + dt.timedelta(days=1)
    if pa.types.is_timestamp(arrow_type):
        lo = dt.datetime.combine(lo, dt.time())
        hi = dt.datetime.combine(hi, dt.time())
    return lo, hi
