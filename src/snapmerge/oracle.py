"""Exact serial-order oracle (independent of Iceberg's physical state).

Ground truth is computed from the LOGICAL scenario only: the generator's base rows and the
logical operations, applied as SQL in DuckDB (DELETE ... WHERE <predicate>; INSERT rows).
  rows(A;C) = apply(ops_A then ops_C, base)      rows(C;A) = apply(ops_C then ops_A, base)
A scenario is a TRUE CONFLICT iff the two orders differ as row multisets (EXCEPT ALL in both
directions is non-empty).  Merged snapshots are read back through BOTH DuckDB's iceberg_scan
and a PyIceberg scan and compared with both orders by EXCEPT ALL.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass

import pyarrow as pa

from . import catalog as C
from . import expr as X
from .writer import LogicalOp

_ids = itertools.count()


@dataclass
class Diff:
    only_left: int
    only_right: int

    @property
    def equal(self) -> bool:
        return self.only_left == 0 and self.only_right == 0

    def to_json(self) -> dict:
        return {"only_left": self.only_left, "only_right": self.only_right}


class Oracle:
    def __init__(self, memory_limit_mb: int | None = None, con=None):
        self.con = con or C.duckdb_connect(memory_limit_mb)
        self._schema: pa.Schema | None = None

    def close(self):
        try:
            self.con.close()
        except Exception:
            pass

    # ----------------------------------------------------------------------- serial application
    def apply(self, name: str, base: pa.Table, ops: list[LogicalOp]) -> str:
        con = self.con
        self._schema = base.schema
        cols = ", ".join(f'"{c}"' for c in base.schema.names)
        tmp = f"_base_{next(_ids)}"
        con.register(tmp, base)
        con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT {cols} FROM {tmp}')
        con.unregister(tmp)
        for op in ops:
            pred = op.delete_pred()
            if not X.is_false(pred):
                con.execute(f'DELETE FROM "{name}" WHERE {X.to_sql(pred, base.schema)}')
            ins = op.insert_rows()
            if ins is not None and ins.num_rows:
                tmp = f"_ins_{next(_ids)}"
                con.register(tmp, ins.select(base.schema.names).cast(base.schema))
                con.execute(f'INSERT INTO "{name}" SELECT {cols} FROM {tmp}')
                con.unregister(tmp)
        return name

    def apply_sql(self, name: str, base_sql: str, schema: pa.Schema, ops: list[LogicalOp]) -> str:
        """Like apply(), but the base rows come from a SQL relation (e.g. read_parquet(...))."""
        con = self.con
        self._schema = schema
        cols = ", ".join(f'"{c}"' for c in schema.names)
        con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT {cols} FROM {base_sql}')
        for op in ops:
            pred = op.delete_pred()
            if not X.is_false(pred):
                con.execute(f'DELETE FROM "{name}" WHERE {X.to_sql(pred, schema)}')
            ins = op.insert_rows()
            if ins is not None and ins.num_rows:
                tmp = f"_ins_{next(_ids)}"
                con.register(tmp, ins.select(schema.names).cast(schema))
                con.execute(f'INSERT INTO "{name}" SELECT {cols} FROM {tmp}')
                con.unregister(tmp)
        return name

    def load_reader(self, name: str, reader) -> str:
        """Materialise a streaming Arrow reader (e.g. a PyIceberg batch reader) as a table."""
        tmp = f"_rd_{next(_ids)}"
        self.con.register(tmp, reader)
        cols = ", ".join(f'"{c}"' for c in self._schema.names)
        self.con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT {cols} FROM {tmp}')
        self.con.unregister(tmp)
        return name

    def serial(self, base: pa.Table, ops_a: list[LogicalOp], ops_c: list[LogicalOp]) -> dict:
        self.apply("o_ac", base, list(ops_a) + list(ops_c))
        self.apply("o_ca", base, list(ops_c) + list(ops_a))
        self.apply("o_a", base, ops_a)
        self.apply("o_c", base, ops_c)
        d = self.diff_tables("o_ac", "o_ca")
        return {"true_conflict": not d.equal, "ac_vs_ca": d.to_json(),
                "rows_ac": self.count("o_ac"), "rows_ca": self.count("o_ca"), "rows_base": base.num_rows}

    # ----------------------------------------------------------------------- comparisons
    def count(self, name: str) -> int:
        return self.con.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]

    def diff_tables(self, left: str, right: str) -> Diff:
        cols = ", ".join(f'"{c}"' for c in self._schema.names)
        a = self.con.execute(f'SELECT count(*) FROM (SELECT {cols} FROM "{left}" EXCEPT ALL '
                             f'SELECT {cols} FROM "{right}")').fetchone()[0]
        b = self.con.execute(f'SELECT count(*) FROM (SELECT {cols} FROM "{right}" EXCEPT ALL '
                             f'SELECT {cols} FROM "{left}")').fetchone()[0]
        return Diff(a, b)

    def load(self, name: str, rows: pa.Table) -> str:
        tmp = f"_ld_{next(_ids)}"
        self.con.register(tmp, rows.select(self._schema.names).cast(self._schema))
        cols = ", ".join(f'"{c}"' for c in self._schema.names)
        self.con.execute(f'CREATE OR REPLACE TABLE "{name}" AS SELECT {cols} FROM {tmp}')
        self.con.unregister(tmp)
        return name

    def compare(self, rows: pa.Table, expected: str) -> Diff:
        self.load("_cmp", rows)
        return self.diff_tables("_cmp", expected)

    def check_snapshot(self, table, snapshot_id: int, expected: list[str]) -> dict:
        """Read a snapshot through both readers; compare each with each expected table."""
        out = {}
        r_duck = C.scan_duckdb(self.con, table, snapshot_id)
        r_py = C.scan_pyiceberg(table, snapshot_id)
        out["rows_duckdb"] = r_duck.num_rows
        out["rows_pyiceberg"] = r_py.num_rows
        self.load("_r_duck", r_duck)
        self.load("_r_py", r_py)
        out["readers_agree"] = self.diff_tables("_r_duck", "_r_py").equal
        for e in expected:
            d = self.diff_tables("_r_duck", e)
            out[f"duckdb_eq_{e}"] = d.equal
            out[f"pyiceberg_eq_{e}"] = self.diff_tables("_r_py", e).equal
            out[f"vs_{e}"] = d.to_json()  # rows only in merged (dup/extra) / only in expected (lost)
        return out
