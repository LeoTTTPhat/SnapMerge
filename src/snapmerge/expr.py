"""Filter AST used to record logical operations.

A filter is a JSON-serialisable dict:
  {"op": "true"} | {"op": "false"}
  {"op": "cmp", "col": c, "cmp": "<"|"<="|">"|">="|"="|"!=", "val": v}
  {"op": "in", "col": c, "vals": [v, ...]}
  {"op": "and", "args": [f, ...]} | {"op": "or", "args": [f, ...]} | {"op": "not", "arg": f}

Literal values are JSON scalars; dates and timestamps are ISO strings and are typed by the
column's Arrow type.  Three independent backends are provided:
  * to_iceberg  -> pyiceberg BooleanExpression (writer deletes, inclusive-metrics evaluation)
  * to_arrow    -> pyarrow.compute Expression (bounded exact reads in the rules)
  * to_sql      -> DuckDB SQL text (the oracle)

Semantics (2026-10-05, review F5).  A recorded predicate f denotes the TOTAL Boolean function
P_f(v) = [f(v) IS TRUE] under SQL three-valued logic, and "not P_f" is its Boolean complement
(f IS NOT TRUE).  A delete removes exactly the rows with P_f; NULLs never satisfy a comparison.
  * eval_arrow fills NULL results with False, so it computes P_f exactly;
  * to_sql is used only in WHERE clauses (DELETE ... WHERE f), which is P_f;
  * PyIceberg's copy-on-write delete keeps every row with a NULL in a referenced column, which
    equals P_f only for conjunctions of comparisons (NULL OR TRUE is TRUE in SQL), and its strict
    metrics evaluator treats NULL != v as true.  Writers therefore hand PyIceberg is_true_form(f),
    a two-valued formula equivalent to "f IS TRUE" in which every comparison is guarded by
    IS NOT NULL;
  * inclusive-metrics pruning with to_iceberg(f) is safe for P_f: a row on which f is TRUE in
    three-valued logic satisfies f under any completion of its unknown comparisons, including
    Iceberg's, which the inclusive evaluator over-approximates.
The prepared TPC-H and NYC sources have no NULLs in filterable columns; the nullable regression
tests (tests/test_nullable.py) exercise all backends on NULL-bearing rows.
"""
from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.compute as pc

TRUE = {"op": "true"}
FALSE = {"op": "false"}
_CMPS = ("<", "<=", ">", ">=", "=", "!=")


# ----------------------------------------------------------------------------- constructors

def cmp(col: str, op: str, val: Any) -> dict:
    if op not in _CMPS:
        raise ValueError(f"bad comparison {op}")
    return {"op": "cmp", "col": col, "cmp": op, "val": _jsonable(val)}


def isin(col: str, vals: Iterable[Any]) -> dict:
    vals = [_jsonable(v) for v in vals]
    if not vals:
        return dict(FALSE)
    return {"op": "in", "col": col, "vals": vals}


def and_(*args: dict) -> dict:
    args = [a for a in args if a.get("op") != "true"]
    if any(a.get("op") == "false" for a in args):
        return dict(FALSE)
    if not args:
        return dict(TRUE)
    if len(args) == 1:
        return args[0]
    return {"op": "and", "args": list(args)}


def or_(*args: dict) -> dict:
    args = [a for a in args if a.get("op") != "false"]
    if any(a.get("op") == "true" for a in args):
        return dict(TRUE)
    if not args:
        return dict(FALSE)
    if len(args) == 1:
        return args[0]
    return {"op": "or", "args": list(args)}


def not_(arg: dict) -> dict:
    if arg.get("op") == "true":
        return dict(FALSE)
    if arg.get("op") == "false":
        return dict(TRUE)
    return {"op": "not", "arg": arg}


def between(col: str, lo: Any, hi: Any) -> dict:
    return and_(cmp(col, ">=", lo), cmp(col, "<=", hi))


def not_null(col: str) -> dict:
    return {"op": "notnull", "col": col}


def is_null(col: str) -> dict:
    return {"op": "isnull", "col": col}


def is_true_form(f: dict) -> dict:
    """Two-valued formula equivalent to "f IS TRUE" (SQL three-valued logic): every comparison is
    guarded by IS NOT NULL on its column, and negation is pushed to the leaves with the matching
    "IS FALSE" form.  The result never evaluates to NULL in any backend."""
    return _truth_form(f, True)


def _truth_form(f: dict, want: bool) -> dict:
    op = f["op"]
    if op in ("true", "false"):
        return dict(TRUE) if (op == "true") == want else dict(FALSE)
    if op in ("cmp", "in"):
        leaf = f if want else _negate_leaf(f)
        return and_(not_null(f["col"]), leaf)
    if op in ("notnull", "isnull"):  # already two-valued
        return f if want else {"op": "isnull" if op == "notnull" else "notnull", "col": f["col"]}
    if op == "not":
        return _truth_form(f["arg"], not want)
    parts = [_truth_form(a, want) for a in f["args"]]
    if op == "and":
        return and_(*parts) if want else or_(*parts)
    if op == "or":
        return or_(*parts) if want else and_(*parts)
    raise ValueError(f"unknown filter op {op}")


def _negate_leaf(f: dict) -> dict:
    """Negation of a comparison on a NON-NULL value (the caller adds the IS NOT NULL guard)."""
    if f["op"] == "cmp":
        inv = {"<": ">=", "<=": ">", ">": "<=", ">=": "<", "=": "!=", "!=": "="}[f["cmp"]]
        return {"op": "cmp", "col": f["col"], "cmp": inv, "val": f["val"]}
    return {"op": "not", "arg": f}


def is_false(f: dict | None) -> bool:
    return f is None or f.get("op") == "false"


def is_true(f: dict | None) -> bool:
    return f is not None and f.get("op") == "true"


def dumps(f: dict) -> str:
    return json.dumps(f, sort_keys=True, separators=(",", ":"))


def loads(s: str | None) -> dict | None:
    return None if s is None else json.loads(s)


def columns(f: dict) -> set[str]:
    op = f["op"]
    if op in ("cmp", "in", "notnull", "isnull"):
        return {f["col"]}
    if op in ("and", "or"):
        out: set[str] = set()
        for a in f["args"]:
            out |= columns(a)
        return out
    if op == "not":
        return columns(f["arg"])
    return set()


def _jsonable(v: Any) -> Any:
    if isinstance(v, _dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, _dt.date):
        return v.isoformat()
    if hasattr(v, "item"):  # numpy scalar
        return v.item()
    return v


# ----------------------------------------------------------------------------- typing helpers

def _py_value(v: Any, typ: pa.DataType) -> Any:
    """Convert a JSON literal into the Python value matching an Arrow type."""
    if pa.types.is_date(typ):
        return v if isinstance(v, _dt.date) else _dt.date.fromisoformat(v)
    if pa.types.is_timestamp(typ):
        return v if isinstance(v, _dt.datetime) else _dt.datetime.fromisoformat(v)
    if pa.types.is_floating(typ):
        return float(v)
    if pa.types.is_integer(typ):
        return int(v)
    if pa.types.is_boolean(typ):
        return bool(v)
    return str(v)


def _field_type(schema: pa.Schema, col: str) -> pa.DataType:
    return schema.field(col).type


# ----------------------------------------------------------------------------- Iceberg backend

def to_iceberg(f: dict, schema: pa.Schema):
    from pyiceberg import expressions as E

    op = f["op"]
    if op == "true":
        return E.AlwaysTrue()
    if op == "false":
        return E.AlwaysFalse()
    if op == "cmp":
        v = _py_value(f["val"], _field_type(schema, f["col"]))
        c = f["col"]
        return {
            "<": E.LessThan, "<=": E.LessThanOrEqual, ">": E.GreaterThan,
            ">=": E.GreaterThanOrEqual, "=": E.EqualTo, "!=": E.NotEqualTo,
        }[f["cmp"]](c, v)
    if op == "in":
        typ = _field_type(schema, f["col"])
        vals = [_py_value(v, typ) for v in f["vals"]]
        if len(vals) == 1:
            return E.EqualTo(f["col"], vals[0])
        return E.In(f["col"], vals)
    if op == "notnull":
        return E.NotNull(f["col"])
    if op == "isnull":
        return E.IsNull(f["col"])
    if op == "and":
        out = to_iceberg(f["args"][0], schema)
        for a in f["args"][1:]:
            out = E.And(out, to_iceberg(a, schema))
        return out
    if op == "or":
        out = to_iceberg(f["args"][0], schema)
        for a in f["args"][1:]:
            out = E.Or(out, to_iceberg(a, schema))
        return out
    if op == "not":
        return E.Not(to_iceberg(f["arg"], schema))
    raise ValueError(f"unknown filter op {op}")


# ----------------------------------------------------------------------------- Arrow backend

def to_arrow(f: dict, schema: pa.Schema) -> pc.Expression:
    op = f["op"]
    if op == "true":
        return pc.scalar(True)
    if op == "false":
        return pc.scalar(False)
    if op == "cmp":
        typ = _field_type(schema, f["col"])
        lit = pa.scalar(_py_value(f["val"], typ), type=typ)
        fld = pc.field(f["col"])
        return {
            "<": fld < lit, "<=": fld <= lit, ">": fld > lit,
            ">=": fld >= lit, "=": fld == lit, "!=": fld != lit,
        }[f["cmp"]]
    if op == "in":
        typ = _field_type(schema, f["col"])
        arr = pa.array([_py_value(v, typ) for v in f["vals"]], type=typ)
        # isin() returns false (not NULL) for a NULL input; the SQL result is NULL.  Both are "not
        # true", so P_f is unaffected; NOT(x IN S) is made NULL-correct by the explicit guard.
        return pc.if_else(pc.field(f["col"]).is_valid(), pc.field(f["col"]).isin(arr),
                          pc.scalar(pa.scalar(None, pa.bool_())))
    if op == "notnull":
        return pc.field(f["col"]).is_valid()
    if op == "isnull":
        return pc.field(f["col"]).is_null()
    if op == "and":
        out = to_arrow(f["args"][0], schema)
        for a in f["args"][1:]:
            out = out & to_arrow(a, schema)
        return out
    if op == "or":
        out = to_arrow(f["args"][0], schema)
        for a in f["args"][1:]:
            out = out | to_arrow(a, schema)
        return out
    if op == "not":
        return ~to_arrow(f["arg"], schema)
    raise ValueError(f"unknown filter op {op}")


def eval_arrow(f: dict, table: pa.Table) -> pa.Array:
    """Boolean mask (no nulls) of rows of `table` satisfying f."""
    if table.num_rows == 0:
        return pa.array([], pa.bool_())
    if f["op"] == "true":
        return pa.array([True] * table.num_rows)
    if f["op"] == "false":
        return pa.array([False] * table.num_rows)
    import pyarrow.dataset as ds

    expr = to_arrow(f, table.schema)
    mask = ds.dataset(table).to_table(columns={"m": expr}).column("m").combine_chunks()
    return pc.fill_null(mask, False)


# ----------------------------------------------------------------------------- SQL backend

def _sql_lit(v: Any, typ: pa.DataType) -> str:
    v = _py_value(v, typ)
    if pa.types.is_date(typ):
        return f"DATE '{v.isoformat()}'"
    if pa.types.is_timestamp(typ):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if pa.types.is_floating(typ):
        return f"CAST({float(v)!r} AS DOUBLE)"
    if pa.types.is_integer(typ):
        return str(int(v))
    if pa.types.is_boolean(typ):
        return "TRUE" if v else "FALSE"
    return "'" + str(v).replace("'", "''") + "'"


def _ident(c: str) -> str:
    return '"' + c.replace('"', '""') + '"'


def to_sql(f: dict, schema: pa.Schema) -> str:
    op = f["op"]
    if op == "true":
        return "TRUE"
    if op == "false":
        return "FALSE"
    if op == "cmp":
        typ = _field_type(schema, f["col"])
        sym = "<>" if f["cmp"] == "!=" else f["cmp"]
        return f"({_ident(f['col'])} {sym} {_sql_lit(f['val'], typ)})"
    if op == "in":
        typ = _field_type(schema, f["col"])
        return f"({_ident(f['col'])} IN ({', '.join(_sql_lit(v, typ) for v in f['vals'])}))"
    if op == "notnull":
        return f"({_ident(f['col'])} IS NOT NULL)"
    if op == "isnull":
        return f"({_ident(f['col'])} IS NULL)"
    if op == "and":
        return "(" + " AND ".join(to_sql(a, schema) for a in f["args"]) + ")"
    if op == "or":
        return "(" + " OR ".join(to_sql(a, schema) for a in f["args"]) + ")"
    if op == "not":
        return f"(NOT {to_sql(f['arg'], schema)})"
    raise ValueError(f"unknown filter op {op}")


def describe(f: dict) -> str:
    """Short human-readable rendering (for witnesses)."""
    op = f["op"]
    if op in ("true", "false"):
        return op.upper()
    if op == "cmp":
        return f"{f['col']} {f['cmp']} {f['val']}"
    if op in ("notnull", "isnull"):
        return f"{f['col']} IS {'NOT ' if op == 'notnull' else ''}NULL"
    if op == "in":
        vals = f["vals"]
        shown = ", ".join(map(str, vals[:5])) + (", ..." if len(vals) > 5 else "")
        return f"{f['col']} IN ({shown})"
    if op == "and":
        return "(" + " AND ".join(describe(a) for a in f["args"]) + ")"
    if op == "or":
        return "(" + " OR ".join(describe(a) for a in f["args"]) + ")"
    return f"NOT {describe(f['arg'])}"
