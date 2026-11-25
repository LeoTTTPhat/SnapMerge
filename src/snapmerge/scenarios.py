"""Seeded scenario generator.

A scenario = base rows + a sequence of 1-6 logical operations per branch (A = target/main,
C = source).  Everything is a deterministic function of (source table, mix, seed, base_rows).
Ops are generated against each side's own logical state (tracked with Arrow), so corrections and
upserts refer to rows that exist on that side.

Mixes (fixed before any campaign; equal weights in difftest):
  partition_disjoint_ingest  appends into (mostly) disjoint partitions, occasional partition-local
                             delete / replace-partitions overwrite
  filter_corrections         copy-on-write OVERWRITE corrections, deletes, replace-partitions, appends
  upsert_keys                UPSERTs (cow / mor) of 3-29 random live rows plus 0-9 fresh rows, plus
                             appends; each of C's upserts additionally reuses up to 3 of A's upserted
                             keys with probability 0.3.  Random key collisions also make sides
                             overlap: about 49% of the main TPC-H scenarios (2,000-row base) are true
                             conflicts (review/conflict_share.py, 2026-10-02)
  deletes_mor                merge-on-read DELETEs on both sides (often the same files), plus appends
  deletes_cow                copy-on-write DELETEs on both sides (often the same files), plus appends
  compaction_on_main         A compacts 1-2 partitions; C deletes / upserts / appends in them
  adversarial_overlap        templates built to break naive policies: inserted rows forced to match
                             the other side's predicate (including predicates whose base matches lie
                             in other partitions), shared upsert keys, masking deletes, duplicates,
                             compaction vs overwrite in one partition

Sensitivity knobs (2026-10-05, review R3 MC3 / G3).  Defaults reproduce the frozen generator draw
for draw (a knob at its default consumes no extra random numbers):
  n_parts    number of consecutive partitions a scenario spans (default 4)
  hist       multiplies the upper bound of every per-branch operation count (default 1: 1-4 ops)
  sel        multiplies the selectivity of range predicates and time windows (default 1.0)
  compact_p  probability that the target additionally compacts 1-2 of the scenario's partitions
             as its last operation, in every mix (default 0.0: never)
"""
from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from . import expr as X
from .sources import SourceSpec, partition_bounds, partition_ids
from .writer import LogicalOp

MIXES = ("partition_disjoint_ingest", "filter_corrections", "upsert_keys", "deletes_mor", "deletes_cow",
         "compaction_on_main", "adversarial_overlap")


def scenario_seed(seed_base: int, source: str, mix: str, idx: int) -> int:
    h = hashlib.sha256(f"{seed_base}|{source}|{mix}|{idx}".encode()).hexdigest()
    return int(h[:15], 16)


@dataclass
class Scenario:
    scenario_id: str
    seed: int
    mix: str
    source: str
    base: pa.Table
    ops: dict[str, list[LogicalOp]]
    meta: dict = field(default_factory=dict)

    def summary(self) -> dict:
        return {"scenario_id": self.scenario_id, "seed": self.seed, "mix": self.mix, "source": self.source,
                "base_rows": self.base.num_rows,
                "ops": {s: [o.summary() for o in ops] for s, ops in self.ops.items()}, "meta": self.meta}


class SourcePool:
    """Source table + partition ids, computed once per process."""

    def __init__(self, spec: SourceSpec, table: pa.Table):
        self.spec = spec
        self.table = table
        self.pids = partition_ids(spec, table)
        uniq, counts = np.unique(self.pids, return_counts=True)
        self.parts = uniq
        self.counts = counts
        self.rows_of = {int(p): np.flatnonzero(self.pids == p) for p in uniq}


def _round(v: float) -> float:
    return float(round(v, 2))


class _Gen:
    def __init__(self, pool: SourcePool, mix: str, seed: int, base_rows: int, n_parts: int = 4,
                 hist: int = 1, sel: float = 1.0):
        self.pool, self.spec, self.mix = pool, pool.spec, mix
        self.hist, self.sel = int(hist), float(sel)
        self.rng = np.random.default_rng(seed)
        rng = self.rng
        need = max(base_rows // n_parts + 50, 60)
        eligible = [i for i in range(len(pool.parts) - n_parts) if
                    all(pool.counts[i + k] >= need for k in range(n_parts + 1))]
        if not eligible:
            eligible = list(range(max(1, len(pool.parts) - n_parts)))
        start = int(rng.choice(eligible))
        self.parts = [int(p) for p in pool.parts[start:start + n_parts]]
        extra_i = start + n_parts if start + n_parts < len(pool.parts) else max(0, start - 1)
        self.extra_part = int(pool.parts[extra_i])
        idx = np.concatenate([pool.rows_of[p] for p in self.parts])
        base_idx = np.sort(rng.choice(idx, size=min(base_rows, idx.size), replace=False))
        rest = np.setdiff1d(np.concatenate([idx, pool.rows_of[self.extra_part]]), base_idx)
        rest = rng.permutation(rest)
        half = rest.size // 2
        self.fresh = {"A": list(rest[:half]), "C": list(rest[half:])}
        self.base = pool.table.take(pa.array(base_idx)).sort_by(self.spec.key)
        self.schema = self.base.schema
        self.state = {"A": self.base, "C": self.base}
        self.ops: dict[str, list[LogicalOp]] = {"A": [], "C": []}
        self.time_type = self.schema.field(self.spec.time_col).type
        self.meta: dict = {}

    # ------------------------------------------------------------------ helpers
    def pid_of(self, t: pa.Table) -> np.ndarray:
        return partition_ids(self.spec, t)

    def part_pred(self, pid: int) -> dict:
        lo, hi = partition_bounds(self.spec, pid, self.time_type)
        return X.and_(X.cmp(self.spec.time_col, ">=", lo), X.cmp(self.spec.time_col, "<", hi))

    def take_fresh(self, side: str, n: int, parts: list[int] | None = None) -> pa.Table:
        pool = self.fresh[side]
        if parts is not None:
            pids = self.pool.pids
            picked = [i for i in pool if int(pids[i]) in parts][:n]
        else:
            picked = pool[:n]
        chosen = set(int(i) for i in picked)
        self.fresh[side] = [i for i in pool if int(i) not in chosen]
        if not picked:
            return self.schema.empty_table()
        return self.pool.table.take(pa.array(picked)).select(self.schema.names)

    def correct(self, rows: pa.Table) -> pa.Table:
        """Deterministic value correction of a mutable numeric column (key and partition kept)."""
        if rows.num_rows == 0:
            return rows
        col = str(self.rng.choice(self.spec.mutable_numeric))
        vals = rows.column(col).to_numpy(zero_copy_only=False).astype(float)
        factor = float(self.rng.choice([0.9, 1.05, 1.1, 1.25]))
        new = np.round(vals * factor + 0.01, 2)
        typ = rows.schema.field(col).type
        arr = pa.array(new).cast(typ) if pa.types.is_floating(typ) else pa.array(np.round(new).astype(np.int64)).cast(typ)
        return rows.set_column(rows.schema.get_field_index(col), col, arr)

    def apply_state(self, side: str, op: LogicalOp) -> None:
        st = self.state[side]
        pred = op.delete_pred()
        if not X.is_false(pred):
            mask = X.eval_arrow(pred, st)
            st = st.filter(pc.invert(mask))
        ins = op.insert_rows()
        if ins is not None and ins.num_rows:
            st = pa.concat_tables([st, ins.select(self.schema.names).cast(self.schema)])
        self.state[side] = st

    def add(self, side: str, op: LogicalOp) -> LogicalOp:
        if op.rows is not None:
            op.rows = op.rows.select(self.schema.names).cast(self.schema)
        self.apply_state(side, op)
        self.ops[side].append(op)
        return op

    # ------------------------------------------------------------------ predicates
    def num_range(self, side: str, sel_lo=0.02, sel_hi=0.25, rows: pa.Table | None = None) -> dict:
        st = rows if rows is not None else self.state[side]
        col = str(self.rng.choice(self.spec.numeric))
        v = np.sort(st.column(col).to_numpy(zero_copy_only=False).astype(float)) if st.num_rows else np.array([0.0])
        sel = float(self.rng.uniform(sel_lo, sel_hi))
        if self.sel != 1.0:
            sel = min(0.9, sel * self.sel)
        q1 = float(self.rng.uniform(0, 1 - sel))
        lo, hi = np.quantile(v, q1), np.quantile(v, q1 + sel)
        typ = self.schema.field(col).type
        if pa.types.is_integer(typ):
            return X.between(col, int(np.floor(lo)), int(np.ceil(hi)))
        return X.between(col, _round(lo), _round(hi))

    def cat_eq(self, side: str) -> dict:
        st = self.state[side]
        col = str(self.rng.choice(self.spec.categorical))
        vals = st.column(col).to_pylist() or [None]
        v = vals[int(self.rng.integers(len(vals)))]
        return X.cmp(col, "=", v)

    def time_window(self) -> dict:
        lo, _ = partition_bounds(self.spec, self.parts[0], self.time_type)
        _, hi = partition_bounds(self.spec, self.parts[-1], self.time_type)
        span = (hi - lo)
        start = lo + span * float(self.rng.uniform(0, 0.8))
        width = span * float(self.rng.uniform(0.05, 0.25))
        if self.sel != 1.0:
            width = min(span * 0.9, width * self.sel)
        a, b = start, start + width
        if isinstance(a, dt.datetime):
            a, b = a.replace(microsecond=0), b.replace(microsecond=0)
        elif isinstance(a, dt.date):
            pass
        return X.and_(X.cmp(self.spec.time_col, ">=", a), X.cmp(self.spec.time_col, "<", b))

    def rare(self, side: str) -> dict:
        """Very selective predicate on two numeric columns (often no base match)."""
        return X.and_(self.num_range(side, 0.01, 0.04), self.num_range(side, 0.01, 0.06))

    def pred(self, side: str, kind: str | None = None) -> dict:
        kind = kind or str(self.rng.choice(["num", "num_in_part", "cat_in_part", "time", "part"],
                                           p=[0.25, 0.3, 0.15, 0.15, 0.15]))
        if kind == "num":
            return self.num_range(side)
        if kind == "num_in_part":
            p = int(self.rng.choice(self.parts))
            return X.and_(self.part_pred(p), self.num_range(side))
        if kind == "cat_in_part":
            p = int(self.rng.choice(self.parts))
            return X.and_(self.part_pred(p), self.cat_eq(side))
        if kind == "time":
            return self.time_window()
        if kind == "part":
            return self.part_pred(int(self.rng.choice(self.parts)))
        if kind == "rare":
            return self.rare(side)
        raise ValueError(kind)

    # ------------------------------------------------------------------ operations
    def op_append(self, side: str, parts: list[int] | None = None, n: int | None = None) -> LogicalOp:
        n = n or int(self.rng.integers(5, 60))
        rows = self.take_fresh(side, n, parts)
        return self.add(side, LogicalOp("APPEND", rows=rows))

    def op_delete(self, side: str, mode: str = "cow", kind: str | None = None, pred: dict | None = None) -> LogicalOp:
        return self.add(side, LogicalOp("DELETE", filter=pred or self.pred(side, kind), mode=mode))

    def op_overwrite(self, side: str, mode: str = "cow", kind: str | None = None, pred: dict | None = None,
                     extra_new: int = 0) -> LogicalOp:
        pred = pred or self.pred(side, kind)
        st = self.state[side]
        matched = st.filter(X.eval_arrow(pred, st))
        if matched.num_rows:
            k = max(1, int(matched.num_rows * float(self.rng.uniform(0.3, 1.0))))
            take = np.sort(self.rng.choice(matched.num_rows, size=k, replace=False))
            rows = self.correct(matched.take(pa.array(take)))
        else:
            rows = self.schema.empty_table()
        if extra_new:
            rows = pa.concat_tables([rows, self.take_fresh(side, extra_new, self.parts)])
        return self.add(side, LogicalOp("OVERWRITE", filter=pred, rows=rows, mode=mode))

    def op_dynamic(self, side: str, pid: int | None = None) -> LogicalOp:
        pid = int(self.rng.choice(self.parts)) if pid is None else pid
        pred = self.part_pred(pid)
        st = self.state[side]
        cur = st.filter(X.eval_arrow(pred, st))
        rows = self.correct(cur) if cur.num_rows else self.take_fresh(side, 10, [pid])
        if rows.num_rows == 0:
            rows = self.take_fresh(side, 10, [pid])
        return self.add(side, LogicalOp("OVERWRITE", filter=pred, rows=rows, mode="cow", dynamic=True))

    def op_upsert(self, side: str, mode: str = "cow", shared: list | None = None) -> LogicalOp:
        st = self.state[side]
        key = self.spec.key
        n_old = int(self.rng.integers(3, 30))
        pick = np.sort(self.rng.choice(st.num_rows, size=min(n_old, st.num_rows), replace=False)) if st.num_rows else []
        old = st.take(pa.array(pick, pa.int64())) if len(pick) else self.schema.empty_table()
        if shared:
            extra = st.filter(pc.is_in(st.column(key), value_set=pa.array(shared, st.schema.field(key).type)))
            old = pa.concat_tables([old, extra])
            # unique by key
            _, first = np.unique(old.column(key).to_numpy(), return_index=True)
            old = old.take(pa.array(np.sort(first)))
        rows = self.correct(old) if old.num_rows else old
        new = self.take_fresh(side, int(self.rng.integers(0, 10)), self.parts)
        rows = pa.concat_tables([rows.cast(self.schema), new.cast(self.schema)])
        keys = sorted(int(k) for k in rows.column(key).to_pylist())
        return self.add(side, LogicalOp("UPSERT", rows=rows, keys=keys, mode=mode, key_col=key))

    def op_replace(self, side: str, pids: list[int] | None = None) -> LogicalOp:
        pids = pids or [int(p) for p in self.rng.choice(self.parts, size=int(self.rng.integers(1, 3)), replace=False)]
        pred = X.or_(*[self.part_pred(p) for p in sorted(pids)])
        sort_by = str(self.rng.choice(self.spec.sort_cols))
        return self.add(side, LogicalOp("REPLACE", filter=pred, sort_by=sort_by))

    def force_match(self, rows: pa.Table, pred: dict) -> pa.Table | None:
        """Modify rows so that every row satisfies `pred` (conjunctions of comparisons only)."""
        sets: dict[str, object] = {}

        def visit(f):
            op = f["op"]
            if op == "and":
                return all(visit(a) for a in f["args"])
            if op == "or":
                return visit(f["args"][0])
            if op == "in":
                sets[f["col"]] = f["vals"][0]
                return True
            if op == "cmp":
                col, c, v = f["col"], f["cmp"], f["val"]
                typ = self.schema.field(col).type
                if c in ("=", ">=", "<="):
                    sets.setdefault(col, v)
                    return True
                if c in (">", "<"):
                    sgn = 1 if c == ">" else -1
                    if pa.types.is_floating(typ):
                        sets[col] = _round(float(v) + 0.01 * sgn)
                    elif pa.types.is_integer(typ):
                        sets[col] = int(v) + sgn
                    elif pa.types.is_date(typ):
                        sets[col] = (dt.date.fromisoformat(v) + dt.timedelta(days=sgn)).isoformat()
                    elif pa.types.is_timestamp(typ):
                        sets[col] = (dt.datetime.fromisoformat(v) + dt.timedelta(seconds=sgn)).isoformat(sep=" ")
                    else:
                        return False
                    return True
                return False
            return False

        if not visit(pred) or rows.num_rows == 0:
            return None
        out = rows
        for col, v in sets.items():
            typ = self.schema.field(col).type
            arr = pa.array([X._py_value(v, typ)] * out.num_rows, type=typ)
            out = out.set_column(out.schema.get_field_index(col), col, arr)
        if not X.eval_arrow(pred, out).to_numpy(zero_copy_only=False).all():
            return None
        return out

    def op_append_matching(self, side: str, pred: dict, avoid_parts: set[int] | None = None) -> LogicalOp:
        parts = [p for p in self.parts + [self.extra_part] if not avoid_parts or p not in avoid_parts]
        rows = self.take_fresh(side, int(self.rng.integers(1, 6)), parts or None)
        forced = self.force_match(rows, pred)
        return self.add(side, LogicalOp("APPEND", rows=forced if forced is not None else rows))

    def base_match_parts(self, pred: dict) -> set[int]:
        m = X.eval_arrow(pred, self.base).to_numpy(zero_copy_only=False)
        return set(int(p) for p in self.pid_of(self.base)[m])

    def n_ops(self, lo: int = 1, hi: int = 6) -> int:
        """Operation count in [lo, hi * hist] (hist = 1 reproduces the frozen draws)."""
        return int(self.rng.integers(lo, hi * self.hist + 1))


# ============================================================================ mixes

def _mix_partition_disjoint(g: _Gen) -> None:
    rng = g.rng
    parts = list(g.parts)
    rng.shuffle(parts)
    if rng.random() < 0.7:
        own = {"A": parts[:2], "C": parts[2:]}
    else:
        own = {"A": parts[:3], "C": parts[1:]}
    for side in ("A", "C"):
        for _ in range(g.n_ops(1, 4)):
            r = rng.random()
            if r < 0.75:
                g.op_append(side, own[side])
            elif r < 0.9:
                p = int(rng.choice(own[side]))
                g.op_delete(side, "cow", pred=X.and_(g.part_pred(p), g.num_range(side)))
            else:
                g.op_dynamic(side, int(rng.choice(own[side])))


def _mix_filter_corrections(g: _Gen) -> None:
    rng = g.rng
    for side in ("A", "C"):
        for _ in range(g.n_ops(1, 4)):
            r = rng.random()
            if r < 0.4:
                g.op_overwrite(side, "cow", kind=str(rng.choice(["num_in_part", "cat_in_part", "num"])))
            elif r < 0.6:
                g.op_delete(side, "cow", kind=str(rng.choice(["num_in_part", "time", "num"])))
            elif r < 0.8:
                g.op_append(side)
            else:
                g.op_dynamic(side)


def _mix_upsert(g: _Gen) -> None:
    rng = g.rng
    for side in ("A", "C"):
        for _ in range(g.n_ops(1, 4)):
            if rng.random() < 0.7:
                shared = None
                if side == "C" and rng.random() < 0.3:
                    a_keys = [k for o in g.ops["A"] if o.kind == "UPSERT" for k in o.keys]
                    if a_keys:
                        shared = [int(k) for k in rng.choice(a_keys, size=min(3, len(a_keys)), replace=False)]
                g.op_upsert(side, str(rng.choice(["cow", "mor"])), shared=shared)
            else:
                g.op_append(side)


def _mix_deletes(g: _Gen, mode: str) -> None:
    rng = g.rng
    for side in ("A", "C"):
        for _ in range(g.n_ops(1, 4)):
            if rng.random() < 0.75:
                g.op_delete(side, mode)
            else:
                g.op_append(side)


def _mix_compaction(g: _Gen) -> None:
    rng = g.rng
    pids = [int(p) for p in rng.choice(g.parts, size=int(rng.integers(1, 3)), replace=False)]
    g.op_replace("A", pids)
    for _ in range(g.n_ops(0, 2)):
        if rng.random() < 0.5:
            g.op_append("A")
        else:
            g.op_delete("A", "mor")
    for _ in range(g.n_ops(1, 4)):
        r = rng.random()
        focus = int(rng.choice(pids)) if rng.random() < 0.7 else int(rng.choice(g.parts))
        if r < 0.6:
            g.op_delete("C", "mor", pred=X.and_(g.part_pred(focus), g.num_range("C")))
        elif r < 0.8:
            g.op_delete("C", "cow", pred=X.and_(g.part_pred(focus), g.num_range("C")))
        elif r < 0.9:
            g.op_upsert("C", "mor")
        else:
            g.op_append("C", [focus])


def _filler(g: _Gen, side: str) -> None:
    r = g.rng.random()
    if r < 0.4:
        g.op_append(side)
    elif r < 0.7:
        g.op_delete(side, str(g.rng.choice(["cow", "mor"])))
    else:
        g.op_upsert(side, str(g.rng.choice(["cow", "mor"])))


def _mix_adversarial(g: _Gen) -> None:
    rng = g.rng
    template = str(rng.choice(["append_matches_delete", "append_matches_remote_delete", "overwrite_inserts_match",
                               "shared_upsert_keys", "masking_delete", "duplicate_appends",
                               "compaction_vs_overwrite", "both_cow_plus_match"]))
    g.meta = {"template": template}
    y, x = ("A", "C") if rng.random() < 0.5 else ("C", "A")
    for _ in range(g.n_ops(0, 1)):
        _filler(g, y)
    if template == "append_matches_delete":
        op = g.op_delete(y, str(rng.choice(["cow", "mor"])), kind=str(rng.choice(["num", "num_in_part", "time"])))
        g.op_append_matching(x, op.filter)
    elif template == "append_matches_remote_delete":
        # predicate whose base matches lie in some partitions; matching rows appended elsewhere
        op = g.op_delete(y, str(rng.choice(["cow", "mor"])), kind="rare")
        g.op_append_matching(x, op.filter, avoid_parts=g.base_match_parts(op.filter))
    elif template == "overwrite_inserts_match":
        op_y = g.op_delete(y, "cow", kind=str(rng.choice(["num", "time"])))
        rows = g.take_fresh(x, int(rng.integers(1, 5)), g.parts)
        forced = g.force_match(rows, op_y.filter)
        pred_x = g.pred(x, "num_in_part")
        g.add(x, LogicalOp("OVERWRITE", filter=pred_x, rows=forced if forced is not None else rows,
                           mode=str(rng.choice(["cow", "mor"]))))
    elif template == "shared_upsert_keys":
        op_y = g.op_upsert(y, str(rng.choice(["cow", "mor"])))
        shared = [int(k) for k in rng.choice(op_y.keys, size=min(int(rng.integers(1, 4)), len(op_y.keys)),
                                              replace=False)] if op_y.keys else None
        g.op_upsert(x, str(rng.choice(["cow", "mor"])), shared=shared)
    elif template == "masking_delete":
        op_y = g.op_delete(y, "cow", kind="num")
        app = g.op_append_matching(x, op_y.filter)
        # x deletes its own matching rows again: sequences commute although a pair does not
        g.op_delete(x, str(rng.choice(["cow", "mor"])), pred=op_y.filter)
    elif template == "duplicate_appends":
        rows = g.take_fresh(y, int(rng.integers(3, 20)), g.parts)
        g.add(y, LogicalOp("APPEND", rows=rows))
        g.add(x, LogicalOp("APPEND", rows=rows))
    elif template == "compaction_vs_overwrite":
        p = int(rng.choice(g.parts))
        g.op_replace(y, [p])
        g.op_overwrite(x, "cow", pred=X.and_(g.part_pred(p), g.num_range(x)))
    elif template == "both_cow_plus_match":
        p = int(rng.choice(g.parts))
        op_y = g.op_delete(y, "cow", pred=X.and_(g.part_pred(p), g.num_range(y)))
        g.op_delete(x, "cow", pred=X.and_(g.part_pred(p), g.num_range(x)))
        if rng.random() < 0.5:
            g.op_append_matching(x, op_y.filter)
    for _ in range(g.n_ops(0, 1)):
        _filler(g, x)


MIX_FUNCS = {
    "partition_disjoint_ingest": _mix_partition_disjoint,
    "filter_corrections": _mix_filter_corrections,
    "upsert_keys": _mix_upsert,
    "deletes_mor": lambda g: _mix_deletes(g, "mor"),
    "deletes_cow": lambda g: _mix_deletes(g, "cow"),
    "compaction_on_main": _mix_compaction,
    "adversarial_overlap": _mix_adversarial,
}


def generate(pool: SourcePool, mix: str, seed: int, base_rows: int = 2000, scenario_id: str | None = None,
             source: str | None = None, n_parts: int = 4, hist: int = 1, sel: float = 1.0,
             compact_p: float = 0.0) -> Scenario:
    g = _Gen(pool, mix, seed, base_rows, n_parts=n_parts, hist=hist, sel=sel)
    MIX_FUNCS[mix](g)
    meta = dict(g.meta)
    if compact_p > 0 and g.rng.random() < compact_p:
        op = g.op_replace("A")
        meta["extra_compaction"] = X.describe(op.filter)
    meta["partitions"] = g.parts
    meta["extra_partition"] = g.extra_part
    knobs = {"n_parts": n_parts, "hist": hist, "sel": sel, "compact_p": compact_p}
    if knobs != {"n_parts": 4, "hist": 1, "sel": 1.0, "compact_p": 0.0}:
        meta["knobs"] = knobs
    return Scenario(scenario_id or f"{mix}-{seed}", seed, mix, source or pool.spec.name, g.base,
                    {"A": g.ops["A"], "C": g.ops["C"]}, meta)
