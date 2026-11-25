"""Adversarial review (stage Review, 2026-10-02): independent randomized differential fuzzing.

Everything here is independent of the system under test's own scenario generator and oracle:
  * an own seeded generator over TINY value domains (many duplicates, exact predicate ties,
    17-character strings whose column bounds are truncated, rows shared by both branches,
    duplicate base rows and duplicate keys, all-rows deletes, compaction of whole tables) and
    three table layouts (month(d), identity(k), unpartitioned);
  * an own pure-Python multiset oracle (collections.Counter of row tuples) with an own predicate
    evaluator written directly against the JSON filter AST (it does not use expr.to_sql /
    to_arrow / to_iceberg);
  * merged snapshots are read through both readers and compared with the Python oracle.

Checked properties (per scenario):
  P0 the DuckDB oracle (oracle.Oracle.serial) equals the Python oracle in both orders;
  P1 each branch head equals the one-sided Python application in both readers (writer check);
  P2 SnapMerge (full) merged => both readers equal BOTH serial orders (soundness, C2);
  P3 true conflict => SnapMerge reports a conflict (recall, C3);
  P4 every SnapMerge ablation variant is sound when it merges;
  P5 no SnapMerge variant raises.
Baseline outcomes (replay, cherry_pick, rowlevel, pointer policies) are recorded, not asserted.

The unit test runs ADV_FUZZ_N scenarios (default 36); review/run_fuzz.py runs large seeds
ranges with per-scenario checkpoints through the memory guard.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import tempfile
import traceback
import unittest
from collections import Counter
from pathlib import Path

import numpy as np
import pyarrow as pa

from helpers import C, LogicalOp, Oracle, RuleConfig, X, snapmerge  # noqa: F401  (helpers sets sys.path)
from snapmerge.baselines import POLICIES
from snapmerge.writer import apply_op, write_base

SCHEMA = pa.schema([("rid", pa.int64()), ("d", pa.date32()), ("q", pa.float64()), ("k", pa.int64()),
                    ("s", pa.string())])
NAMES = SCHEMA.names
Q_DOM = [0.0, 0.5, 1.0, 1.5, 2.0]
K_DOM = [0, 1, 2, 3, 4]
S_DOM = ["a", "b", "DELIVER IN PERSON", "DELIVER IN PERSOO"]  # 17 chars: bounds truncated to 16
D_DOM = [dt.date(2024, m, d) for m in (1, 2, 3) for d in (1, 15)] + [dt.date(2024, 1, 31), dt.date(2024, 2, 29),
                                                                     dt.date(2024, 3, 31)]
LAYOUTS = {"month": ("d", "month"), "identity_k": ("k", "identity"), "none": None}
VARIANTS = {
    "snapmerge": RuleConfig(),
    "snapmerge[no_net_effect]": RuleConfig(net_effect=False),
    "snapmerge[no_delete_union]": RuleConfig(delete_union=False),
    "snapmerge[no_compaction_rebase]": RuleConfig(compaction_rebase=False),
    "snapmerge[no_bounded_reads]": RuleConfig(bounded_reads=False),
    "snapmerge[filelevel]": RuleConfig(mode="filelevel"),
}
BASELINE_NAMES = ["replay", "cherry_pick", "rowlevel", "lakefs", "nessie_normal", "nessie_force", "fast_forward"]


# ============================================================================ independent oracle

def _typed(col: str, v):
    if col == "d":
        return v if isinstance(v, dt.date) else dt.date.fromisoformat(v)
    if col == "q":
        return float(v)
    if col in ("rid", "k"):
        return int(v)
    return str(v)


def py_eval(f: dict, row: dict) -> bool:
    """Two-valued predicate evaluation straight from the JSON AST (no expr backend)."""
    op = f["op"]
    if op == "true":
        return True
    if op == "false":
        return False
    if op == "cmp":
        a, b = row[f["col"]], _typed(f["col"], f["val"])
        return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b, "=": a == b, "!=": a != b}[f["cmp"]]
    if op == "in":
        return row[f["col"]] in {_typed(f["col"], v) for v in f["vals"]}
    if op == "and":
        return all(py_eval(a, row) for a in f["args"])
    if op == "or":
        return any(py_eval(a, row) for a in f["args"])
    if op == "not":
        return not py_eval(f["arg"], row)
    raise ValueError(op)


def to_rows(t: pa.Table) -> list[dict]:
    return t.select(NAMES).to_pylist()


def py_apply(base: list[dict], ops: list[LogicalOp]) -> list[dict]:
    cur = list(base)
    for op in ops:
        if op.kind in ("DELETE", "OVERWRITE"):
            cur = [r for r in cur if not py_eval(op.filter, r)]
        elif op.kind == "UPSERT":
            ks = set(int(k) for k in op.keys)
            cur = [r for r in cur if r[op.key_col] not in ks]
        if op.kind in ("APPEND", "OVERWRITE", "UPSERT") and op.rows is not None:
            cur += to_rows(op.rows)
    return cur


def bag(rows: list[dict]) -> Counter:
    return Counter(tuple(r[n] for n in NAMES) for r in rows)


def bag_arrow(t: pa.Table) -> Counter:
    t = t.select(NAMES)
    return Counter(zip(*[c.to_pylist() for c in t.columns])) if t.num_rows else Counter()


def bag_diff(x: Counter, y: Counter) -> dict:
    return {"only_left": sum((x - y).values()), "only_right": sum((y - x).values())}


# ============================================================================ independent generator

class FuzzGen:
    def __init__(self, seed: int, heavy: bool = False):
        self.seed = seed
        self.heavy = heavy
        self.rng = np.random.default_rng(seed)
        r = self.rng
        self.layout = str(r.choice(list(LAYOUTS)))
        n = int(r.choice([1, 2, 6, 15, 30, 45] if not heavy else [40, 80, 150]))
        self.next_rid = 10_000
        base = [self._fresh_row(rid=i) for i in range(n)]
        # duplicate full rows (same rid) and duplicate keys with different values
        for _ in range(int(r.integers(0, 4))):
            src = base[int(r.integers(len(base)))]
            base.append(dict(src) if r.random() < 0.6 else dict(self._fresh_row(), rid=src["rid"]))
        self.base = base
        self.state = {"A": list(base), "C": list(base)}
        self.ops: dict[str, list[LogicalOp]] = {"A": [], "C": []}
        self.shared_pool = [self._fresh_row() for _ in range(3)]  # rows both sides may insert verbatim
        self.chunks = int(r.choice([1, 2, 3]))

    # ---- values / rows
    def _fresh_row(self, rid: int | None = None) -> dict:
        r = self.rng
        if rid is None:
            rid = self.next_rid + int(r.integers(0, 4))  # small rid pool: both sides collide on keys
            if r.random() < 0.5:
                self.next_rid += 1
        return {"rid": int(rid), "d": D_DOM[int(r.integers(len(D_DOM)))], "q": float(r.choice(Q_DOM)),
                "k": int(r.choice(K_DOM)), "s": str(r.choice(S_DOM))}

    def _table(self, rows: list[dict]) -> pa.Table:
        return pa.Table.from_pylist(rows, schema=SCHEMA) if rows else SCHEMA.empty_table()

    def _corrected(self, row: dict) -> dict:
        out = dict(row)
        col = str(self.rng.choice(["q", "k", "s"]))
        dom = {"q": Q_DOM, "k": K_DOM, "s": S_DOM}[col]
        out[col] = type(out[col])(self.rng.choice(dom))  # may equal the old value (no-op correction)
        if self.layout == "identity_k" and col == "k" and self.rng.random() < 0.5:
            out["k"] = row["k"]
        return out

    # ---- predicates (exact ties with domain values, between-values, IN, NOT, !=)
    def _atom(self) -> dict:
        r = self.rng
        col = str(r.choice(["q", "k", "s", "d", "rid", "q", "d"]))
        op = str(r.choice(["<", "<=", ">", ">=", "=", "!=", "in"]))
        if col == "q":
            vals = Q_DOM + [0.75, 1.25]
        elif col == "k":
            vals = K_DOM
        elif col == "s":
            vals = S_DOM + ["DELIVER IN PERSO", "c"]
        elif col == "d":
            vals = D_DOM + [dt.date(2024, 2, 1), dt.date(2024, 3, 1)]
        else:
            vals = list(range(0, 8)) + [10_000, 10_001, 10_002]
        if op == "in":
            k = int(r.integers(1, 4))
            pick = [vals[int(i)] for i in r.choice(len(vals), size=min(k, len(vals)), replace=False)]
            return X.isin(col, pick)
        return X.cmp(col, op, vals[int(r.integers(len(vals)))])

    def pred(self) -> dict:
        r = self.rng
        u = r.random()
        if u < 0.45:
            return self._atom()
        if u < 0.7:
            return X.and_(self._atom(), self._atom())
        if u < 0.85:
            return X.or_(self._atom(), self._atom())
        if u < 0.93:
            return X.not_(self._atom())
        return self.part_pred()

    def part_pred(self) -> dict:
        r = self.rng
        if self.layout == "month":
            m = int(r.integers(1, 4))
            return X.and_(X.cmp("d", ">=", dt.date(2024, m, 1)), X.cmp("d", "<", dt.date(2024, m + 1, 1)))
        if self.layout == "identity_k":
            return X.cmp("k", "=", int(r.choice(K_DOM)))
        return dict(X.TRUE)

    # ---- operations
    def _add(self, side: str, op: LogicalOp) -> None:
        if op.rows is not None:
            op.rows = op.rows.select(NAMES).cast(SCHEMA)
        self.ops[side].append(op)
        self.state[side] = py_apply(self.state[side], [op])

    def _insert_rows(self, side: str, lo: int = 0, hi: int = 4) -> list[dict]:
        r = self.rng
        out = []
        for _ in range(int(r.integers(lo, hi + 1))):
            u = r.random()
            if u < 0.3:
                out.append(dict(self.shared_pool[int(r.integers(len(self.shared_pool)))]))
            elif u < 0.45 and self.base:
                out.append(dict(self.base[int(r.integers(len(self.base)))]))  # duplicate of a base row
            else:
                out.append(self._fresh_row())
        return out

    def op(self, side: str) -> None:
        r = self.rng
        kind = str(r.choice(["APPEND", "DELETE", "OVERWRITE", "UPSERT", "REPLACE", "DYNAMIC"],
                            p=[0.22, 0.22, 0.16, 0.2, 0.12, 0.08]))
        mode = str(r.choice(["cow", "mor"]))
        st = self.state[side]
        if kind == "APPEND":
            self._add(side, LogicalOp("APPEND", rows=self._table(self._insert_rows(side, 0, 4))))
        elif kind == "DELETE":
            self._add(side, LogicalOp("DELETE", filter=self.pred(), mode=mode))
        elif kind == "OVERWRITE":
            p = self.pred()
            matched = [row for row in st if py_eval(p, row)]
            rows = [self._corrected(row) for row in matched if r.random() < 0.6] + self._insert_rows(side, 0, 2)
            self._add(side, LogicalOp("OVERWRITE", filter=p, rows=self._table(rows), mode=mode))
        elif kind == "UPSERT":
            rows = []
            for _ in range(int(r.integers(1, 4))):
                u = r.random()
                if u < 0.5 and st:
                    rows.append(self._corrected(st[int(r.integers(len(st)))]))
                elif u < 0.75:
                    rows.append(dict(self.shared_pool[int(r.integers(len(self.shared_pool)))]))
                else:
                    rows.append(self._fresh_row())
            keys = sorted({int(x["rid"]) for x in rows})
            self._add(side, LogicalOp("UPSERT", rows=self._table(rows), keys=keys, mode=mode, key_col="rid"))
        elif kind == "REPLACE":
            sel = self.part_pred() if r.random() < 0.7 else dict(X.TRUE)
            self._add(side, LogicalOp("REPLACE", filter=sel, sort_by=str(r.choice(["q", "s", "k", "rid"]))))
        else:  # partition-aligned (replace-partitions) overwrite
            p = self.part_pred()
            cur = [row for row in st if py_eval(p, row)]
            rows = [self._corrected(row) for row in cur] or self._insert_rows(side, 1, 2)
            if self.layout == "month":
                lo = dt.date.fromisoformat(p["args"][0]["val"])
                rows = [dict(x, d=lo) if not py_eval(p, x) else x for x in rows]
            elif self.layout == "identity_k":
                rows = [dict(x, k=int(p["val"])) for x in rows]
            self._add(side, LogicalOp("OVERWRITE", filter=p, rows=self._table(rows), mode="cow", dynamic=True))

    def build(self) -> "FuzzGen":
        r = self.rng
        for side in ("A", "C"):
            for _ in range(int(r.choice([0, 1, 1, 2, 2, 3, 4] if not self.heavy else [2, 3, 4, 5, 6]))):
                self.op(side)
        return self

    def describe(self) -> dict:
        return {"seed": self.seed, "heavy": self.heavy, "layout": self.layout, "base_rows": len(self.base),
                "chunks": self.chunks,
                "commute_biased": getattr(self, "commute_biased", False),
                "ops": {s: [o.summary() for o in self.ops[s]] for s in ("A", "C")}}

    def commutes(self) -> bool:
        return bag(py_apply(self.base, self.ops["A"] + self.ops["C"])) == \
            bag(py_apply(self.base, self.ops["C"] + self.ops["A"]))


def make_scenario(seed: int) -> FuzzGen:
    """Even seeds: unconstrained.  Odd seeds: commute-biased by deterministic rejection sampling
    (sub-seeds derived from the seed) so that merges -- and hence soundness -- are exercised; the
    first sub-scenario whose two sides both act and whose serial orders agree is used.
    seed % 4 in (2, 3): heavy profile (40-150 base rows, 2-6 operations per side)."""
    heavy = seed % 4 >= 2
    if seed % 2 == 1:
        for j in range(200):
            g = FuzzGen(seed * 1009 + 7 + j * 1_000_003, heavy).build()
            if g.ops["A"] and g.ops["C"] and g.commutes():
                g.commute_biased = True
                g.seed = seed
                return g
    return FuzzGen(seed, heavy).build()


# ============================================================================ one scenario

def run_fuzz_scenario(seed: int, work_root: str | None = None, policies: list[str] | None = None) -> dict:
    g = make_scenario(seed)
    rec: dict = {"scenario": g.describe(), "policies": {}}
    work = Path(tempfile.mkdtemp(prefix=f"adv-fuzz-{seed}-", dir=work_root))
    oracle = Oracle()
    try:
        cat = C.open_catalog(work)
        base_t = g._table(g.base)
        table = C.create_table(cat, "t.t", SCHEMA, LAYOUTS[g.layout])
        table = write_base(table, base_t, chunks=g.chunks)
        base_id = C.head_id(table, "main")
        table = C.create_branch(table, "A", base_id)
        table = C.create_branch(table, "C", base_id)
        for side in ("A", "C"):
            for i, op in enumerate(g.ops[side]):
                table = apply_op(table, side, op, f"{side}{i}")
        # ---- independent oracle vs DuckDB oracle (P0)
        ac = bag(py_apply(g.base, g.ops["A"] + g.ops["C"]))
        ca = bag(py_apply(g.base, g.ops["C"] + g.ops["A"]))
        true_conflict = ac != ca
        orc = oracle.serial(base_t, g.ops["A"], g.ops["C"])
        duck_ac = bag_arrow(oracle.con.execute('SELECT * FROM "o_ac"').to_arrow_table())
        duck_ca = bag_arrow(oracle.con.execute('SELECT * FROM "o_ca"').to_arrow_table())
        rec["oracle"] = {"true_conflict": true_conflict, "duckdb_true_conflict": orc["true_conflict"],
                         "py_eq_duckdb_ac": duck_ac == ac, "py_eq_duckdb_ca": duck_ca == ca,
                         "rows_ac": sum(ac.values()), "rows_ca": sum(ca.values())}
        # ---- writer check (P1)
        wc = {}
        for side in ("A", "C"):
            exp = bag(py_apply(g.base, g.ops[side]))
            sid = C.head_id(table, side)
            wc[side] = {"duckdb": bag_arrow(C.scan_duckdb(oracle.con, table, sid)) == exp,
                        "pyiceberg": bag_arrow(C.scan_pyiceberg(table, sid)) == exp}
        rec["writer_check"] = wc
        a_head = C.head_id(table, "A")
        for pname in (policies or list(VARIANTS) + BASELINE_NAMES):
            branch = "m_" + pname.replace("[", "_").replace("]", "")
            table = C.create_branch(table, branch, a_head)
            try:
                if pname in VARIANTS:
                    out = snapmerge(table, branch, "C", VARIANTS[pname], policy=pname)
                elif pname == "rowlevel":
                    out = POLICIES[pname](table, branch, "C", con=oracle.con)
                else:
                    out = POLICIES[pname](table, branch, "C")
                if out.table is not None:
                    table = out.table
                pr = {"verdict": out.verdict, "rule": out.rule, "conflict_class": out.conflict_class}
                if out.emitted:
                    table = table.refresh()
                    r_duck = bag_arrow(C.scan_duckdb(oracle.con, table, out.snapshot_id))
                    r_py = bag_arrow(C.scan_pyiceberg(table, out.snapshot_id))
                    pr.update({"readers_agree": r_duck == r_py, "duck_eq_ac": r_duck == ac, "duck_eq_ca": r_duck == ca,
                               "py_eq_ac": r_py == ac, "py_eq_ca": r_py == ca,
                               "vs_ac": bag_diff(r_duck, ac)})
                    pr["serial_equivalent"] = (r_duck == r_py) and (r_duck == ac or r_duck == ca)
                    pr["commuting_equivalent"] = (r_duck == r_py) and r_duck == ac and r_duck == ca
                    pr["outcome"] = "FN" if true_conflict else "TN"
                else:
                    pr["outcome"] = "TP" if true_conflict else "FP"
                rec["policies"][pname] = pr
            except Exception as e:  # recorded as a result
                rec["policies"][pname] = {"verdict": "error", "error": repr(e),
                                          "traceback": traceback.format_exc()[-1500:]}
            table = table.refresh()
    finally:
        oracle.close()
        shutil.rmtree(work, ignore_errors=True)
    return rec


def violations(rec: dict) -> list[str]:
    """Property violations of one scenario record (empty list = all properties hold)."""
    bad = []
    o = rec["oracle"]
    if not (o["py_eq_duckdb_ac"] and o["py_eq_duckdb_ca"] and o["true_conflict"] == o["duckdb_true_conflict"]):
        bad.append("P0 python oracle != duckdb oracle")
    for side, w in rec["writer_check"].items():
        if not (w["duckdb"] and w["pyiceberg"]):
            bad.append(f"P1 writer check failed on {side}: {w}")
    for pname, pr in rec["policies"].items():
        if pname not in VARIANTS:
            continue
        if pr["verdict"] == "error":
            bad.append(f"P5 {pname} raised {pr['error']}")
            continue
        if pr["verdict"] == "merged" and not pr.get("commuting_equivalent"):
            bad.append(f"P2/P4 {pname} unsound merge: {pr}")
        if pname == "snapmerge" and o["true_conflict"] and pr["verdict"] != "conflict":
            bad.append("P3 snapmerge missed a true conflict")
    return bad


class AdversarialFuzz(unittest.TestCase):
    """Independent fuzzing: every property P0-P5 must hold on every scenario."""

    def test_fuzz_properties(self):
        n = int(os.environ.get("ADV_FUZZ_N", "36"))
        start = int(os.environ.get("ADV_FUZZ_START", "900000"))
        failures = {}
        for seed in range(start, start + n):
            rec = run_fuzz_scenario(seed)
            bad = violations(rec)
            if bad:
                failures[seed] = {"violations": bad, "scenario": rec["scenario"]}
        self.assertEqual(failures, {}, json.dumps(failures, indent=1, default=str)[:6000])

    def test_generator_is_deterministic(self):
        a = FuzzGen(4242).build()
        b = FuzzGen(4242).build()
        self.assertEqual(a.describe(), b.describe())
        for s in ("A", "C"):
            for x, y in zip(a.ops[s], b.ops[s]):
                self.assertEqual(x.rows is None, y.rows is None)
                if x.rows is not None:
                    self.assertTrue(x.rows.equals(y.rows))


if __name__ == "__main__":
    unittest.main()
