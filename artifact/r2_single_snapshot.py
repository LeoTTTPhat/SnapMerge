#!/usr/bin/env python3
"""Single-snapshot copy-on-write regression (paper Section 8.2).

Base F = {(1,0),(2,0)} and H = {(3,1)} in one partition.  The target commits OVERWRITE(k=1, {(1,1)})
as ONE copy-on-write snapshot (remove F, add G = {(1,1),(2,0)}); the source commits DELETE(x=1).
(A;C) = {(2,0)} and (C;A) = {(1,1),(2,0)}: a true conflict.

Prints the oracle result, SnapMerge's outcome with its net-effect witness, and the result of the
rule that takes copy-on-write inserts only from append snapshots (which merges to (C;A) only).
Run from the repository root:  python artifact/r2_single_snapshot.py
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pyarrow as pa  # noqa: E402

from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.rules import RuleConfig  # noqa: E402
from snapmerge.writer import LogicalOp, apply_op, op_properties  # noqa: E402

S = pa.schema([("k", pa.int64()), ("x", pa.int64())])


def T(*rows):
    return pa.table({"k": [r[0] for r in rows], "x": [r[1] for r in rows]}, schema=S)


def main() -> int:
    d = Path(tempfile.mkdtemp(prefix="snapmerge-r2-"))
    o = Oracle()
    try:
        cat = C.open_catalog(d)
        t = C.create_table(cat, "t.t", S, None)
        t.append(T((1, 0), (2, 0)), snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": "b0"})
        t.append(T((3, 1)), snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": "b1"})
        t = t.refresh()
        base = C.head_id(t, "main")
        t = C.create_branch(t, "A", base)
        t = C.create_branch(t, "C", base)
        F = [f for f in C.load_state(t, base).data_files() if f.record_count == 2][0]
        op_a = LogicalOp("OVERWRITE", filter=X.cmp("k", "=", 1), rows=T((1, 1)), mode="cow", key_col="k")
        G = C.write_data_files(t, T((1, 1), (2, 0)))
        t, _ = C.commit_delta(t, "A", C.load_state(t, C.head_id(t, "A")), remove_paths={F.path}, add_files=G,
                              operation="overwrite", properties=op_properties(op_a, "A0", part="mixed", table=t))
        op_c = LogicalOp("DELETE", filter=X.cmp("x", "=", 1), mode="cow", key_col="k")
        t = apply_op(t.refresh(), "C", op_c, "C0")
        orc = o.serial(T((1, 0), (2, 0), (3, 1)), [op_a], [op_c])
        print("oracle: true conflict =", orc["true_conflict"])
        for name, cfg in (("snapmerge", RuleConfig()), ("append-snapshot rule", RuleConfig(discovery="append_rule"))):
            br = "m_" + name.split()[0].replace("-", "_")
            t = C.create_branch(t, br, C.head_id(t, "A"))
            out = snapmerge(t, br, "C", cfg)
            t = (out.table or t).refresh()
            line = f"{name}: {out.to_json()['category']}"
            if out.emitted:
                chk = o.check_snapshot(t, out.snapshot_id, ["o_ac", "o_ca"])
                line += f"; equals (A;C): {chk['duckdb_eq_o_ac']}, equals (C;A): {chk['duckdb_eq_o_ca']}"
            else:
                w = out.witness
                line += "; witness " + json.dumps({k: w[k] for k in ("example_row", "H_A", "H_C", "order_difference")})
            print(line)
    finally:
        o.close()
        shutil.rmtree(d, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
