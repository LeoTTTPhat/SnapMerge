#!/usr/bin/env python3
"""Worked trace: user statement -> recorded operation -> snapshot group (schema-v2 metadata).

Runs the writer on a three-row table and prints, per snapshot, the Iceberg operation, the files it
adds/removes and the snapmerge.* summary properties, for: an UPDATE recorded as a copy-on-write
OVERWRITE (PyIceberg delete part + insert part in one transaction), the same statement committed by a
single-snapshot writer (mixed part), and a merge-on-read upsert.  Writes results/metadata_trace.json.
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
from snapmerge.writer import LogicalOp, apply_op  # noqa: E402

S = pa.schema([("k", pa.int64()), ("x", pa.int64()), ("p", pa.int64())])


def T(*rows):
    return pa.table({n: [r[i] for r in rows] for i, n in enumerate(S.names)}, schema=S)


def trace(writer: str, op: LogicalOp) -> list[dict]:
    d = Path(tempfile.mkdtemp(prefix="sm-trace-"))
    try:
        cat = C.open_catalog(d)
        t = C.create_table(cat, "t.t", S, ("p", "identity"))
        t.append(T((1, 0, 1), (2, 0, 1), (3, 5, 2)), snapshot_properties={"snapmerge.op": "BASE", "snapmerge.op-id": "b0"})
        t = t.refresh()
        before = C.head_id(t, "main")
        t = apply_op(t, "main", op, "op1", writer=writer)
        out, sid = [], C.head_id(t, "main")
        while sid is not None and sid != before:
            snap = t.metadata.snapshot_by_id(sid)
            props = C.summary_props(snap)
            st, par = C.load_state(t, sid), C.load_state(t, snap.parent_snapshot_id)
            added = [f for p, f in st.files.items() if p not in par.files]
            removed = [f for p, f in par.files.items() if p not in st.files]
            out.append({"operation": props["operation"],
                        "adds": [f"{f.content} file ({f.record_count} rows)" for f in added],
                        "removes": [f"{f.content} file ({f.record_count} rows)" for f in removed],
                        "props": {k: v for k, v in sorted(props.items()) if k.startswith("snapmerge.")}})
            sid = snap.parent_snapshot_id
        return list(reversed(out))
    finally:
        shutil.rmtree(d, ignore_errors=True)


def main() -> int:
    upd = LogicalOp("OVERWRITE", filter=X.cmp("k", "=", 1), rows=T((1, 1, 1)), mode="cow", key_col="k")
    ups = LogicalOp("UPSERT", rows=T((2, 7, 1), (4, 1, 2)), keys=[2, 4], mode="mor", key_col="k")
    res = {"statement_1": "UPDATE t SET x = x + 1 WHERE k = 1  (recorded as OVERWRITE(k = 1, {(1,1,1)}), fixed rows)",
           "split_writer": trace("split", upd), "single_snapshot_writer": trace("single", upd),
           "statement_2": "MERGE INTO t USING {(2,7,1),(4,1,2)} ON k  (recorded as UPSERT(K={2,4}, rows), merge-on-read)",
           "mor_upsert": trace("split", ups)}
    (ROOT / "results" / "metadata_trace.json").write_text(json.dumps(res, indent=1) + "\n")
    print(json.dumps(res, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
