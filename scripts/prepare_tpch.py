#!/usr/bin/env python3
"""Generate TPC-H lineitem / orders locally with DuckDB's tpch extension and record provenance.

Output: data/tpch/sf{SF}/{lineitem,orders}.parquet + data/tpch/sf{SF}/provenance.json
Columns are normalised by snapmerge.sources (rid key, DECIMAL -> DOUBLE, l_comment dropped).
Idempotent: an existing file whose sha256 matches its provenance record is skipped.
Large scale factors spill through an on-disk DuckDB database under work/ and honour
MEMGUARD_BATCH_SCALE (DuckDB memory_limit).  Run SF >= 1 through tools/memory_guard.py.

TPC fair-use caveat: data are produced by DuckDB's port of dbgen.  Nothing derived from them is a
TPC-H benchmark result or comparable to published TPC-H results.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snapmerge.sources import LINEITEM_SQL, ORDERS_SQL, tpch_path  # noqa: E402

CAVEAT = ("Generated locally with DuckDB's tpch extension (dbgen port). Not a TPC-H benchmark result; "
          "must not be compared with published TPC-H results (TPC fair use policy, "
          "https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf).")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def prepare(sf: float, tables: list[str], data_dir: Path) -> dict:
    import duckdb

    out_dir = data_dir / "tpch" / f"sf{sf:g}"
    out_dir.mkdir(parents=True, exist_ok=True)
    prov_path = out_dir / "provenance.json"
    prov = json.loads(prov_path.read_text()) if prov_path.exists() else {"files": {}}
    todo = []
    for t in tables:
        p = tpch_path(sf, t, data_dir)
        rec = prov["files"].get(t)
        if p.exists() and rec and rec.get("sha256") == sha256(p):
            print(f"skip {p} (sha256 matches provenance)")
            continue
        todo.append(t)
    if not todo:
        return prov
    scale = float(os.environ.get("MEMGUARD_BATCH_SCALE", "1") or 1)
    tmpdb = ROOT / "work" / f"tpch_sf{sf:g}.duckdb"
    tmpdb.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmpdb) if sf >= 1 else ":memory:")
    con.execute(f"SET memory_limit='{max(1, int(16 * scale))}GB'")
    con.execute(f"SET temp_directory='{ROOT / 'work' / 'duckdb_tmp'}'")
    con.execute("LOAD tpch")
    have = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    if "lineitem" not in have:
        con.execute(f"CALL dbgen(sf={sf})")
    version = con.execute("SELECT version()").fetchone()[0]
    ext = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='tpch'").fetchone()[0]
    for t in todo:
        p = tpch_path(sf, t, data_dir)
        tmp = p.with_suffix(".parquet.tmp")
        sql = LINEITEM_SQL if t == "lineitem" else ORDERS_SQL
        con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT parquet, ROW_GROUP_SIZE 122880)")
        os.replace(tmp, p)
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]
        nulls = con.execute(f"SELECT count(*) FROM read_parquet('{p}') t WHERE "
                            f"NOT (COLUMNS(*) IS NOT NULL)").fetchone()[0]
        if nulls:
            raise RuntimeError(f"{p}: unexpected NULLs")
        prov["files"][t] = {"path": str(p.relative_to(ROOT)), "sha256": sha256(p), "rows": rows,
                            "bytes": p.stat().st_size}
        print(f"wrote {p} rows={rows}")
    con.close()
    if tmpdb.exists():
        tmpdb.unlink()
    prov.update({
        "source": "TPC-H (DuckDB tpch extension, dbgen)",
        "url": "https://duckdb.org/docs/stable/core_extensions/tpch",
        "license": "TPC EULA / fair use; locally generated, not redistributed",
        "caveat": CAVEAT, "scale_factor": sf, "duckdb_version": version, "tpch_extension_version": ext,
        "retrieved": dt.date.today().isoformat(), "generator": "scripts/prepare_tpch.py",
    })
    tmp = prov_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, prov_path)
    return prov


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", type=float, nargs="+", default=[0.01, 0.1])
    ap.add_argument("--tables", default="lineitem,orders")
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    a = ap.parse_args()
    for sf in a.sf:
        prepare(sf, a.tables.split(","), Path(a.data_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
