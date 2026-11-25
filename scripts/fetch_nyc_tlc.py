#!/usr/bin/env python3
"""Download NYC TLC yellow-taxi trip records (Parquet, 2023) and prepare clean copies.

Raw:   data/raw/nyc_tlc/yellow_tripdata_YYYY-MM.parquet    (git-ignored)
Clean: data/nyc/yellow_tripdata_YYYY-MM.clean.parquet + data/nyc/provenance.json
Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
        (linked from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
Terms: NYC Taxi & Limousine Commission trip record data, published under the NYC Open Data
       terms of use (https://www.nyc.gov/home/terms-of-use.page); record the terms with the data.
Cleaning (documented, deterministic): keep VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tip_amount, total_amount;
keep rows whose pickup lies in the file's month and that have no NULL in these columns; add
rid = month_index * 10^8 + row number (month_index = (year-2000)*12 + month), where rows are numbered
in the order of ALL cleaned columns (pickup, dropoff, VendorID, PULocationID, DOLocationID,
trip_distance, fare_amount, tip_amount, total_amount, payment_type), so the (rid, row) set does not
depend on DuckDB's scan order (rows tied on every column are identical, so any tie order gives the
same set); the clean file is written in rid order.
About 50 MB per month; 12 months about 600 MB (within the 20 GB download budget).
Downloads are streamed to a temporary file and moved into place atomically; existing files whose
sha256 matches the provenance record are skipped.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID",
        "DOLocationID", "payment_type", "fare_amount", "tip_amount", "total_amount"]
ORDER_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "VendorID", "PULocationID", "DOLocationID",
              "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(month: str, raw_dir: Path, prov: dict) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    dst = raw_dir / f"yellow_tripdata_{month}.parquet"
    rec = prov["raw"].get(month)
    if dst.exists() and rec and rec.get("sha256") == sha256(dst):
        print(f"skip download {dst}")
        return dst
    url = URL.format(m=month)
    tmp = dst.with_suffix(".parquet.tmp")
    with urllib.request.urlopen(url, timeout=120) as r, open(tmp, "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
    os.replace(tmp, dst)
    prov["raw"][month] = {"url": url, "sha256": sha256(dst), "bytes": dst.stat().st_size,
                          "retrieved": dt.date.today().isoformat()}
    return dst


def clean(month: str, raw: Path, out_dir: Path, prov: dict) -> Path:
    import duckdb

    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / f"yellow_tripdata_{month}.clean.parquet"
    y, m = (int(x) for x in month.split("-"))
    lo = dt.date(y, m, 1)
    hi = dt.date(y + m // 12, m % 12 + 1, 1)
    midx = (y - 2000) * 12 + m
    con = duckdb.connect()
    sel = ", ".join([
        "CAST(VendorID AS BIGINT) AS VendorID",
        "CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime",
        "CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime",
        "CAST(trip_distance AS DOUBLE) AS trip_distance",
        "CAST(PULocationID AS BIGINT) AS PULocationID",
        "CAST(DOLocationID AS BIGINT) AS DOLocationID",
        "CAST(payment_type AS BIGINT) AS payment_type",
        "CAST(fare_amount AS DOUBLE) AS fare_amount",
        "CAST(tip_amount AS DOUBLE) AS tip_amount",
        "CAST(total_amount AS DOUBLE) AS total_amount",
    ])
    notnull = " AND ".join(f"{c} IS NOT NULL" for c in COLS)
    tmp = dst.with_suffix(".parquet.tmp")
    order = ", ".join(ORDER_COLS)
    con.execute(f"""COPY (
        SELECT * FROM (
          SELECT CAST({midx} AS BIGINT) * 100000000 + row_number() OVER (ORDER BY {order}) AS rid, * FROM (
            SELECT {sel} FROM read_parquet('{raw}')
            WHERE {notnull} AND tpep_pickup_datetime >= TIMESTAMP '{lo}' AND tpep_pickup_datetime < TIMESTAMP '{hi}'))
        ORDER BY rid
        ) TO '{tmp}' (FORMAT parquet)""")
    os.replace(tmp, dst)
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
    raw_rows = con.execute(f"SELECT count(*) FROM read_parquet('{raw}')").fetchone()[0]
    con.close()
    prov["clean"][month] = {"path": str(dst.relative_to(ROOT)), "sha256": sha256(dst), "rows": rows,
                            "raw_rows": raw_rows, "dropped_rows": raw_rows - rows}
    return dst


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--months", nargs="+", default=["2023-01"])
    ap.add_argument("--data-dir", default=str(ROOT / "data"))
    a = ap.parse_args()
    data = Path(a.data_dir)
    prov_path = data / "nyc" / "provenance.json"
    prov_path.parent.mkdir(parents=True, exist_ok=True)
    prov = json.loads(prov_path.read_text()) if prov_path.exists() else {"raw": {}, "clean": {}}
    prov.update({"source": "NYC TLC Trip Record Data (yellow taxi)",
                 "landing_page": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
                 "license": "NYC Open Data terms of use, https://www.nyc.gov/home/terms-of-use.page",
                 "cleaning": "scripts/fetch_nyc_tlc.py docstring"})
    for m in a.months:
        raw = download(m, data / "raw" / "nyc_tlc", prov)
        clean(m, raw, data / "nyc", prov)
        tmp = prov_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(prov, indent=2, sort_keys=True) + "\n")
        os.replace(tmp, prov_path)
        print(f"prepared {m}: {prov['clean'][m]['rows']} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
