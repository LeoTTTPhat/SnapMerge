#!/usr/bin/env python3
"""Install and check the DuckDB extensions used by the reproduction workflows."""
import duckdb


def main():
    with duckdb.connect() as con:
        for extension in ("iceberg", "avro", "tpch"):
            con.execute(f"INSTALL {extension}")
            con.execute(f"LOAD {extension}")
        for row in con.execute(
            "SELECT extension_name, extension_version, installed, loaded "
            "FROM duckdb_extensions() WHERE extension_name IN ('iceberg', 'avro', 'tpch') "
            "ORDER BY extension_name"
        ).fetchall():
            print(*row, sep="\t")


if __name__ == "__main__":
    main()
