"""Environment records for campaign directories (results/<campaign>/environment.json).

Records the interpreter, package and DuckDB-extension versions, host facts, the sha256 of every
source data file and of every code file, the campaign configuration and the memory-guard variables
of each invocation.  Written atomically; one entry is appended per invocation.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
import time
from pathlib import Path

PACKAGES = ("pyiceberg", "pyiceberg-core", "pyarrow", "duckdb", "numpy", "pandas", "sqlalchemy", "cachetools")
_SHA_CACHE: dict[tuple, str] = {}


def sha256_file(path: str | os.PathLike) -> str:
    p = Path(path)
    st = p.stat()
    key = (str(p.resolve()), st.st_size, st.st_mtime_ns)
    hit = _SHA_CACHE.get(key)
    if hit is None:
        h = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 22), b""):
                h.update(chunk)
        hit = _SHA_CACHE[key] = h.hexdigest()
    return hit


def package_versions() -> dict[str, str | None]:
    from importlib import metadata

    out = {}
    for name in PACKAGES:
        try:
            out[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            out[name] = None
    return out


def duckdb_extensions() -> dict:
    try:
        import duckdb

        con = duckdb.connect()
        rows = con.execute("SELECT extension_name, installed, extension_version FROM duckdb_extensions() "
                           "WHERE extension_name IN ('iceberg', 'tpch', 'avro', 'parquet')").fetchall()
        ver = con.execute("SELECT version()").fetchone()[0]
        con.close()
        return {"duckdb_version": ver, "extensions": {r[0]: {"installed": r[1], "version": r[2]} for r in rows}}
    except Exception as e:  # never fail a campaign over the environment record
        return {"error": repr(e)}


def host() -> dict:
    out = {"platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count()}
    try:
        out["loadavg_at_start"] = [round(x, 2) for x in os.getloadavg()]
    except OSError:
        pass
    try:
        import psutil

        out["memory_bytes"] = psutil.virtual_memory().total
    except Exception:
        pass
    return out


def write_environment(rdir: Path, *, campaign: str, config: dict, code_fingerprint: str,
                      code_files: list[Path], source_files: list[Path], argv: list[str]) -> Path:
    path = Path(rdir) / "environment.json"
    env = json.loads(path.read_text()) if path.exists() else None
    invocation = {
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "argv": argv, "pid": os.getpid(),
        "memguard": {k: os.environ.get(k) for k in ("MEMGUARD_BATCH_SCALE", "MEMGUARD_DELAY_SECONDS",
                                                     "MEMGUARD_RESTART") if os.environ.get(k) is not None},
        "host": host(),
    }
    if env is None:
        env = {
            "campaign": campaign,
            "created": invocation["started"],
            "python": {"version": sys.version, "executable": sys.executable},
            "packages": package_versions(),
            "duckdb": duckdb_extensions(),
            "code_fingerprint": code_fingerprint,
            "code_sha256": {str(p.name): sha256_file(p) for p in code_files},
            "source_files": {str(p): {"sha256": sha256_file(p), "bytes": Path(p).stat().st_size}
                             for p in source_files},
            "config": config,
            "invocations": [],
        }
    env["invocations"].append(invocation)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".json.tmp{os.getpid()}")
    tmp.write_text(json.dumps(env, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)
    return path
