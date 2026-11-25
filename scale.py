#!/usr/bin/env python3
"""Merge cost versus table size: SnapMerge vs partition-level replay vs row-level merge.

Sizes: tpch:<SF> (TPC-H lineitem, month(l_shipdate) partitions) or nyc:<N> (first N months of 2023,
day(pickup) partitions).  Per size a base table is written once (checkpointed by a marker file).
Per repetition r and scenario kind:
  disjoint  A: MOR delete in p1 + append into p2;   C: MOR delete in p3 + append into p4
            (replay can merge; SnapMerge merges without data reads)
  overlap   A: compaction (REPLACE) of p1;          C: MOR delete in p1 + append into p1
            (replay reports a partition conflict; SnapMerge rebases C's deletes onto A's compaction)
Every emitted merge is checked against the exact oracle (DuckDB, base rows read from the prepared
Parquet file, operations applied as SQL in both orders) through DuckDB iceberg_scan and a
PyIceberg batch scan, both streamed into an on-disk DuckDB database (work/<size>/oracle.duckdb; no
full-table Arrow materialisation).  Both readers are compared with both serial orders (EXCEPT ALL in
both directions); comparisons implied exactly by multiset equality (readers identical, or the two
orders identical) are derived instead of recomputed and marked in check["derived"].
One JSON per (size, kind, rep, policy) under results/<campaign>/raw/, atomic, fingerprint-skipped;
environment.json records versions, sha256 of code and source files, and each invocation.
Base layout is FIXED (appends of BASE_CHUNK_ROWS rows per chunk, flushed at each source-file end);
MEMGUARD_BATCH_SCALE only scales DuckDB's memory limit and Arrow read-batch sizes (never the layout
or the workload); MEMGUARD_DELAY_SECONDS pauses between records.
Timing: before every timed policy the in-process metadata caches are cleared; the 1-minute load
average is recorded per record.  Memory: current and peak process RSS are recorded after the base
build, the oracle, each policy and each check (mem_* fields).  The PyIceberg side of a check reads
the snapshot in groups of 8 x MEMGUARD_BATCH_SCALE scan tasks (catalog.pyiceberg_reader_groups).  --time-budget-min stops before starting a new (size, kind, rep)
unit; rerun the same command to resume (prints pending=<units left>).

Revision (2026-10-05, review R3 MC4/MC5, R1 MC5, R4):
  * sizes tpch-unpart:<SF> build an UNPARTITIONED table; its overlap shape compacts the whole table,
    so the entire table is one contended region;
  * kinds contended-<K> (the target compacts K partitions; the source MOR-deletes and appends in all
    K; K=all uses every partition) and history-<L> (each side commits L small operations in its own
    partitions; commuting by construction);
  * --isolate runs every timed policy in a FRESH process (scale.py --child): cold in-process caches,
    and the record carries that process's baseline and peak RSS and the peak size of its DuckDB
    temporary directory (spill); --warm reads every data, delete and metadata file of the table into
    the OS page cache before each timed run (macOS cannot drop the page cache without root, so all
    policies run warm instead of the first one running cold);
  * --order random permutes the policies of every (size, kind, rep) unit (seeded);
  * --reps-policy p=n gives policy p its own repetition count; --check-reps n oracle-checks only
    repetitions below n (the others record check_skipped);
  * policies recompute and replay_intent (snapmerge.intent) are available.

Example:
  python scale.py --campaign scale_v2_small --sizes tpch:0.1,tpch:1 --reps 5
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from snapmerge import catalog as C  # noqa: E402
from snapmerge import expr as X  # noqa: E402
from snapmerge.baselines import replay, rowlevel  # noqa: E402
from snapmerge.intent import recompute, replay_intent  # noqa: E402
from snapmerge.envinfo import write_environment  # noqa: E402
from snapmerge.merge import snapmerge  # noqa: E402
from snapmerge.oracle import Oracle  # noqa: E402
from snapmerge.sources import (SPECS, generate_tpch, nyc_path, partition_bounds, partition_ids,  # noqa: E402
                               tpch_path)
from snapmerge.writer import LogicalOp, apply_op  # noqa: E402

POLICIES = ("snapmerge", "replay", "rowlevel")
KINDS = ("disjoint", "overlap")
BASE_CHUNK_ROWS = 2_000_000  # fixed base-append chunk: the base layout never depends on MEMGUARD
RAW = "raw"


def atomic_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


def code_files() -> list[Path]:
    return sorted((ROOT / "src" / "snapmerge").glob("*.py")) + [ROOT / "scale.py"]


def code_fingerprint() -> str:
    h = hashlib.sha256()
    for p in code_files():
        h.update(p.name.encode())
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def loadavg() -> float | None:
    try:
        return round(os.getloadavg()[0], 2)
    except OSError:
        return None


def mem_gb() -> dict:
    """Current RSS and the process's peak RSS so far (GB), for memory attribution per phase."""
    import resource

    out = {}
    try:
        import psutil

        out["rss_gb"] = round(psutil.Process().memory_info().rss / 1e9, 3)
    except Exception:
        pass
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    out["peak_rss_gb"] = round(peak / 1e9 if sys.platform == "darwin" else peak * 1024 / 1e9, 3)
    return out


def parse_size(tag: str) -> tuple[str, float | None, tuple[str, ...]]:
    kind, val = tag.split(":")
    if kind in ("tpch", "tpch-unpart"):
        return "tpch_lineitem", float(val), ()
    months = tuple(f"2023-{m:02d}" for m in range(1, int(val) + 1))
    return "nyc_yellow", None, months


def source_files(source: str, sf, months, allow_generate: bool, work: Path) -> list[Path]:
    if source == "tpch_lineitem":
        p = tpch_path(sf, "lineitem")
        if not p.exists():
            if not allow_generate:
                raise FileNotFoundError(f"{p} missing; run scripts/prepare_tpch.py --sf {sf:g}")
            p = work / f"lineitem_sf{sf:g}.parquet"
            if not p.exists():
                pq.write_table(generate_tpch(sf, "lineitem"), p)
        return [p]
    files = [nyc_path(m) for m in months]
    for f in files:
        if not f.exists():
            raise FileNotFoundError(f"{f} missing; run scripts/fetch_nyc_tlc.py --months ...")
    return files


def build_base(table, files: list[Path], spec, scale: float = 1.0) -> dict:
    """Append the source in FIXED chunks of BASE_CHUNK_ROWS rows (rows [k*N, (k+1)*N) of each source
    file, remainder flushed at the file end), so the base layout is identical for every
    MEMGUARD_BATCH_SCALE; `scale` only shrinks the Parquet read batch."""
    rows_total = 0
    chunks = 0
    read_batch = max(10_000, int(250_000 * scale))
    arrow_schema = table.schema().as_arrow()

    def flush(t: pa.Table) -> None:
        nonlocal rows_total, chunks
        table.append(t.cast(arrow_schema), snapshot_properties={"snapmerge.op": "BASE",
                                                                "snapmerge.op-id": f"base{rows_total}"})
        rows_total += t.num_rows
        chunks += 1

    for f in files:
        pf = pq.ParquetFile(f)
        buf: list[pa.RecordBatch] = []
        n = 0
        for batch in pf.iter_batches(batch_size=read_batch):
            buf.append(batch)
            n += batch.num_rows
            while n >= BASE_CHUNK_ROWS:
                t = pa.Table.from_batches(buf)
                flush(t.slice(0, BASE_CHUNK_ROWS))
                rest = t.slice(BASE_CHUNK_ROWS)
                buf, n = rest.to_batches(), rest.num_rows
        if n:
            flush(pa.Table.from_batches(buf))
    table = table.refresh()
    return {"rows": rows_total, "snapshot_id": C.head_id(table, "main"), "append_chunks": chunks,
            "chunk_rows": BASE_CHUNK_ROWS,
            "data_files": len(C.load_state(table, C.head_id(table, "main")).data_files())}


def oracle_connection(db: Path, mem_mb: int, threads: int):
    import duckdb

    con = duckdb.connect(str(db))
    con.execute("LOAD iceberg")
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET memory_limit='{int(mem_mb)}MB'")
    con.execute(f"SET temp_directory='{db.parent / 'duckdb_tmp'}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def check_merge(oracle: Oracle, table, snapshot_id: int, true_conflict: bool, check_threads: int,
                policy_threads: int, tasks_per_group: int = 8) -> dict:
    """Both readers vs both serial orders, streamed into on-disk DuckDB tables (exact EXCEPT ALL).
    Derived comparisons: if the two readers' multisets are equal, the PyIceberg comparisons equal the
    DuckDB ones; if o_ac == o_ca (not a true conflict), the (C;A) comparisons equal the (A;C) ones."""
    con = oracle.con
    con.execute(f"SET threads={int(check_threads)}")
    cols = ", ".join(f'"{c}"' for c in oracle._schema.names)
    try:
        con.execute(f'CREATE OR REPLACE TABLE "_m_duck" AS SELECT {cols} FROM iceberg_scan('
                    f"'{C.metadata_location(table)}', snapshot_from_id={int(snapshot_id)})")
        # PyIceberg reader, streamed in bounded groups of scan tasks (see catalog.pyiceberg_reader_groups)
        con.execute(f'CREATE OR REPLACE TABLE "_m_py" AS SELECT {cols} FROM "o_ac" LIMIT 0')
        for gi, reader in enumerate(C.pyiceberg_reader_groups(table, snapshot_id, tasks_per_group)):
            name = f"_rdg_{gi}"
            con.register(name, reader)
            con.execute(f'INSERT INTO "_m_py" SELECT {cols} FROM {name}')
            con.unregister(name)
        derived = []
        d_readers = oracle.diff_tables("_m_duck", "_m_py")
        agree = d_readers.equal
        d_duck_ac = oracle.diff_tables("_m_duck", "o_ac")
        if agree:
            d_py_ac = d_duck_ac
            derived.append("pyiceberg_vs_ac")
        else:
            d_py_ac = oracle.diff_tables("_m_py", "o_ac")
        if true_conflict:
            d_duck_ca = oracle.diff_tables("_m_duck", "o_ca")
            if agree:
                d_py_ca = d_duck_ca
                derived.append("pyiceberg_vs_ca")
            else:
                d_py_ca = oracle.diff_tables("_m_py", "o_ca")
        else:
            d_duck_ca, d_py_ca = d_duck_ac, d_py_ac
            derived += ["duckdb_vs_ca", "pyiceberg_vs_ca"]
        rows_duck = oracle.count("_m_duck")
    finally:
        for t in ("_m_duck", "_m_py"):
            try:
                con.execute(f'DROP TABLE IF EXISTS "{t}"')
            except Exception:
                pass
        con.execute(f"SET threads={int(policy_threads)}")
    eq_ac = d_duck_ac.equal and d_py_ac.equal
    eq_ca = d_duck_ca.equal and d_py_ca.equal
    return {"duckdb_vs_ac": d_duck_ac.to_json(), "pyiceberg_vs_ac": d_py_ac.to_json(),
            "duckdb_vs_ca": d_duck_ca.to_json(), "pyiceberg_vs_ca": d_py_ca.to_json(),
            "readers": d_readers.to_json(), "readers_agree": agree, "rows_duckdb": rows_duck,
            "eq_ac": eq_ac, "eq_ca": eq_ca, "serial_equivalent": bool(eq_ac or eq_ca),
            "derived": sorted(derived)}


def scenario_ops(spec, files: list[Path], kind: str, rep: int, size_tag: str,
                 partitioned: bool = True) -> dict[str, list[LogicalOp]]:
    seed = int(hashlib.sha256(f"{size_tag}|{kind}|{rep}".encode()).hexdigest()[:12], 16)
    rng = np.random.default_rng(seed)
    import duckdb

    con = duckdb.connect()
    flist = ", ".join(f"'{f}'" for f in files)
    tcol = spec.time_col
    src = pq.read_schema(files[0])
    ttype = src.field(tcol).type
    # partitions by count, using DuckDB on the parquet files (no full load into Python)
    if spec.partition[1] == "month":
        pid_sql = f"(year({tcol}) - 1970) * 12 + month({tcol}) - 1"
    else:
        pid_sql = f"CAST(epoch({tcol}) // 86400 AS BIGINT)"
    parts = [r[0] for r in con.execute(f"SELECT {pid_sql} AS p, count(*) c FROM read_parquet([{flist}]) "
                                       f"GROUP BY 1 HAVING count(*) >= 200 ORDER BY 1").fetchall()]
    p1, p2, p3, p4 = (int(x) for x in rng.choice(parts, size=4, replace=False))
    num = "l_discount" if spec.name.startswith("tpch") else "tip_amount"

    def ppred(p):
        lo, hi = partition_bounds(spec, p, ttype)
        return X.and_(X.cmp(tcol, ">=", lo), X.cmp(tcol, "<", hi))

    def numpred():
        if num == "l_discount":
            lo = float(rng.choice([0.0, 0.02, 0.04, 0.06]))
            return X.between(num, lo, round(lo + 0.01, 2))
        lo = float(rng.choice([0.0, 1.0, 2.0, 3.0]))
        return X.between(num, lo, lo + 0.5)

    def new_rows(p, n, salt):
        t = con.execute(f"SELECT * FROM read_parquet([{flist}]) WHERE {pid_sql} = {p} "
                        f"ORDER BY rid LIMIT {n}").to_arrow_table()
        rid = pc.add(t.column("rid"), pa.scalar(10**12 + rep * 10**10 + salt * 10**9, pa.int64()))
        t = t.set_column(t.schema.get_field_index("rid"), "rid", rid)
        return t

    con_rows = con.execute(f"SELECT count(*) FROM read_parquet([{flist}])").fetchone()[0]
    n_new = int(max(100, min(20_000, con_rows // 1000)))
    if kind == "disjoint":
        ops = {"A": [LogicalOp("DELETE", filter=X.and_(ppred(p1), numpred()), mode="mor"),
                     LogicalOp("APPEND", rows=new_rows(p2, n_new, 1))],
               "C": [LogicalOp("DELETE", filter=X.and_(ppred(p3), numpred()), mode="mor"),
                     LogicalOp("APPEND", rows=new_rows(p4, n_new, 2))]}
    elif kind == "overlap" and not partitioned:
        # unpartitioned table: the compaction rewrites every file, so the whole table is contended
        ops = {"A": [LogicalOp("REPLACE", filter=dict(X.TRUE), sort_by=spec.sort_cols[0])],
               "C": [LogicalOp("DELETE", filter=X.and_(ppred(p1), numpred()), mode="mor"),
                     LogicalOp("APPEND", rows=new_rows(p1, n_new, 3))]}
    elif kind == "overlap":
        ops = {"A": [LogicalOp("REPLACE", filter=ppred(p1), sort_by=spec.sort_cols[0])],
               "C": [LogicalOp("DELETE", filter=X.and_(ppred(p1), numpred()), mode="mor"),
                     LogicalOp("APPEND", rows=new_rows(p1, n_new, 3))]}
    elif kind.startswith("contended-"):
        k = kind.split("-", 1)[1]
        chosen = sorted(int(x) for x in (parts if k == "all" else rng.choice(parts, size=min(int(k), len(parts)),
                                                                              replace=False)))
        region = X.or_(*[ppred(p) for p in chosen])
        n_each = max(10, n_new // len(chosen))
        app = pa.concat_tables([new_rows(p, n_each, 4) for p in chosen])
        ops = {"A": [LogicalOp("REPLACE", filter=region, sort_by=spec.sort_cols[0])],
               "C": [LogicalOp("DELETE", filter=X.and_(region, numpred()), mode="mor"),
                     LogicalOp("APPEND", rows=app)]}
    elif kind.startswith("history-"):
        n_ops = int(kind.split("-", 1)[1])
        m = min(8, len(parts) // 2)
        pick = [int(x) for x in rng.choice(parts, size=2 * m, replace=False)]
        own = {"A": pick[:m], "C": pick[m:]}
        ops = {"A": [], "C": []}
        for side, salt in (("A", 5), ("C", 6)):
            for i in range(n_ops):
                p = own[side][i % m]
                if i % 2 == 0:
                    ops[side].append(LogicalOp("APPEND", rows=new_rows(p, 100, salt * 100 + i)))
                else:
                    ops[side].append(LogicalOp("DELETE", filter=X.and_(ppred(p), numpred()), mode="mor"))
    else:
        raise ValueError(kind)
    con.close()
    return ops


# ============================================================================ isolated policy runs

def _tree_bytes(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def warm_page_cache(table, snapshot_ids: list[int]) -> dict:
    """Read every data, delete, manifest and manifest-list file of the given snapshots once."""
    paths = set()
    for sid in snapshot_ids:
        snap = table.metadata.snapshot_by_id(sid)
        paths.add(C.local_path(snap.manifest_list))
        st = C.load_state(table, sid)
        paths |= {C.local_path(m.manifest_path) for m in st.manifests}
        paths |= {C.local_path(f.path) for f in st.files.values()}
    n = 0
    for pth in sorted(paths):
        try:
            with open(pth, "rb") as fh:
                while chunk := fh.read(1 << 23):
                    n += len(chunk)
        except OSError:
            pass
    return {"files": len(paths), "bytes": n}


def run_child(spec_path: str) -> int:
    """One timed policy run in a fresh process (scale.py --child spec.json)."""
    import resource
    import threading

    import duckdb
    import psutil

    spec = json.loads(Path(spec_path).read_text())
    proc = psutil.Process()
    cat = C.open_catalog(spec["work"])
    table = cat.load_table("sm.t")
    warm = warm_page_cache(table, spec["warm_snapshots"]) if spec["warm"] else None
    C.clear_metadata_caches()
    rss0 = proc.memory_info().rss
    tmp = Path(spec["temp_dir"])
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    peak = {"rss": rss0, "tmp": 0}
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                peak["rss"] = max(peak["rss"], proc.memory_info().rss)
            except psutil.Error:
                pass
            peak["tmp"] = max(peak["tmp"], _tree_bytes(tmp))
            stop.wait(0.1)

    con = None
    if spec["policy"] in ("rowlevel", "recompute"):
        con = duckdb.connect()
        con.execute("LOAD iceberg")
        con.execute(f"SET threads={int(spec['threads'])}")
        con.execute(f"SET memory_limit='{int(spec['memory_mb'])}MB'")
        con.execute(f"SET temp_directory='{tmp}'")
        con.execute("SET preserve_insertion_order=false")
    th = threading.Thread(target=sample, daemon=True)
    th.start()
    load = loadavg()
    try:
        p = spec["policy"]
        if p == "snapmerge":
            out = snapmerge(table, spec["branch"], spec["source"])
        elif p == "replay":
            out = replay(table, spec["branch"], spec["source"])
        elif p == "replay_intent":
            out = replay_intent(table, spec["branch"], spec["source"])
        elif p == "rowlevel":
            out = rowlevel(table, spec["branch"], spec["source"], con=con)
        elif p == "recompute":
            out = recompute(table, spec["branch"], spec["source"], con=con)
        else:
            raise ValueError(p)
        res = out.to_json()
    except Exception as e:
        res = {"verdict": "error", "error": repr(e), "traceback": traceback.format_exc()[-2000:]}
    stop.set()
    th.join()
    if con is not None:
        con.close()
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    res["isolated"] = {"baseline_rss_gb": round(rss0 / 1e9, 3), "peak_rss_gb": round(peak["rss"] / 1e9, 3),
                       "policy_rss_increment_gb": round((peak["rss"] - rss0) / 1e9, 3),
                       "process_maxrss_gb": round((maxrss if sys.platform == "darwin" else maxrss * 1024) / 1e9, 3),
                       "peak_temp_bytes": int(peak["tmp"]), "warm": warm, "loadavg_1m": load}
    shutil.rmtree(tmp, ignore_errors=True)
    atomic_json(Path(spec["result"]), res)
    return 0


def _reps_policy(spec: str) -> dict[str, int]:
    out = {}
    for part in (spec or "").split(","):
        if part.strip():
            k, v = part.split("=")
            out[k.strip()] = int(v)
    return out


def run(a) -> int:
    rdir = Path(a.results_root) / a.campaign
    code_fp = code_fingerprint()
    cfg = {"sizes": a.sizes, "reps": a.reps, "kinds": a.kinds, "policies": a.policies,
           "base_chunk_rows": BASE_CHUNK_ROWS, "duckdb_threads": a.duckdb_threads,
           "duckdb_memory_gb": a.duckdb_memory_gb}
    revision = {"isolate": a.isolate, "warm": a.warm, "order": a.order, "reps_policy": _reps_policy(a.reps_policy),
                "check_reps": a.check_reps}
    if revision != {"isolate": False, "warm": False, "order": "fixed", "reps_policy": {}, "check_reps": None}:
        cfg["revision"] = revision
    meta_path = rdir / "campaign.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta["code_fingerprint"] != code_fp or meta["config"] != cfg:
            print(f"ABORT: {rdir} was produced by different code/config; use a new --campaign", file=sys.stderr)
            return 2
    else:
        atomic_json(meta_path, {"campaign": a.campaign, "config": cfg, "code_fingerprint": code_fp,
                                "created": time.strftime("%Y-%m-%dT%H:%M:%S")})
    scale = float(os.environ.get("MEMGUARD_BATCH_SCALE", "1") or 1)
    delay = float(os.environ.get("MEMGUARD_DELAY_SECONDS", "0") or 0)
    mem_mb = max(512, int(a.duckdb_memory_gb * 1024 * scale))
    crash_after = int(os.environ.get("SNAPMERGE_CRASH_AFTER", "0") or 0)
    tasks_per_group = max(1, int(8 * scale))  # check read concurrency only (results never depend on it)
    policies = a.policies.split(",")
    reps_of = {p: revision["reps_policy"].get(p, a.reps) for p in policies}
    sizes = a.sizes.split(",")
    all_sources = []
    for size_tag in sizes:
        source, sf, months = parse_size(size_tag)
        try:
            all_sources += source_files(source, sf, months, False, Path(a.work_root))
        except FileNotFoundError:
            pass
    write_environment(rdir, campaign=a.campaign, config=cfg, code_fingerprint=code_fp, code_files=code_files(),
                      source_files=all_sources, argv=sys.argv)
    written = 0
    t_start = time.time()
    pending = 0
    for size_tag in sizes:
        source, sf, months = parse_size(size_tag)
        partitioned = not size_tag.startswith("tpch-unpart")
        spec = SPECS[source]
        stag = size_tag.replace(":", "_")
        units = []
        for kind in a.kinds.split(","):
            for rep in range(max(reps_of.values())):
                pols = [p for p in policies if rep < reps_of[p]]
                recs = {p: rdir / RAW / f"{stag}__{kind}__r{rep}__{p}.json" for p in pols}
                fps = {p: hashlib.sha256(f"{code_fp}|{size_tag}|{kind}|{rep}|{p}".encode()).hexdigest()[:24]
                       for p in recs}
                todo = []
                for p, path in recs.items():
                    try:
                        ok = path.exists() and json.loads(path.read_text()).get("fingerprint") == fps[p]
                    except json.JSONDecodeError:
                        ok = False
                    if not ok:
                        todo.append(p)
                if todo:
                    units.append((kind, rep, recs, fps, todo))
        work = Path(a.work_root) / a.campaign / stag
        if not units:
            if work.exists() and not a.keep_work:
                shutil.rmtree(work, ignore_errors=True)
            continue
        if a.time_budget_min and (time.time() - t_start) > a.time_budget_min * 60:
            pending += len(units)
            continue
        work.mkdir(parents=True, exist_ok=True)
        files = source_files(source, sf, months, a.allow_generate, work)
        cat = C.open_catalog(work)
        marker = work / "base.json"
        if marker.exists():
            table = cat.load_table("sm.t")
            base_info = json.loads(marker.read_text())
        else:
            if (work / "catalog.db").exists():  # incomplete earlier attempt: rebuild
                shutil.rmtree(work / "warehouse", ignore_errors=True)
                (work / "catalog.db").unlink()
                cat = C.open_catalog(work)
            schema = pq.read_schema(files[0])
            table = C.create_table(cat, "sm.t", schema, spec.partition if partitioned else None)
            t0 = time.perf_counter()
            base_info = build_base(table, files, spec, scale)
            base_info["write_s"] = round(time.perf_counter() - t0, 2)
            base_info["mem_after_build"] = mem_gb()
            base_info["partitioned"] = partitioned
            atomic_json(marker, base_info)
            table = cat.load_table("sm.t")
        base_sql = "read_parquet([" + ", ".join(f"'{f}'" for f in files) + "])"
        db = work / "oracle.duckdb"
        for f in (db, db.with_name(db.name + ".wal")):
            if f.exists():
                f.unlink()  # derived state only; recomputed per unit
        oracle = Oracle(con=oracle_connection(db, mem_mb, a.duckdb_threads))
        for ui, (kind, rep, recs, fps, todo) in enumerate(units):
            if a.time_budget_min and (time.time() - t_start) > a.time_budget_min * 60:
                pending += len(units) - ui
                break
            ops = scenario_ops(spec, files, kind, rep, size_tag, partitioned)
            bA, bC = f"A_{kind}_{rep}", f"C_{kind}_{rep}"
            sfile = work / f"scenario_{kind}_{rep}.json"
            table = table.refresh()
            if not sfile.exists():
                for b in (bA, bC):
                    if b in table.metadata.refs:
                        table.manage_snapshots().remove_branch(b).commit()
                table = table.refresh()
                table = C.create_branch(table, bA, base_info["snapshot_id"])
                table = C.create_branch(table, bC, base_info["snapshot_id"])
                for side, b in (("A", bA), ("C", bC)):
                    for i, op in enumerate(ops[side]):
                        table = apply_op(table, b, op, f"{side}{i}")
                atomic_json(sfile, {"A": C.head_id(table, bA), "C": C.head_id(table, bC)})
            heads = json.loads(sfile.read_text())
            schema = table.schema().as_arrow()
            t_or = time.perf_counter()
            oracle.con.execute(f"SET threads={int(a.check_threads)}")
            oracle.apply_sql("o_ac", base_sql, schema, ops["A"] + ops["C"])
            oracle.apply_sql("o_ca", base_sql, schema, ops["C"] + ops["A"])
            d_orders = oracle.diff_tables("o_ac", "o_ca")
            true_conflict = not d_orders.equal
            rows_ac, rows_ca = oracle.count("o_ac"), oracle.count("o_ca")
            oracle.con.execute(f"SET threads={int(a.duckdb_threads)}")
            oracle_s = round(time.perf_counter() - t_or, 2)
            mem_oracle = mem_gb()
            run_order = [p for p in policies if p in recs]
            if a.order == "random":
                np.random.default_rng(int(hashlib.sha256(f"order|{size_tag}|{kind}|{rep}".encode())
                                          .hexdigest()[:12], 16)).shuffle(run_order)
            for order, p in enumerate(run_order):
                if p not in todo:
                    continue
                branch = f"m_{p}_{kind}_{rep}"
                table = table.refresh()
                if branch in table.metadata.refs:
                    table.manage_snapshots().remove_branch(branch).commit()
                    table = table.refresh()
                table = C.create_branch(table, branch, C.head_id(table, bA))
                rec = {"size": size_tag, "kind": kind, "rep": rep, "policy": p, "order": order,
                       "fingerprint": fps[p], "base": base_info, "true_conflict": true_conflict,
                       "oracle_orders": d_orders.to_json(), "rows_ac": rows_ac, "rows_ca": rows_ca,
                       "oracle_s": oracle_s, "mem_after_oracle": mem_oracle,
                       "ops": {s_: [o.summary() for o in v] for s_, v in ops.items()},
                       "memguard": {"batch_scale": scale, "duckdb_memory_limit_mb": mem_mb,
                                    "restart": os.environ.get("MEMGUARD_RESTART"),
                                    # minor revision 2026-10-08 (M3): process-tree guard and invocation
                                    "guard_limit_gb": os.environ.get("SNAPMERGE_GUARD_LIMIT_GB"),
                                    "guard_invocation": os.environ.get("SNAPMERGE_GUARD_INVOCATION")}}
                C.clear_metadata_caches()  # cold in-process metadata caches for every timed policy
                rec["loadavg_1m"] = loadavg()
                try:
                    if a.isolate:
                        spec_path = work / f"child_{p}_{kind}_{rep}.json"
                        res_path = work / f"child_{p}_{kind}_{rep}.result.json"
                        res_path.unlink(missing_ok=True)
                        atomic_json(spec_path, {"work": str(work), "policy": p, "branch": branch, "source": bC,
                                                "warm": a.warm, "warm_snapshots": [base_info["snapshot_id"],
                                                                                   heads["A"], heads["C"]],
                                                "temp_dir": str(work / f"tmp_{p}"), "threads": a.duckdb_threads,
                                                "memory_mb": mem_mb, "result": str(res_path)})
                        t_child = time.perf_counter()
                        cp = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--child", str(spec_path)],
                                            capture_output=True, text=True)
                        rec["child_wall_s"] = round(time.perf_counter() - t_child, 3)
                        if cp.returncode != 0 or not res_path.exists():
                            raise RuntimeError(f"child failed rc={cp.returncode}: {cp.stderr[-1500:]}")
                        res = json.loads(res_path.read_text())
                        spec_path.unlink(missing_ok=True)
                        res_path.unlink(missing_ok=True)
                        rec.update(res)
                        table = table.refresh()
                        emitted = res.get("verdict") == "merged"
                        out_table, out_sid = table, res.get("snapshot_id")
                    else:
                        if p == "snapmerge":
                            out = snapmerge(table, branch, bC)
                        elif p == "replay":
                            out = replay(table, branch, bC)
                        elif p == "replay_intent":
                            out = replay_intent(table, branch, bC)
                        elif p == "recompute":
                            out = recompute(table, branch, bC, con=oracle.con)
                        else:
                            out = rowlevel(table, branch, bC, con=oracle.con)
                        rec.update(out.to_json())
                        emitted = out.emitted
                        out_table, out_sid = (out.table.refresh() if out.table is not None else table), out.snapshot_id
                    rec["loadavg_1m_after"] = loadavg()
                    rec["mem_after_policy"] = mem_gb()
                    if emitted and (a.check_reps is None or rep < a.check_reps):
                        t_ck = time.perf_counter()
                        rec["check"] = check_merge(oracle, out_table, out_sid, true_conflict,
                                                   a.check_threads, a.duckdb_threads, tasks_per_group)
                        rec["check_s"] = round(time.perf_counter() - t_ck, 2)
                        rec["mem_after_check"] = mem_gb()
                        rec["serial_equivalent"] = rec["check"]["serial_equivalent"]
                        rec["soundness_violation"] = not rec["check"]["serial_equivalent"]
                        rec["missed_conflict"] = bool(true_conflict)
                    elif emitted:
                        rec["check_skipped"] = True
                        rec["missed_conflict"] = bool(true_conflict)
                    else:
                        rec["missed_conflict"] = False
                except Exception as e:
                    rec.update({"verdict": "error", "error": repr(e), "traceback": traceback.format_exc()[-2000:]})
                atomic_json(recs[p], rec)
                written += 1
                iso = rec.get("isolated", {})
                print(f"{size_tag} {kind} r{rep} {p}: {rec.get('verdict')} {rec.get('latency_ms')} ms "
                      f"check={rec.get('check', {}).get('serial_equivalent')} load={rec.get('loadavg_1m')} "
                      f"peak={iso.get('peak_rss_gb')} tmp={iso.get('peak_temp_bytes')}", flush=True)
                if crash_after and written >= crash_after:
                    os._exit(3)
                if delay:
                    time.sleep(delay)
            for t in ("o_ac", "o_ca"):
                oracle.con.execute(f'DROP TABLE IF EXISTS "{t}"')
            oracle.con.execute("CHECKPOINT")
        oracle.close()
        for f in (db, db.with_name(db.name + ".wal")):
            if f.exists():
                f.unlink()
        shutil.rmtree(work / "duckdb_tmp", ignore_errors=True)
    print(f"done: wrote {written} records pending={pending}")
    return 0


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["--child"]:
        return run_child(argv[1])
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--sizes", default="tpch:0.1")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--kinds", default=",".join(KINDS))
    ap.add_argument("--policies", default=",".join(POLICIES))
    ap.add_argument("--duckdb-memory-gb", type=float, default=8)
    ap.add_argument("--duckdb-threads", type=int, default=2, help="DuckDB threads during timed policies")
    ap.add_argument("--check-threads", type=int, default=4, help="DuckDB threads for oracle/checks (untimed)")
    ap.add_argument("--time-budget-min", type=float, default=0, help="stop starting new units after this")
    ap.add_argument("--keep-work", action="store_true", help="keep work/<campaign>/<size> after completion")
    ap.add_argument("--work-root", default=str(ROOT / "work" / "scale"))
    ap.add_argument("--results-root", default=str(ROOT / "results"))
    ap.add_argument("--allow-generate", action="store_true")
    ap.add_argument("--isolate", action="store_true", help="each timed policy in a fresh process")
    ap.add_argument("--warm", action="store_true", help="warm the OS page cache before each isolated run")
    ap.add_argument("--order", default="fixed", choices=["fixed", "random"])
    ap.add_argument("--reps-policy", default="", help="per-policy repetitions, e.g. rowlevel=3,recompute=3")
    ap.add_argument("--check-reps", type=int, default=None, help="oracle-check only repetitions below this")
    return run(ap.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
