#!/usr/bin/env bash
# One-command small reproduction (a few minutes; about 2.5 min on the authors' host, capped at 4 GB). Run from the repository root with the
# pinned environment (requirements.txt; DuckDB's iceberg, avro and tpch extensions installed).
#   bash artifact/reproduce_small.sh [python]
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
PY=${1:-python3}
RESULTS=results/quick
OUT=$RESULTS/artifact_small
echo "== 1. single-snapshot copy-on-write regression"
$PY artifact/r2_single_snapshot.py
echo "== 2. regression tests: provenance, NULL semantics, position-level composition, input and outcome contract, commit resolution and budget stages"
(cd tests && $PY -m unittest test_revision test_nullable test_physical test_rereview test_minor_revision)
echo "== 3. small differential campaign (TPC-H SF 0.01): SnapMerge, controls, replay"
# The runner records the SHA-256 of its source file, so generate the SF 0.01 lineitem file first
# (deterministic DuckDB dbgen port; a few seconds; skipped when the file already matches its provenance).
$PY scripts/prepare_tpch.py --sf 0.01 --tables lineitem
$PY difftest.py --campaign artifact_small --source tpch_lineitem --sf 0.01 --per-mix 6 \
  --base-rows 1000 --variants full,no_net_effect --policies replay,replay_intent,replay_intent_exact,rebase_occ,recompute \
  --provenance-check --results-root "$RESULTS" --work-root work
$PY artifact/summarize_small.py $OUT
echo "== 4. physical-state enumeration (first 200 of 1,568 cases)"
$PY scripts/physical_enum.py --campaign artifact_enum --limit 200 --results-root "$RESULTS" --work-root work
$PY - <<'PY'
import json, glob
r = [json.load(open(f)) for f in glob.glob("results/quick/artifact_enum/raw/*.json")]
m = [x for x in r if x.get("verdict") == "merged"]
assert len(r) == 200, f"expected 200 cases, found {len(r)}"
assert all(x["sound"] for x in m), "unsound or position-inexact merge"
assert all(x["true_conflict"] for x in r if x.get("verdict") == "conflict"), "false conflict"
print(len(r), "cases;", len(m), "merges;", sum(1 for x in m if x["sound"]), "sound and position-exact;",
      sum(1 for x in r if x.get("verdict") == "conflict" and x["true_conflict"]), "conflicts, all true:",
      all(x["true_conflict"] for x in r if x.get("verdict") == "conflict"))
PY
