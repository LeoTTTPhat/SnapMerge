# SnapMerge

SnapMerge merges diverged branches of an Apache Iceberg v2 table when their recorded logical operations commute. A successful merge must equal both serial orders as a row multiset. Otherwise, it reports a conflict with a witness, an unsupported input, a physical conflict, a retryable attempt, or an unresolved commit outcome.

This repository contains the implementation, baseline policies, regression tests, deterministic workload generators, experiment runners, and reference records for 23 campaigns.

## Setup

Use **CPython 3.12.12** on macOS or Linux. Internet access is needed to install dependencies and DuckDB extensions, and to download the NYC dataset. The small workflow is designed for less than 4 GB of RAM. The full scale experiments require substantially more memory and disk space; allow approximately 80 GB of RAM and 100 GB of free disk space for the largest configurations.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.lock
python scripts/install_extensions.py
python scripts/unpack_reference.py
```

`requirements.txt` lists the direct dependencies. `requirements.lock` pins their dependency closure. The recorded Python and package versions are also available in each campaign's `environment.json`.

## Quick reproduction

From the repository root:

```bash
bash artifact/reproduce_small.sh .venv/bin/python
```

The workflow runs a single-snapshot copy-on-write regression, the focused regression suites, a differential campaign with **42 scenarios**, and the first **200 physical-state cases**. It generates TPC-H SF 0.01 locally and writes checkpoints to `results/quick/artifact_small/` and `results/quick/artifact_enum/`. Repeating the command resumes completed records.

Expected observations:

- The copy-on-write example reports a logical conflict with a witness. The append-snapshot control produces a result equal to only one serial order.
- The regression suites pass.
- SnapMerge's emitted results agree with both recorded orders and both readers; the information-matched controls and replay are reported separately.
- All 200 physical-state cases complete, every emitted merge is sound and position-exact, and each reported conflict is a true conflict.

Run the complete test suite after preparing the small analysis input:

```bash
python scripts/prepare_tpch.py --sf 0.1 --tables lineitem
python -m unittest discover -s tests
```

An expected failure records the current limitation on merging branches again after an earlier merge snapshot.

## Recompute the reference analysis

The reference records are distributed as one compressed archive per campaign in `reference/`. The setup command `python scripts/unpack_reference.py` verifies their checksums and expands them into `results/<campaign>/raw/`, together with campaign settings, environment records, and runner summaries. Input datasets are prepared locally using their recorded provenance:

```bash
python scripts/prepare_tpch.py --sf 0.1 1 10 --tables lineitem
python scripts/fetch_nyc_tlc.py --months 2023-01
python analyze.py --out-dir results/recomputed --figdir results/recomputed/figures
```

This checks code fingerprints, source-data SHA-256 hashes, and individual record identities before aggregation. It produces `results/recomputed/summary.json`, `analysis.md`, and evaluation charts. The full analysis uses 10,000 bootstrap resamples and a fixed seed; `--boot 100` is useful for a faster check. Historical comparisons with superseded campaigns are absent because only current campaign records are included. Runtime, memory, generated timestamps, and host telemetry can vary.

TPC-H inputs are generated with DuckDB's `tpch` extension. These experiments are not official TPC-H benchmark results. NYC yellow-taxi records are obtained from the [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) under its [terms of use](https://www.nyc.gov/home/terms-of-use.page). Cleaning rules and dataset hashes are recorded in `data/**/provenance.json` and in the preparation scripts.

## Run all campaigns again

Prepare the inputs above, then run:

```bash
python scripts/run_campaigns.py --list
python scripts/run_campaigns.py
SNAPMERGE_RESULTS="$PWD/results/reproduced" python analyze.py \
  --out-dir results/reproduced --figdir results/reproduced/figures
```

The runner uses the original seeds and campaign settings and writes fresh results to `results/reproduced/` and working tables to `work/reproduced/`. It leaves reference records intact. Use `--only NAME` to select a campaign or `--list` to inspect every command. Interrupted campaigns can be resumed by repeating the same command. No external scheduler or workspace-specific utility is required.

## Repository layout

| Path | Purpose |
|---|---|
| `src/snapmerge/` | Merge rules, lineage checks, physical composition, writers, readers, provenance checks, and baselines |
| `tests/` | Unit, regression, adversarial, and differential tests |
| `difftest.py` | Checkpointed workload generation and serial-order comparisons |
| `scale.py` | Isolated scale, contention, and history-length measurements |
| `analyze.py`, `analysis_revision.py` | Fingerprint checks, statistics, reports, and charts |
| `artifact/` | Small reproduction workflow and its helpers |
| `scripts/` | Dataset preparation, full campaign runner, metadata example, and exhaustive enumeration |
| `data/` | Dataset provenance and locally prepared inputs |
| `reference/` | Compact, checksum-verified archives of the 23 reference campaigns |
| `results/` | Reference experiment records and audit summaries |
| `MANIFEST.sha256` | Checksums of the distributed files |

Verify the distributed files with `shasum -a 256 -c MANIFEST.sha256` on macOS or `sha256sum -c MANIFEST.sha256` on Linux.
