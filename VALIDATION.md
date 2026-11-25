# Reproduction validation

Validated on 25 November 2026 with CPython 3.12.12 on macOS arm64, using the package versions pinned in `requirements.lock`.

| Check | Result |
|---|---|
| Complete regression suite | 167 tests; successful with 1 expected failure |
| Small differential workflow | 42 scenarios; no SnapMerge soundness or reader-agreement violations |
| Provenance checks in the small workflow | 202 operations; 0 violations |
| Physical-state subset | 200 cases; 179 sound and position-exact merges, 21 true conflicts |
| Checkpoint resume | All 42 differential scenarios and 200 physical cases reused successfully |
| Reference campaign fingerprints | Exact match for all 23 campaigns |
| Reference input hashes | All required source-data SHA-256 checks passed |
| Full reference aggregation | 10,000 bootstrap resamples, seed 20261002; all 23 campaign summaries exactly equal to the distributed reference summaries |
| Portable full-run entry point | All 23 campaign commands present |

The expected failure documents the current limitation when merging branches again after an earlier merge snapshot. It is preserved as an explicit regression marker.

The reference aggregation reads the distributed raw experiment records. Fresh execution was validated with the complete test suite and the small differential and physical-state workflows. Dependency versions were checked against the available pinned environment; installing dependencies into a fresh environment requires internet access.

Commands used:

```bash
python scripts/install_extensions.py
python scripts/unpack_reference.py
python scripts/prepare_tpch.py --sf 0.1 --tables lineitem
python -m unittest discover -s tests
bash artifact/reproduce_small.sh python
python analyze.py --out-dir results/recomputed --figdir results/recomputed/figures
python scripts/run_campaigns.py --list
```

Local generated datasets and validation outputs are ignored by Git. Dataset preparation scripts and provenance records are distributed for reproduction.
