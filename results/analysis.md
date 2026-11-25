# SnapMerge: Analysis-stage results

Generated 2026-10-08T19:50:44+0700 by `analyze.py` from fingerprint-verified raw results only. Bootstrap B = 10000, seed 20261002. All numbers below are recomputed from `results/<campaign>/raw/*.json`. Intervals are 95% percentile bootstraps over scenarios (one seed each), stratified by mix, unless marked otherwise. Comparisons with SnapMerge are paired: both policies use the same resamples. Where a count is 0 of n or n of n, the exact Clopper-Pearson interval is also given.

Rates: *recall* = TP / true conflicts (errors and merges on true conflicts count as misses). *Precision* = TP / reported conflicts. *Sound auto-merge rate of commuting* = merges equal to a serial order / commuting scenarios. A *soundness violation* is a merge equal to neither serial order in DuckDB and PyIceberg.

## 1. Inputs and fingerprint gate

| Directory | Runner | Recorded code fp | Code vs current source | Source data sha256 | Records read | Records rejected | Planned but missing | Status |
|---|---|---|---|---|---|---|---|---|
| ablation_v2 | difftest | 957ccdc57ea8b573 | mismatch | ok | 0 | 1505 |  | rejected |
| ablation_v3 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 1505 |  | rejected |
| ablation_v4 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 1505 |  | rejected |
| ablation_v5 | difftest | bf098abf1db6c7a8 | exact | ok | 1505 | 0 | 0 | accepted |
| contended_sf10_v1 | scale | 598ab5a422684f9b | mismatch | ok | 0 | 18 |  | rejected |
| contended_sf10_v2 | scale | 91c43b727578d9dd | mismatch | ok | 0 | 18 |  | rejected |
| contended_sf10_v3 | scale | 3607bd40bcb44b33 | exact | ok | 18 | 0 | 0 | accepted |
| contended_unpart_v1 | scale | 598ab5a422684f9b | mismatch | ok | 0 | 9 |  | rejected |
| contended_unpart_v2 | scale | 91c43b727578d9dd | mismatch | ok | 0 | 9 |  | rejected |
| contended_unpart_v3 | scale | 3607bd40bcb44b33 | exact | ok | 9 | 0 | 0 | accepted |
| contended_v1 | scale | 598ab5a422684f9b | mismatch | ok | 0 | 48 |  | rejected |
| contended_v2 | scale | 91c43b727578d9dd | mismatch | ok | 0 | 48 |  | rejected |
| contended_v3 | scale | 3607bd40bcb44b33 | exact | ok | 48 | 0 | 0 | accepted |
| counterexamples_v1 | counterexamples | 4abc27e495ea3f43 | mismatch | ok | 0 | 31 |  | rejected |
| counterexamples_v2 | counterexamples | 3746e9be8cd7da34 | mismatch | ok | 0 | 31 |  | rejected |
| counterexamples_v3 | counterexamples | 54475678875ae36e | mismatch | ok | 0 | 31 |  | rejected |
| counterexamples_v4 | counterexamples | be4017c734428201 | exact | ok | 31 | 0 |  | accepted |
| difftest_main_nyc_v2 | difftest | 9cbc8a0e6f67537b | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_nyc_v3 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_nyc_v4 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_nyc_v5 | difftest | bf098abf1db6c7a8 | exact | ok | 3003 | 0 | 0 | accepted |
| difftest_main_tpch_v2 | difftest | 957ccdc57ea8b573 | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_tpch_v3 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_tpch_v4 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 3003 |  | rejected |
| difftest_main_tpch_v5 | difftest | bf098abf1db6c7a8 | exact | ok | 3003 | 0 | 0 | accepted |
| fallback_filelevel_v2 | difftest | 957ccdc57ea8b573 | mismatch | ok | 0 | 1001 |  | rejected |
| fallback_filelevel_v3 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 1001 |  | rejected |
| fallback_filelevel_v4 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 1001 |  | rejected |
| fallback_filelevel_v5 | difftest | bf098abf1db6c7a8 | exact | ok | 1001 | 0 | 0 | accepted |
| history_v1 | scale | 598ab5a422684f9b | mismatch | ok | 0 | 36 |  | rejected |
| history_v2 | scale | 91c43b727578d9dd | mismatch | ok | 0 | 36 |  | rejected |
| history_v3 | scale | 3607bd40bcb44b33 | exact | ok | 36 | 0 | 0 | accepted |
| mixed_metadata_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 1001 |  | rejected |
| mixed_metadata_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 1001 |  | rejected |
| mixed_metadata_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 1001 | 0 | 0 | accepted |
| physical_enum_v1 | enumerator | 8451b89ce23acf51 | mismatch | ok | 0 | 1568 |  | rejected |
| physical_enum_v2 | enumerator | 6d7402e773d55617 | mismatch | ok | 0 | 1568 |  | rejected |
| physical_enum_v3 | enumerator | 7d454a5cd55d5f0d | exact | ok | 1568 | 0 | 0 | accepted |
| pilot_v2 | difftest | 957ccdc57ea8b573 | mismatch | ok | 0 | 301 |  | rejected |
| review_difftest_s7777 | difftest | 86b52e9a7fc3c972 | mismatch | MISMATCH | 0 | 210 |  | rejected |
| review_fuzz_v1 | - | - | - | - | 0 | 0 |  | not read |
| review_latency_probe | - | - | - | - | 0 | 0 |  | not read |
| review_scale_probe_sf0.1 | scale | c81bbce60be6c1b0 | mismatch | ok | 0 | 6 |  | rejected |
| review_scale_probe_sf1 | scale | c81bbce60be6c1b0 | mismatch | ok | 0 | 6 |  | rejected |
| review_scale_probe_sf1_norowlevel | scale | c81bbce60be6c1b0 | mismatch | ok | 0 | 4 |  | rejected |
| review_workload_share | - | - | - | - | 0 | 0 |  | not read |
| scale_v2_small | scale | cf819f85978711d2 | mismatch | ok | 0 | 60 |  | rejected |
| scale_v3_tpch | scale | eae58fcdf716e571 | mismatch | ok | 0 | 90 |  | rejected |
| scale_v4_tpch | scale | 598ab5a422684f9b | mismatch | ok | 0 | 156 |  | rejected |
| scale_v5_tpch | scale | 91c43b727578d9dd | mismatch | ok | 0 | 156 |  | rejected |
| scale_v6_tpch | scale | 3607bd40bcb44b33 | exact | ok | 156 | 0 | 0 | accepted |
| sens_compact05_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_compact05_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_compact05_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_compact1_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_compact1_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_compact1_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_hist2_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_hist2_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_hist2_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_hist4_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_hist4_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_hist4_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_parts16_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts16_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts16_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_parts32_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts32_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts32_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_parts8_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts8_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_parts8_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_sel025_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_sel025_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_sel025_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| sens_sel4_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 420 |  | rejected |
| sens_sel4_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 420 |  | rejected |
| sens_sel4_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 420 | 0 | 0 | accepted |
| smoke_implement | difftest | 86b52e9a7fc3c972 | mismatch | MISMATCH | 0 | 21 |  | rejected |
| smoke_implement_scale | scale | c81bbce60be6c1b0 | mismatch | ok | 0 | 6 |  | rejected |
| workflow_v1 | workflow | 4f0bc73f56fd121a | mismatch | ok | 0 | 211 |  | rejected |
| workflow_v2 | workflow | 8313b925498f2d11 | mismatch | ok | 0 | 211 |  | rejected |
| workflow_v3 | workflow | 96e5b23366a4f757 | exact | ok | 211 | 0 | 0 | accepted |
| writer_single_v1 | difftest | 4b0caaf0fe47d14b | mismatch | ok | 0 | 1001 |  | rejected |
| writer_single_v2 | difftest | 6dd1107f3e1d6c8d | mismatch | ok | 0 | 1001 |  | rejected |
| writer_single_v3 | difftest | bf098abf1db6c7a8 | exact | ok | 1001 | 0 | 0 | accepted |

*equivalent* (): the current code differs from the campaign code only by the top-level function `catalog.pyiceberg_reader_groups`, added in the Experiment stage for scale checks. Removing it from the current `catalog.py` gives a file whose sha256 equals the one recorded in each campaign's environment.json, and that reproduces the recorded code fingerprint. No code file of these runners references the function. Rejected directories were produced by earlier code and are not analysed. The submitted version's campaigns (`difftest_main_tpch_v2`, `difftest_main_nyc_v2`, `ablation_v2`, `fallback_filelevel_v2`, `scale_v3_tpch`, `counterexamples_v1`, `pilot_v2`, `scale_v2_small`) are superseded by the revision campaigns (code changed 2026-10-05), the 2026-10-05 revision campaigns (`ablation_v3`, `contended_sf10_v1`, `contended_unpart_v1`, `contended_v1`, `counterexamples_v2`, `difftest_main_nyc_v3`, `difftest_main_tpch_v3`, `fallback_filelevel_v3`, `history_v1`, `mixed_metadata_v1`, `physical_enum_v1`, `scale_v4_tpch`, `sens_compact05_v1`, `sens_compact1_v1`, `sens_hist2_v1`, `sens_hist4_v1`, `sens_parts16_v1`, `sens_parts32_v1`, `sens_parts8_v1`, `sens_sel025_v1`, `sens_sel4_v1`, `workflow_v1`, `writer_single_v1`) by the re-review campaigns (code changed 2026-10-07), and the re-review campaigns (`ablation_v4`, `contended_sf10_v2`, `contended_unpart_v2`, `contended_v2`, `counterexamples_v3`, `difftest_main_nyc_v4`, `difftest_main_tpch_v4`, `fallback_filelevel_v4`, `history_v2`, `mixed_metadata_v2`, `physical_enum_v2`, `scale_v5_tpch`, `sens_compact05_v2`, `sens_compact1_v2`, `sens_hist2_v2`, `sens_hist4_v2`, `sens_parts16_v2`, `sens_parts32_v2`, `sens_parts8_v2`, `sens_sel025_v2`, `sens_sel4_v2`, `workflow_v2`, `writer_single_v2`) by the minor-revision campaigns (code changed 2026-10-08; same seeds and settings throughout). The review and smoke runs predate the Experiment-stage fixes.

## 2. Oracle-check counts

| Campaign | Scenarios | True conflicts (oracle) | Writer checks pass | Merges oracle-checked (all policies) | Merges emitted (all policies) | SnapMerge merges = serial order | SnapMerge merges |
|---|---|---|---|---|---|---|---|
| difftest_main_tpch_v5 | 3003 | 1662 | 6006/6006 | 17827 | 17827 | 1341 | 1341 |
| difftest_main_nyc_v5 | 3003 | 1745 | 6006/6006 | 17121 | 17121 | 1258 | 1258 |
| ablation_v5 | 1505 | 866 | 3010/3010 | 3458 | 3458 | 639 | 639 |
| fallback_filelevel_v5 | 1001 | 559 | 2002/2002 | 2599 | 2599 | 168 | 168 |
| writer_single_v3 | 1001 | 523 | 2002/2002 | 4118 | 4118 | 478 | 478 |
| mixed_metadata_v3 | 1001 | 559 | 2002/2002 | 685 | 685 | 249 | 249 |
| sens_parts8_v3 | 420 | 212 | 840/840 | 883 | 883 | 208 | 208 |
| sens_parts16_v3 | 420 | 220 | 840/840 | 864 | 864 | 200 | 200 |
| sens_parts32_v3 | 420 | 179 | 840/840 | 1041 | 1041 | 241 | 241 |
| sens_hist2_v3 | 420 | 289 | 840/840 | 598 | 598 | 131 | 131 |
| sens_hist4_v3 | 420 | 334 | 840/840 | 411 | 411 | 86 | 86 |
| sens_sel025_v3 | 420 | 202 | 840/840 | 919 | 919 | 218 | 218 |
| sens_sel4_v3 | 420 | 226 | 840/840 | 827 | 827 | 194 | 194 |
| sens_compact05_v3 | 420 | 214 | 840/840 | 830 | 830 | 206 | 206 |
| sens_compact1_v3 | 420 | 214 | 840/840 | 769 | 769 | 206 | 206 |

scale_v5_tpch: 156 records, 126 merges, 42 oracle-checked, 42 serial-equivalent, 42 with readers agreeing. Each check makes 5 comparisons; 126 of them are derived exactly from multiset equality. Errors: 0.
counterexamples_v3: 31 cases, original property holds on 31 and the minimised property on 31, 0 errors. Every candidate is checked with the pure-Python multiset oracle, both readers and the writer check.

## 3. Table 1: differential test per mix (main campaign `difftest_main_tpch_v4`)

TPC-H SF 0.1 lineitem, base 2,000 rows, 429 scenarios per mix, seed base 20261002. Each cell gives recall / precision / unsound merges.

| Mix | n | True conf. | SnapMerge | Replay (#13959) | Iterated cherry-pick | Row-level 3-way | lakeFS policy | Nessie NORMAL | Nessie FORCE | Fast-forward only |
|---|---|---|---|---|---|---|---|---|---|---|
| All mixes | 3003 | 1662 | 1.000 / 1.000 / 0 | 0.885 / 0.652 / 0 | 0.904 / 0.611 / 0 | 0.389 / 0.997 / 196 | 1.000 / 0.590 / 0 | 1.000 / 0.553 / 0 | 0.000 / - / 2756 | 1.000 / 0.553 / 0 |
| Partition-disjoint ingest | 429 | 65 | 1.000 / 1.000 / 0 | 0.661 / 1.000 / 0 | 0.677 / 0.308 / 0 | 0.154 / 1.000 / 4 | 1.000 / 0.151 / 0 | 1.000 / 0.151 / 0 | 0.000 / - / 429 | 1.000 / 0.151 / 0 |
| Filter corrections | 429 | 391 | 1.000 / 1.000 / 0 | 0.903 / 0.978 / 0 | 0.918 / 0.942 / 0 | 0.785 / 1.000 / 9 | 1.000 / 0.911 / 0 | 1.000 / 0.911 / 0 | 0.000 / - / 416 | 1.000 / 0.911 / 0 |
| Upsert by key | 429 | 210 | 1.000 / 1.000 / 0 | 1.000 / 0.547 / 0 | 1.000 / 0.547 / 0 | 0.943 / 1.000 / 0 | 1.000 / 0.489 / 0 | 1.000 / 0.489 / 0 | 0.000 / - / 429 | 1.000 / 0.489 / 0 |
| Deletes (MOR) | 429 | 299 | 1.000 / 1.000 / 0 | 0.860 / 0.698 / 0 | 0.886 / 0.676 / 0 | 0.000 / - / 63 | 1.000 / 0.697 / 0 | 1.000 / 0.697 / 0 | 0.000 / - / 425 | 1.000 / 0.697 / 0 |
| Deletes (COW) | 429 | 278 | 1.000 / 1.000 / 0 | 0.849 / 0.661 / 0 | 0.874 / 0.625 / 0 | 0.000 / - / 53 | 1.000 / 0.648 / 0 | 1.000 / 0.648 / 0 | 0.000 / - / 422 | 1.000 / 0.648 / 0 |
| Compaction on main | 429 | 148 | 1.000 / 1.000 / 0 | 0.980 / 0.357 / 0 | 0.980 / 0.346 / 0 | 0.264 / 1.000 / 4 | 1.000 / 0.534 / 0 | 1.000 / 0.345 / 0 | 0.000 / - / 277 | 1.000 / 0.345 / 0 |
| Adversarial overlap | 429 | 271 | 1.000 / 1.000 / 0 | 0.838 / 0.674 / 0 | 0.875 / 0.669 / 0 | 0.343 / 0.979 / 63 | 1.000 / 0.684 / 0 | 1.000 / 0.632 / 0 | 0.000 / - / 358 | 1.000 / 0.632 / 0 |

### Figure 1 data: sound auto-merge rate of commuting scenarios, with 95% CI

| Mix | Commuting | SnapMerge | Replay | Cherry-pick | Row-level (sound only) | SnapMerge - replay (paired) |
|---|---|---|---|---|---|---|
| All mixes | 1341 | 1.000 [1.000, 1.000] | 0.415 [0.395, 0.435] | 0.285 [0.264, 0.305] | 0.965 [0.956, 0.974] | +0.585 [+0.565, +0.605] |
| Partition-disjoint ingest | 364 | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 0.728 [0.681, 0.772] | 1.000 [1.000, 1.000] | +0.000 [+0.000, +0.000] |
| Filter corrections | 38 | 1.000 [1.000, 1.000] | 0.789 [0.655, 0.911] | 0.421 [0.263, 0.577] | 1.000 [1.000, 1.000] | +0.210 [+0.089, +0.345] |
| Upsert by key | 219 | 1.000 [1.000, 1.000] | 0.205 [0.154, 0.259] | 0.205 [0.154, 0.259] | 1.000 [1.000, 1.000] | +0.794 [+0.741, +0.846] |
| Deletes (MOR) | 130 | 1.000 [1.000, 1.000] | 0.146 [0.087, 0.209] | 0.023 [0.000, 0.053] | 1.000 [1.000, 1.000] | +0.854 [+0.791, +0.913] |
| Deletes (COW) | 151 | 1.000 [1.000, 1.000] | 0.199 [0.135, 0.264] | 0.033 [0.007, 0.066] | 1.000 [1.000, 1.000] | +0.801 [+0.736, +0.865] |
| Compaction on main | 281 | 1.000 [1.000, 1.000] | 0.071 [0.043, 0.103] | 0.025 [0.007, 0.044] | 1.000 [1.000, 1.000] | +0.929 [+0.897, +0.957] |
| Adversarial overlap | 158 | 1.000 [1.000, 1.000] | 0.304 [0.234, 0.376] | 0.260 [0.192, 0.331] | 0.703 [0.629, 0.773] | +0.696 [+0.624, +0.765] |

## 4. Per-configuration tables (all mixes pooled)

### difftest_main_tpch_v5 (main): source tpch_lineitem SF 0.1, base 2000 rows, 429 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 1662/0/0/1341 | 0 | 1341/1341 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99725, 1.0]) | 18.4 [17.9, 18.8] | 63.9 |
| SnapMerge w/o net-effect test | 1662/64/0/1277 | 0 | 1277/1277 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.963 [0.954, 0.972] | 0.952 [0.941, 0.963] | 13.6 [13.4, 13.8] | 39.4 |
| SnapMerge w/o compaction rebase | 1662/300/0/1041 | 0 | 1041/1041 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.847 [0.835, 0.859] | 0.776 [0.760, 0.792] | 15.4 [15.1, 15.7] | 55.1 |
| SnapMerge w/o delete union | 1662/448/0/893 | 0 | 893/893 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.788 [0.770, 0.804] | 0.666 [0.644, 0.688] | 15.7 [15.4, 16.1] | 35.5 |
| SnapMerge w/o bounded reads | 1662/725/0/616 | 0 | 616/616 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.696 [0.680, 0.713] | 0.459 [0.440, 0.479] | 9.9 [9.7, 10.0] | 0.0 |
| SnapMerge file-level rules only | 1662/695/0/646 | 0 | 646/646 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.705 [0.689, 0.721] | 0.482 [0.458, 0.505] | 9.6 [9.4, 9.8] | 13.6 |
| Replay (#13959) | 1471/785/191/556 | 0 | 747/747 | 0 | 0 | 0.885 [0.870, 0.900] | 0.652 [0.634, 0.670] | 0.415 [0.395, 0.435] | 9.7 [9.5, 9.9] | 0.0 |
| Iterated cherry-pick | 1503/959/159/382 | 0 | 541/541 | 0 | 0 | 0.904 [0.890, 0.918] | 0.611 [0.592, 0.629] | 0.285 [0.264, 0.305] | 9.1 [8.9, 9.3] | 0.0 |
| Row-level 3-way | 647/2/1015/1339 | 0 | 2158/2354 | 196 | 0 | 0.389 [0.371, 0.408] | 0.997 [0.992, 1.000] | 0.965 [0.956, 0.974] | 40.8 [40.5, 41.0] | 371.5 |
| lakeFS policy | 1662/1156/0/185 | 0 | 185/185 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.590 [0.574, 0.606] | 0.138 [0.122, 0.153] | 8.2 [8.1, 8.3] | 0.0 |
| Nessie NORMAL | 1662/1341/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.553 [0.537, 0.569] | 0.000 [0.000, 0.000] (exact [0.0, 0.00275]) | 7.9 [7.8, 8.0] | 0.0 |
| Nessie FORCE | 0/0/1662/1341 | 0 | 247/3003 | 2756 | 0 | 0.000 [0.000, 0.000] (exact [0.0, 0.00222]) | n/a | 0.156 [0.139, 0.173] | 11.5 [11.3, 11.6] | 0.0 |
| Fast-forward only | 1662/1341/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.553 [0.537, 0.569] | 0.000 [0.000, 0.000] (exact [0.0, 0.00275]) | 7.9 [7.8, 8.0] | 0.0 |
| Rebase-OCC | 1393/596/269/745 | 0 | 1014/1014 | 0 | 0 | 0.838 [0.821, 0.855] | 0.700 [0.682, 0.718] | 0.556 [0.535, 0.577] | 12.6 [12.4, 12.8] | 6.6 |
| Recompute | 1662/0/0/1341 | 0 | 1341/1341 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99725, 1.0]) | 40.8 [40.3, 41.2] | 167.6 |
| Intent replay (pairwise) | 1662/600/0/741 | 0 | 741/741 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.735 [0.718, 0.751] | 0.553 [0.531, 0.574] | 11.9 [11.6, 12.1] | 9.3 |
| Exact replay | 1662/595/0/746 | 0 | 746/746 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 0.736 [0.720, 0.752] | 0.556 [0.535, 0.578] | 15.4 [15.1, 15.8] | 27.5 |
| SnapMerge, append-snapshot insert rule | 1662/0/0/1341 | 0 | 1341/1341 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99778, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99725, 1.0]) | 18.4 [17.8, 18.9] | 63.9 |

Load average (1 min) during policies: {'min': 7.23, 'median': 10.14, 'max': 22.81}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 18 policies).

### difftest_main_nyc_v5 (second_source): source nyc_yellow months 2023-01, partition tpep_pickup_datetime/day, base 2000 rows, 429 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 1745/0/0/1258 | 0 | 1258/1258 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99707, 1.0]) | 13.9 [13.6, 14.2] | 47.1 |
| SnapMerge w/o net-effect test | 1745/53/0/1205 | 0 | 1205/1205 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.971 [0.963, 0.978] | 0.958 [0.947, 0.968] | 10.8 [10.6, 11.1] | 29.6 |
| SnapMerge w/o compaction rebase | 1745/267/0/991 | 0 | 991/991 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.867 [0.856, 0.878] | 0.788 [0.771, 0.804] | 12.3 [12.0, 12.6] | 40.1 |
| SnapMerge w/o delete union | 1745/439/0/819 | 0 | 819/819 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.799 [0.783, 0.815] | 0.651 [0.629, 0.673] | 12.3 [12.1, 12.6] | 26.3 |
| SnapMerge w/o bounded reads | 1745/680/0/578 | 0 | 578/578 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.720 [0.703, 0.736] | 0.460 [0.439, 0.480] | 8.2 [8.1, 8.4] | 0.0 |
| SnapMerge file-level rules only | 1745/672/0/586 | 0 | 586/586 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.722 [0.706, 0.737] | 0.466 [0.441, 0.491] | 8.0 [7.9, 8.1] | 10.0 |
| Replay (#13959) | 1518/752/227/506 | 0 | 733/733 | 0 | 0 | 0.870 [0.854, 0.886] | 0.669 [0.650, 0.686] | 0.402 [0.382, 0.423] | 8.2 [8.0, 8.3] | 0.0 |
| Iterated cherry-pick | 1575/918/170/340 | 0 | 510/510 | 0 | 0 | 0.903 [0.889, 0.916] | 0.632 [0.614, 0.649] | 0.270 [0.249, 0.291] | 7.7 [7.6, 7.8] | 0.0 |
| Row-level 3-way | 643/0/1102/1258 | 0 | 2177/2360 | 183 | 0 | 0.368 [0.351, 0.386] | 1.000 [1.000, 1.000] | 0.967 [0.958, 0.976] | 27.9 [27.8, 28.1] | 279.5 |
| lakeFS policy | 1745/1101/0/157 | 0 | 157/157 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.613 [0.597, 0.629] | 0.125 [0.109, 0.141] | 6.9 [6.8, 7.0] | 0.0 |
| Nessie NORMAL | 1745/1258/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.581 [0.565, 0.596] | 0.000 [0.000, 0.000] (exact [0.0, 0.00293]) | 6.7 [6.6, 6.8] | 0.0 |
| Nessie FORCE | 0/0/1745/1258 | 0 | 218/3003 | 2785 | 0 | 0.000 [0.000, 0.000] (exact [0.0, 0.00211]) | n/a | 0.144 [0.127, 0.161] | 9.4 [9.3, 9.5] | 0.0 |
| Fast-forward only | 1745/1258/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.581 [0.565, 0.596] | 0.000 [0.000, 0.000] (exact [0.0, 0.00293]) | 6.8 [6.7, 6.9] | 0.0 |
| Rebase-OCC | 1434/561/311/697 | 0 | 1008/1008 | 0 | 0 | 0.822 [0.804, 0.840] | 0.719 [0.701, 0.737] | 0.554 [0.532, 0.576] | 10.1 [10.0, 10.3] | 4.6 |
| Recompute | 1745/0/0/1258 | 0 | 1258/1258 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99707, 1.0]) | 27.9 [27.6, 28.3] | 127.3 |
| Intent replay (pairwise) | 1745/562/0/696 | 0 | 696/696 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.756 [0.740, 0.772] | 0.553 [0.531, 0.575] | 9.7 [9.6, 9.9] | 6.4 |
| Exact replay | 1745/557/0/701 | 0 | 701/701 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 0.758 [0.742, 0.774] | 0.557 [0.535, 0.579] | 12.1 [11.9, 12.4] | 19.9 |
| SnapMerge, append-snapshot insert rule | 1745/0/0/1258 | 0 | 1258/1258 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99789, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99707, 1.0]) | 13.9 [13.6, 14.2] | 47.1 |

Load average (1 min) during policies: {'min': 3.16, 'median': 6.79, 'max': 10.3}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 18 policies).

### ablation_v5 (ablation): source tpch_lineitem SF 0.1, base 2000 rows, 215 per mix, seed base 20261003, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 866/0/0/639 | 0 | 639/639 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99424, 1.0]) | 15.2 [14.6, 15.7] | 67.8 |
| SnapMerge w/o net-effect test | 866/41/0/598 | 0 | 598/598 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.955 [0.941, 0.968] | 0.936 [0.917, 0.954] | 11.7 [11.4, 12.0] | 39.0 |
| SnapMerge w/o compaction rebase | 866/142/0/497 | 0 | 497/497 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.859 [0.843, 0.875] | 0.778 [0.754, 0.801] | 13.2 [12.7, 13.6] | 59.5 |
| SnapMerge w/o delete union | 866/229/0/410 | 0 | 410/410 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.791 [0.768, 0.814] | 0.642 [0.611, 0.674] | 13.3 [13.0, 13.7] | 36.5 |
| SnapMerge w/o bounded reads | 866/356/0/283 | 0 | 283/283 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.709 [0.685, 0.732] | 0.443 [0.415, 0.472] | 8.7 [8.5, 8.9] | 0.0 |
| Replay (#13959) | 771/388/95/251 | 0 | 346/346 | 0 | 0 | 0.890 [0.870, 0.910] | 0.665 [0.640, 0.690] | 0.393 [0.366, 0.421] | 8.7 [8.5, 8.9] | 0.0 |
| Intent replay (pairwise) | 866/297/0/342 | 0 | 342/342 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.745 [0.722, 0.767] | 0.535 [0.506, 0.566] | 10.4 [10.1, 10.7] | 9.2 |
| Exact replay | 866/296/0/343 | 0 | 343/343 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99575, 1.0]) | 0.745 [0.723, 0.768] | 0.537 [0.507, 0.568] | 13.0 [12.7, 13.3] | 28.8 |

Load average (1 min) during policies: {'min': 4.33, 'median': 5.87, 'max': 7.19}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 8 policies).

### fallback_filelevel_v5 (fallback): source tpch_lineitem SF 0.1, base 2000 rows, 143 per mix, seed base 20261004, writers record logical metadata: False

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 559/274/0/168 | 0 | 168/168 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.671 [0.643, 0.700] | 0.380 [0.339, 0.422] | 8.4 [8.2, 8.6] | 5.4 |
| SnapMerge file-level rules only | 559/274/0/168 | 0 | 168/168 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.671 [0.643, 0.700] | 0.380 [0.339, 0.422] | 8.3 [8.1, 8.5] | 5.4 |
| Replay (#13959) | 496/246/63/196 | 0 | 259/259 | 0 | 0 | 0.887 [0.861, 0.913] | 0.668 [0.637, 0.701] | 0.443 [0.407, 0.481] | 9.0 [8.8, 9.3] | 0.0 |
| Iterated cherry-pick | 517/321/42/121 | 0 | 163/163 | 0 | 0 | 0.925 [0.903, 0.947] | 0.617 [0.586, 0.648] | 0.274 [0.237, 0.312] | 8.3 [8.1, 8.5] | 0.0 |
| Row-level 3-way | 216/0/343/442 | 0 | 724/785 | 61 | 0 | 0.386 [0.357, 0.418] | 1.000 [1.000, 1.000] | 0.959 [0.941, 0.977] | 35.4 [35.1, 35.7] | 368.7 |
| lakeFS policy | 559/387/0/55 | 0 | 55/55 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.591 [0.564, 0.619] | 0.124 [0.097, 0.152] | 7.4 [7.2, 7.6] | 0.0 |
| Nessie NORMAL | 559/442/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.558 [0.531, 0.585] | 0.000 [0.000, 0.000] (exact [0.0, 0.00831]) | 7.2 [7.0, 7.4] | 0.0 |
| Nessie FORCE | 0/0/559/442 | 0 | 76/1001 | 925 | 0 | 0.000 [0.000, 0.000] (exact [0.0, 0.00658]) | n/a | 0.129 [0.101, 0.158] | 9.8 [9.6, 10.0] | 0.0 |
| Fast-forward only | 559/442/0/0 | 0 | 0/0 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.558 [0.531, 0.585] | 0.000 [0.000, 0.000] (exact [0.0, 0.00831]) | 7.1 [6.9, 7.3] | 0.0 |

Load average (1 min) during policies: {'min': 4.73, 'median': 6.57, 'max': 11.7}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 9 policies).

### writer_single_v3 (writer_single): source tpch_lineitem SF 0.1, base 2000 rows, 143 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 523/0/0/478 | 0 | 478/478 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99297, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99231, 1.0]) | 19.6 [18.5, 20.6] | 164.4 |
| SnapMerge w/o net-effect test | 523/18/0/460 | 0 | 460/460 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99297, 1.0]) | 0.967 [0.951, 0.981] | 0.962 [0.945, 0.978] | 15.6 [13.8, 16.7] | 124.5 |
| Replay (#13959) | 452/279/71/199 | 0 | 270/270 | 0 | 0 | 0.864 [0.835, 0.893] | 0.618 [0.585, 0.651] | 0.416 [0.383, 0.450] | 6.8 [6.7, 7.0] | 0.0 |
| Iterated cherry-pick | 465/334/58/144 | 0 | 202/202 | 0 | 0 | 0.889 [0.861, 0.915] | 0.582 [0.549, 0.614] | 0.301 [0.267, 0.336] | 6.6 [6.5, 6.7] | 0.0 |
| Row-level 3-way | 199/0/324/478 | 0 | 725/802 | 77 | 0 | 0.381 [0.346, 0.414] | 1.000 [1.000, 1.000] | 0.967 [0.950, 0.981] | 36.3 [35.7, 36.8] | 341.8 |
| Rebase-OCC | 432/212/91/266 | 0 | 357/357 | 0 | 0 | 0.826 [0.794, 0.857] | 0.671 [0.637, 0.703] | 0.556 [0.521, 0.592] | 13.8 [13.2, 14.5] | 91.6 |
| Recompute | 523/0/0/478 | 0 | 478/478 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99297, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.99231, 1.0]) | 40.7 [39.7, 42.2] | 226.8 |
| Intent replay (pairwise) | 523/213/0/265 | 0 | 265/265 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99297, 1.0]) | 0.711 [0.681, 0.739] | 0.554 [0.520, 0.591] | 12.2 [11.5, 13.2] | 95.3 |
| Exact replay | 523/212/0/266 | 0 | 266/266 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99297, 1.0]) | 0.712 [0.682, 0.740] | 0.556 [0.521, 0.592] | 16.1 [15.2, 17.4] | 131.0 |
| SnapMerge, append-snapshot insert rule | 460/1/63/477 | 0 | 505/540 | 35 | 0 | 0.879 [0.853, 0.906] | 0.998 [0.993, 1.000] | 0.998 [0.994, 1.000] | 14.2 [13.5, 15.3] | 64.7 |

Load average (1 min) during policies: {'min': 7.59, 'median': 10.6, 'max': 39.71}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 10 policies).

### mixed_metadata_v3 (mixed_metadata): source tpch_lineitem SF 0.1, base 2000 rows, 143 per mix, seed base 20261004, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 559/193/0/249 | 0 | 249/249 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.743 [0.715, 0.772] | 0.563 [0.520, 0.608] | 11.8 [11.5, 12.1] | 14.5 |
| Replay (#13959) | 496/246/63/196 | 0 | 259/259 | 0 | 0 | 0.887 [0.861, 0.913] | 0.668 [0.638, 0.700] | 0.443 [0.407, 0.481] | 10.3 [10.0, 10.7] | 0.0 |
| Intent replay (pairwise) | 559/265/0/177 | 0 | 177/177 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.99342, 1.0]) | 0.678 [0.650, 0.707] | 0.401 [0.359, 0.443] | 10.6 [10.1, 10.9] | 2.5 |

Load average (1 min) during policies: {'min': 11.28, 'median': 24.88, 'max': 67.41}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 3 policies).

### sens_parts8_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 212/0/0/208 | 0 | 208/208 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98275, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98242, 1.0]) | 18.4 [17.2, 19.9] | 87.9 |
| SnapMerge w/o net-effect test | 212/10/0/198 | 0 | 198/198 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98275, 1.0]) | 0.955 [0.927, 0.979] | 0.952 [0.923, 0.977] | 14.3 [13.6, 14.9] | 56.2 |
| Replay (#13959) | 193/120/19/88 | 0 | 107/107 | 0 | 0 | 0.910 [0.871, 0.946] | 0.617 [0.564, 0.667] | 0.423 [0.373, 0.474] | 10.3 [9.8, 10.6] | 0.0 |
| Rebase-OCC | 184/94/28/114 | 0 | 142/142 | 0 | 0 | 0.868 [0.822, 0.910] | 0.662 [0.609, 0.713] | 0.548 [0.496, 0.602] | 13.0 [12.1, 13.8] | 7.1 |
| Intent replay (pairwise) | 212/94/0/114 | 0 | 114/114 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98275, 1.0]) | 0.693 [0.645, 0.740] | 0.548 [0.496, 0.602] | 12.5 [11.8, 13.2] | 12.2 |
| Exact replay | 212/94/0/114 | 0 | 114/114 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98275, 1.0]) | 0.693 [0.645, 0.740] | 0.548 [0.496, 0.602] | 15.9 [15.1, 17.1] | 33.7 |

Load average (1 min) during policies: {'min': 7.33, 'median': 9.1, 'max': 17.27}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_parts16_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 220/0/0/200 | 0 | 200/200 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98337, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98172, 1.0]) | 21.4 [19.8, 22.8] | 98.4 |
| SnapMerge w/o net-effect test | 220/12/0/188 | 0 | 188/188 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98337, 1.0]) | 0.948 [0.919, 0.974] | 0.940 [0.908, 0.970] | 16.2 [15.4, 17.2] | 45.7 |
| Replay (#13959) | 194/114/26/86 | 0 | 112/112 | 0 | 0 | 0.882 [0.839, 0.922] | 0.630 [0.580, 0.678] | 0.430 [0.382, 0.481] | 11.7 [10.9, 12.5] | 0.0 |
| Rebase-OCC | 186/90/34/110 | 0 | 144/144 | 0 | 0 | 0.846 [0.798, 0.890] | 0.674 [0.625, 0.722] | 0.550 [0.498, 0.604] | 14.5 [13.7, 15.0] | 8.4 |
| Intent replay (pairwise) | 220/90/0/110 | 0 | 110/110 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98337, 1.0]) | 0.710 [0.666, 0.753] | 0.550 [0.498, 0.604] | 14.3 [13.0, 14.9] | 11.2 |
| Exact replay | 220/90/0/110 | 0 | 110/110 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98337, 1.0]) | 0.710 [0.666, 0.753] | 0.550 [0.498, 0.604] | 18.5 [16.7, 19.7] | 41.9 |

Load average (1 min) during policies: {'min': 7.84, 'median': 9.65, 'max': 12.29}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_parts32_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 179/0/0/241 | 0 | 241/241 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9796, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98481, 1.0]) | 28.6 [26.2, 30.2] | 118.2 |
| SnapMerge w/o net-effect test | 179/13/0/228 | 0 | 228/228 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9796, 1.0]) | 0.932 [0.897, 0.965] | 0.946 [0.918, 0.972] | 23.0 [21.9, 24.5] | 53.1 |
| Replay (#13959) | 151/145/28/96 | 0 | 124/124 | 0 | 0 | 0.844 [0.788, 0.894] | 0.510 [0.459, 0.563] | 0.398 [0.354, 0.444] | 15.8 [15.0, 16.7] | 0.0 |
| Rebase-OCC | 143/104/36/137 | 0 | 173/173 | 0 | 0 | 0.799 [0.740, 0.854] | 0.579 [0.524, 0.634] | 0.569 [0.521, 0.619] | 19.4 [18.1, 21.0] | 8.9 |
| Intent replay (pairwise) | 179/104/0/137 | 0 | 137/137 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9796, 1.0]) | 0.632 [0.583, 0.682] | 0.569 [0.521, 0.619] | 18.7 [17.5, 20.4] | 11.8 |
| Exact replay | 179/103/0/138 | 0 | 138/138 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9796, 1.0]) | 0.635 [0.586, 0.684] | 0.573 [0.526, 0.624] | 23.6 [22.2, 24.9] | 40.5 |

Load average (1 min) during policies: {'min': 6.93, 'median': 23.27, 'max': 35.82}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_hist2_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 289/0/0/131 | 0 | 131/131 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98732, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.97223, 1.0]) | 27.6 [26.2, 29.3] | 89.2 |
| SnapMerge w/o net-effect test | 289/5/0/126 | 0 | 126/126 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98732, 1.0]) | 0.983 [0.967, 0.997] | 0.962 [0.927, 0.992] | 18.6 [17.7, 20.1] | 35.3 |
| Replay (#13959) | 264/70/25/61 | 0 | 86/86 | 0 | 0 | 0.913 [0.880, 0.944] | 0.790 [0.749, 0.831] | 0.466 [0.407, 0.530] | 13.9 [12.9, 14.9] | 0.0 |
| Rebase-OCC | 253/58/36/73 | 0 | 109/109 | 0 | 0 | 0.875 [0.839, 0.911] | 0.814 [0.772, 0.854] | 0.557 [0.492, 0.628] | 17.5 [16.3, 18.1] | 7.5 |
| Intent replay (pairwise) | 289/59/0/72 | 0 | 72/72 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98732, 1.0]) | 0.831 [0.793, 0.867] | 0.550 [0.485, 0.620] | 15.9 [15.1, 17.1] | 10.3 |
| Exact replay | 289/57/0/74 | 0 | 74/74 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98732, 1.0]) | 0.835 [0.799, 0.871] | 0.565 [0.500, 0.634] | 26.2 [23.7, 27.1] | 61.7 |

Load average (1 min) during policies: {'min': 6.18, 'median': 11.08, 'max': 23.25}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_hist4_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 334/0/0/86 | 0 | 86/86 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98902, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.95801, 1.0]) | 50.2 [46.6, 53.1] | 146.7 |
| SnapMerge w/o net-effect test | 334/9/0/77 | 0 | 77/77 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98902, 1.0]) | 0.974 [0.956, 0.989] | 0.895 [0.830, 0.955] | 29.9 [27.8, 32.3] | 24.9 |
| Replay (#13959) | 318/38/16/48 | 0 | 64/64 | 0 | 0 | 0.952 [0.929, 0.973] | 0.893 [0.860, 0.923] | 0.558 [0.477, 0.646] | 22.5 [21.0, 23.9] | 0.0 |
| Rebase-OCC | 307/34/27/52 | 0 | 79/79 | 0 | 0 | 0.919 [0.890, 0.946] | 0.900 [0.868, 0.931] | 0.605 [0.520, 0.695] | 26.9 [24.9, 28.7] | 10.6 |
| Intent replay (pairwise) | 334/34/0/52 | 0 | 52/52 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98902, 1.0]) | 0.908 [0.877, 0.935] | 0.605 [0.520, 0.695] | 26.0 [24.0, 27.4] | 12.4 |
| Exact replay | 334/33/0/53 | 0 | 53/53 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98902, 1.0]) | 0.910 [0.881, 0.938] | 0.616 [0.531, 0.705] | 54.3 [47.7, 58.4] | 125.6 |

Load average (1 min) during policies: {'min': 5.42, 'median': 20.34, 'max': 43.16}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_sel025_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 202/0/0/218 | 0 | 218/218 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9819, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98322, 1.0]) | 17.3 [15.6, 18.1] | 70.8 |
| SnapMerge w/o net-effect test | 202/7/0/211 | 0 | 211/211 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9819, 1.0]) | 0.967 [0.941, 0.990] | 0.968 [0.944, 0.990] | 12.8 [12.2, 13.7] | 52.5 |
| Replay (#13959) | 169/132/33/86 | 0 | 119/119 | 0 | 0 | 0.837 [0.785, 0.885] | 0.561 [0.511, 0.612] | 0.395 [0.349, 0.443] | 9.2 [8.7, 9.7] | 0.0 |
| Rebase-OCC | 163/107/39/111 | 0 | 150/150 | 0 | 0 | 0.807 [0.753, 0.859] | 0.604 [0.552, 0.654] | 0.509 [0.459, 0.560] | 11.0 [10.6, 11.9] | 5.4 |
| Intent replay (pairwise) | 202/108/0/110 | 0 | 110/110 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9819, 1.0]) | 0.652 [0.605, 0.697] | 0.505 [0.454, 0.556] | 10.4 [10.0, 11.1] | 8.5 |
| Exact replay | 202/107/0/111 | 0 | 111/111 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.9819, 1.0]) | 0.654 [0.607, 0.699] | 0.509 [0.459, 0.560] | 13.3 [12.5, 14.2] | 23.1 |

Load average (1 min) during policies: {'min': 5.84, 'median': 7.65, 'max': 10.59}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_sel4_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 226/0/0/194 | 0 | 194/194 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98381, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98116, 1.0]) | 16.9 [15.4, 17.9] | 69.2 |
| SnapMerge w/o net-effect test | 226/10/0/184 | 0 | 184/184 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98381, 1.0]) | 0.958 [0.930, 0.980] | 0.949 [0.916, 0.976] | 12.2 [11.5, 13.1] | 45.8 |
| Replay (#13959) | 193/114/33/80 | 0 | 113/113 | 0 | 0 | 0.854 [0.806, 0.898] | 0.629 [0.579, 0.676] | 0.412 [0.364, 0.465] | 8.9 [8.5, 9.4] | 0.0 |
| Rebase-OCC | 187/95/39/99 | 0 | 138/138 | 0 | 0 | 0.827 [0.778, 0.874] | 0.663 [0.614, 0.712] | 0.510 [0.456, 0.566] | 10.9 [10.3, 11.7] | 5.1 |
| Intent replay (pairwise) | 226/96/0/98 | 0 | 98/98 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98381, 1.0]) | 0.702 [0.658, 0.745] | 0.505 [0.452, 0.561] | 10.1 [9.6, 10.8] | 8.1 |
| Exact replay | 226/94/0/100 | 0 | 100/100 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98381, 1.0]) | 0.706 [0.663, 0.749] | 0.515 [0.462, 0.571] | 13.5 [12.5, 14.6] | 27.3 |

Load average (1 min) during policies: {'min': 6.5, 'median': 7.12, 'max': 8.4}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_compact05_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 214/0/0/206 | 0 | 206/206 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98225, 1.0]) | 17.9 [16.9, 18.9] | 74.3 |
| SnapMerge w/o net-effect test | 214/9/0/197 | 0 | 197/197 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.960 [0.933, 0.983] | 0.956 [0.927, 0.981] | 13.2 [12.3, 14.1] | 52.9 |
| Replay (#13959) | 179/129/35/77 | 0 | 112/112 | 0 | 0 | 0.836 [0.786, 0.884] | 0.581 [0.530, 0.631] | 0.374 [0.323, 0.426] | 9.2 [8.8, 9.7] | 0.0 |
| Rebase-OCC | 174/114/40/92 | 0 | 132/132 | 0 | 0 | 0.813 [0.761, 0.863] | 0.604 [0.553, 0.655] | 0.447 [0.391, 0.502] | 11.1 [10.6, 11.5] | 5.1 |
| Intent replay (pairwise) | 214/115/0/91 | 0 | 91/91 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.650 [0.604, 0.696] | 0.442 [0.387, 0.498] | 10.1 [9.7, 10.6] | 8.2 |
| Exact replay | 214/114/0/92 | 0 | 92/92 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.652 [0.606, 0.698] | 0.447 [0.391, 0.502] | 13.0 [12.2, 13.8] | 25.1 |

Load average (1 min) during policies: {'min': 5.4, 'median': 5.85, 'max': 7.08}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

### sens_compact1_v3 (sensitivity): source tpch_lineitem SF 0.1, base 2000 rows, 60 per mix, seed base 20261002, writers record logical metadata: True

| Policy | TP/FP/FN/TN | Errors | Merges = serial order / merges | Unsound merges | Reader disagreements | Recall [95% CI] | Precision [95% CI] | Sound auto-merge of commuting [95% CI] | Median latency ms [95% CI] | Mean data+delete KB read |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 214/0/0/206 | 0 | 206/206 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] (exact [0.98225, 1.0]) | 19.3 [17.3, 20.6] | 79.1 |
| SnapMerge w/o net-effect test | 214/9/0/197 | 0 | 197/197 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.960 [0.932, 0.983] | 0.956 [0.926, 0.981] | 13.5 [12.8, 14.7] | 56.9 |
| Replay (#13959) | 181/137/33/69 | 0 | 102/102 | 0 | 0 | 0.846 [0.796, 0.892] | 0.569 [0.519, 0.618] | 0.335 [0.282, 0.389] | 9.6 [9.2, 10.1] | 0.0 |
| Rebase-OCC | 178/130/36/76 | 0 | 112/112 | 0 | 0 | 0.832 [0.781, 0.881] | 0.578 [0.528, 0.627] | 0.369 [0.314, 0.425] | 11.1 [10.5, 11.6] | 5.1 |
| Intent replay (pairwise) | 214/130/0/76 | 0 | 76/76 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.622 [0.576, 0.668] | 0.369 [0.314, 0.425] | 10.5 [9.9, 11.1] | 8.2 |
| Exact replay | 214/130/0/76 | 0 | 76/76 | 0 | 0 | 1.000 [1.000, 1.000] (exact [0.98291, 1.0]) | 0.622 [0.576, 0.668] | 0.369 [0.314, 0.425] | 12.7 [11.9, 13.4] | 25.1 |

Load average (1 min) during policies: {'min': 4.88, 'median': 5.6, 'max': 6.37}. MEMGUARD batch scales seen: ['1.0']. Cross-check against `difftest.policy_metrics` on the same records: ok (0 mismatches over 6 policies).

## 5. Paired comparisons against SnapMerge (main campaign, all mixes)

| Policy | SnapMerge - policy: sound auto-merge of commuting | SnapMerge - policy: recall | Latency policy/SnapMerge, both merged soundly (geomean) | Latency policy/SnapMerge, all scenarios (time to decision) |
|---|---|---|---|---|
| SnapMerge w/o net-effect test | +0.048 [+0.037, +0.059] | +0.000 [+0.000, +0.000] | 1.01 [1.00, 1.02] (n=1277) | 0.77 [0.76, 0.79] (n=3003) |
| SnapMerge w/o compaction rebase | +0.224 [+0.208, +0.239] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=1041) | 0.87 [0.86, 0.88] (n=3003) |
| SnapMerge w/o delete union | +0.334 [+0.312, +0.356] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=893) | 0.82 [0.81, 0.84] (n=3003) |
| SnapMerge w/o bounded reads | +0.541 [+0.521, +0.560] | +0.000 [+0.000, +0.000] | 1.00 [0.98, 1.01] (n=616) | 0.50 [0.49, 0.51] (n=3003) |
| SnapMerge file-level rules only | +0.518 [+0.495, +0.542] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=646) | 0.53 [0.52, 0.54] (n=3003) |
| Replay (#13959) | +0.585 [+0.565, +0.605] | +0.115 [+0.100, +0.130] | 1.31 [1.28, 1.35] (n=556) | 0.52 [0.51, 0.53] (n=3003) |
| Iterated cherry-pick | +0.715 [+0.695, +0.736] | +0.096 [+0.082, +0.110] | 1.37 [1.32, 1.42] (n=382) | 0.49 [0.48, 0.50] (n=3003) |
| Row-level 3-way | +0.035 [+0.026, +0.044] | +0.611 [+0.592, +0.629] | 1.84 [1.78, 1.89] (n=1294) | 2.05 [2.02, 2.09] (n=3003) |
| lakeFS policy | +0.862 [+0.847, +0.878] | +0.000 [+0.000, +0.000] | 0.36 [0.34, 0.37] (n=185) | 0.42 [0.41, 0.43] (n=3003) |
| Nessie NORMAL | +1.000 [+1.000, +1.000] | +0.000 [+0.000, +0.000] | n/a | 0.41 [0.41, 0.42] (n=3003) |
| Nessie FORCE | +0.844 [+0.827, +0.861] | +1.000 [+1.000, +1.000] | 0.35 [0.33, 0.37] (n=209) | 0.60 [0.59, 0.61] (n=3003) |
| Fast-forward only | +1.000 [+1.000, +1.000] | +0.000 [+0.000, +0.000] | n/a | 0.41 [0.40, 0.42] (n=3003) |
| Rebase-OCC | +0.444 [+0.423, +0.465] | +0.162 [+0.145, +0.179] | 1.35 [1.32, 1.38] (n=745) | 0.65 [0.64, 0.67] (n=3003) |
| Recompute | +0.000 [+0.000, +0.000] | +0.000 [+0.000, +0.000] | 2.08 [2.02, 2.14] (n=1341) | 2.11 [2.08, 2.15] (n=3003) |
| Intent replay (pairwise) | +0.447 [+0.426, +0.469] | +0.000 [+0.000, +0.000] | 1.40 [1.37, 1.43] (n=741) | 0.63 [0.62, 0.65] (n=3003) |
| Exact replay | +0.444 [+0.422, +0.465] | +0.000 [+0.000, +0.000] | 1.41 [1.38, 1.43] (n=746) | 0.80 [0.78, 0.81] (n=3003) |
| SnapMerge, append-snapshot insert rule | +0.000 [+0.000, +0.000] | +0.000 [+0.000, +0.000] | 1.01 [1.00, 1.02] (n=1341) | 1.00 [0.99, 1.00] (n=3003) |

## 6. Table 2: silent loss of FORCE / source-wins merges per mix (main campaign)

| Mix | FORCE merges | Equal neither order | Lose rows vs both orders | Rows lost (vs closer order) | Rows extra (vs closer order) |
|---|---|---|---|---|---|
| All mixes | 3003 | 2756 | 2052 | 239665 | 650978 |
| Partition-disjoint ingest | 429 | 429 | 397 | 73770 | 62015 |
| Filter corrections | 429 | 416 | 395 | 115483 | 172476 |
| Upsert by key | 429 | 429 | 429 | 24361 | 11290 |
| Deletes (MOR) | 429 | 425 | 218 | 7347 | 155493 |
| Deletes (COW) | 429 | 422 | 197 | 5811 | 155837 |
| Compaction on main | 429 | 277 | 159 | 5607 | 53855 |
| Adversarial overlap | 429 | 358 | 257 | 7286 | 40012 |

## 7. Merge cost versus table size (`scale_v5_tpch`, TPC-H lineitem; isolated processes, seeded random order)

Latency in ms: mean [Student-t 95% CI], median, min-max. Bytes are medians. Metadata bytes written are approximate (F12). Replay's overlap rows give the time to report a (false) partition conflict, not a merge.

| SF | Kind | Policy | Verdicts | Oracle pass | Latency mean [t-CI] | Median | Min-max | Data+delete MB read, median (min-max) | Metadata MB read | Data+delete MB written | Metadata KB written (approx) | Load avg |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.1 | disjoint | Recompute | merged 3 | 2/2 | 972.2 [960.7, 983.6] | 970.0 | 969.0-977.5 | 13.650 (13.650-13.650) | 0.215 | 14.469 | 107.4 | 12.9 |
| 0.1 | disjoint | Replay (#13959) | merged 10 | 2/2 | 19.6 [17.4, 21.7] | 19.3 | 14.3-23.4 | 0.000 (0.000-0.000) | 0.362 | 0.000 | 15.4 | 10.7 |
| 0.1 | disjoint | Row-level 3-way | merged 3 | 2/2 | 910.5 [900.0, 921.1] | 912.4 | 905.6-913.5 | 40.888 (40.887-40.888) | 0.215 | 14.469 | 107.2 | 12.2 |
| 0.1 | disjoint | SnapMerge | merged 10 | 2/2 | 14.7 [13.4, 15.9] | 14.3 | 12.5-18.1 | 0.000 (0.000-0.000) | 0.215 | 0.000 | 13.3 | 10.7 |
| 0.1 | overlap | Recompute | merged 3 | 2/2 | 971.7 [912.5, 1030.8] | 982.3 | 944.4-988.3 | 13.630 (13.630-13.630) | 0.243 | 14.496 | 101.3 | 10.0 |
| 0.1 | overlap | Replay (#13959) | conflict 10 | 0/0 | 6.3 [6.1, 6.6] | 6.3 | 5.7-6.8 | 0.000 (0.000-0.000) | 0.243 | 0.000 | 0.0 | 9.8 |
| 0.1 | overlap | Row-level 3-way | merged 3 | 2/2 | 895.1 [856.9, 933.2] | 901.0 | 877.6-906.6 | 40.870 (40.868-40.872) | 0.243 | 14.496 | 101.1 | 10.0 |
| 0.1 | overlap | SnapMerge | merged 10 | 2/2 | 36.4 [34.5, 38.2] | 36.6 | 32.0-40.2 | 0.380 (0.184-0.401) | 0.243 | 0.009 | 13.1 | 9.7 |
| 1 | disjoint | Recompute | merged 3 | 2/2 | 8317.5 [7839.1, 8795.9] | 8309.3 | 8129.1-8514.0 | 151.292 (151.292-151.294) | 0.690 | 160.837 | 253.3 | 10.1 |
| 1 | disjoint | Replay (#13959) | merged 10 | 2/2 | 27.7 [25.6, 29.8] | 27.9 | 23.5-33.2 | 0.000 (0.000-0.000) | 1.155 | 0.000 | 16.6 | 8.3 |
| 1 | disjoint | Row-level 3-way | merged 3 | 2/2 | 8363.5 [7936.1, 8790.8] | 8392.4 | 8178.8-8519.2 | 453.420 (453.420-453.423) | 0.690 | 160.837 | 252.9 | 9.6 |
| 1 | disjoint | SnapMerge | merged 10 | 2/2 | 21.0 [19.9, 22.1] | 21.5 | 18.1-23.1 | 0.000 (0.000-0.000) | 0.690 | 0.000 | 14.4 | 8.5 |
| 1 | overlap | Recompute | merged 3 | 2/2 | 8252.2 [7890.8, 8613.7] | 8305.5 | 8087.6-8363.7 | 151.136 (151.136-151.136) | 0.843 | 161.115 | 244.7 | 7.8 |
| 1 | overlap | Replay (#13959) | conflict 10 | 0/0 | 17.3 [11.7, 22.9] | 14.9 | 14.1-39.6 | 0.000 (0.000-0.000) | 0.843 | 0.000 | 0.0 | 7.5 |
| 1 | overlap | Row-level 3-way | merged 3 | 2/2 | 8126.6 [7957.0, 8296.2] | 8099.4 | 8076.2-8204.3 | 453.327 (453.327-453.334) | 0.843 | 161.115 | 244.8 | 7.8 |
| 1 | overlap | SnapMerge | merged 10 | 2/2 | 159.5 [143.2, 175.8] | 165.3 | 95.4-174.7 | 4.206 (1.758-4.281) | 0.843 | 0.081 | 13.2 | 7.4 |
| 10 | disjoint | Recompute | merged 3 | 2/2 | 225888.0 [220586.5, 231189.4] | 225761.8 | 223819.8-228082.4 | 1662.134 (1661.595-1662.137) | 4.930 | 1472.450 | 1559.5 | 11.5 |
| 10 | disjoint | Replay (#13959) | merged 10 | 2/2 | 100.2 [90.4, 109.9] | 102.2 | 79.3-119.1 | 0.000 (0.000-0.000) | 8.229 | 0.000 | 27.2 | 12.9 |
| 10 | disjoint | Row-level 3-way | merged 3 | 2/2 | 165877.2 [159751.6, 172002.9] | 165183.8 | 163832.3-168615.6 | 4985.324 (4985.083-4985.837) | 4.930 | 1472.450 | 1560.3 | 11.8 |
| 10 | disjoint | SnapMerge | merged 10 | 2/2 | 74.9 [72.3, 77.5] | 74.4 | 68.9-82.3 | 0.000 (0.000-0.000) | 4.930 | 0.000 | 23.9 | 13.1 |
| 10 | overlap | Recompute | merged 3 | 2/2 | 223315.1 [215113.7, 231516.4] | 225056.9 | 219507.4-225380.8 | 1661.583 (1661.581-1661.591) | 6.195 | 1473.417 | 1528.3 | 13.8 |
| 10 | overlap | Replay (#13959) | conflict 10 | 0/0 | 108.7 [105.8, 111.6] | 107.8 | 102.4-115.3 | 0.000 (0.000-0.000) | 6.195 | 0.000 | 0.0 | 11.5 |
| 10 | overlap | Row-level 3-way | merged 3 | 2/2 | 163549.4 [158332.2, 168766.6] | 163985.2 | 161265.5-165397.6 | 4983.308 (4981.604-4983.931) | 6.195 | 1473.417 | 1531.6 | 11.8 |
| 10 | overlap | SnapMerge | merged 10 | 2/2 | 1292.5 [1018.2, 1566.8] | 1481.6 | 327.4-1512.1 | 39.950 (6.808-41.321) | 6.195 | 0.823 | 13.2 | 11.5 |

Paired latency ratios by repetition (other / SnapMerge; geometric mean, 95% paired bootstrap over the paired reps, t-interval on log ratios; repetitions are paired by index):

| SF | Kind | Other | Basis | Geomean ratio [paired bootstrap 95% CI] | Geomean ratio [t 95% CI on logs] | Reps other slower |
|---|---|---|---|---|---|---|
| 0.1 | disjoint | Recompute | both merged | 73.249 [70.449, 78.104] | 73.249 [63.735, 84.182] | 3/3 |
| 0.1 | disjoint | Replay (#13959) | both merged | 1.328 [1.263, 1.392] | 1.328 [1.252, 1.409] | 10/10 |
| 0.1 | disjoint | Row-level 3-way | both merged | 68.603 [65.774, 72.991] | 68.603 [59.868, 78.613] | 3/3 |
| 0.1 | overlap | Recompute | both merged | 25.940 [23.476, 27.448] | 25.940 [20.915, 32.173] | 3/3 |
| 0.1 | overlap | Replay (#13959) | time to decision (other policy reported a conflict) | 0.174 [0.165, 0.182] | 0.174 [0.163, 0.184] | 0/10 |
| 0.1 | overlap | Row-level 3-way | both merged | 23.898 [22.397, 25.001] | 23.898 [20.713, 27.573] | 3/3 |
| 1 | disjoint | Recompute | both merged | 410.936 [369.084, 448.406] | 410.936 [321.431, 525.366] | 3/3 |
| 1 | disjoint | Replay (#13959) | both merged | 1.316 [1.230, 1.387] | 1.316 [1.221, 1.418] | 10/10 |
| 1 | disjoint | Row-level 3-way | both merged | 413.223 [369.309, 462.924] | 413.223 [312.109, 547.093] | 3/3 |
| 1 | overlap | Recompute | both merged | 50.225 [49.975, 50.618] | 50.225 [49.379, 51.085] | 3/3 |
| 1 | overlap | Replay (#13959) | time to decision (other policy reported a conflict) | 0.104 [0.088, 0.129] | 0.104 [0.081, 0.133] | 0/10 |
| 1 | overlap | Row-level 3-way | both merged | 49.464 [48.735, 50.013] | 49.464 [47.850, 51.133] | 3/3 |
| 10 | disjoint | Recompute | both merged | 3079.736 [2975.945, 3248.426] | 3079.736 [2741.679, 3459.477] | 3/3 |
| 10 | disjoint | Replay (#13959) | both merged | 1.327 [1.239, 1.424] | 1.327 [1.219, 1.444] | 10/10 |
| 10 | disjoint | Row-level 3-way | both merged | 2261.456 [2137.631, 2447.216] | 2261.456 [1898.573, 2693.699] | 3/3 |
| 10 | overlap | Recompute | both merged | 294.356 [157.519, 688.327] | 294.356 [44.284, 1956.562] | 3/3 |
| 10 | overlap | Replay (#13959) | time to decision (other policy reported a conflict) | 0.091 [0.074, 0.123] | 0.091 [0.065, 0.126] | 0/10 |
| 10 | overlap | Row-level 3-way | both merged | 215.581 [114.775, 492.515] | 215.581 [33.643, 1381.434] | 3/3 |

Distinct paired-bootstrap resamples: 3 pairs -> 10, 10 pairs -> 92378; with 3 pairs the intervals are coarse.

Base tables: SF 0.1: 600,572 rows in 84 files, SF 1: 6,001,215 rows in 334 files, SF 10: 59,986,052 rows in 2,520 files. Process peak RSS (max over records): SF 0.1 1.5 GB, SF 1 7.0 GB, SF 10 22.6 GB.

## 8. Ablation of rule families (`ablation_v4`, 1,505 scenarios; main campaign as replication)

| Variant | ablation_v4 sound auto-merge of commuting | Full - variant (paired) | Share of SnapMerge's advantage over replay lost | Unsound merges | Main campaign (replication) |
|---|---|---|---|---|---|
| SnapMerge | 1.000 [1.000, 1.000] | - | - | 0 | 1.000 [1.000, 1.000] |
| SnapMerge w/o net-effect test | 0.936 [0.917, 0.954] | +0.064 [+0.046, +0.083] | 11% | 0 | 0.952 [0.941, 0.963] |
| SnapMerge w/o compaction rebase | 0.778 [0.754, 0.801] | +0.222 [+0.199, +0.246] | 37% | 0 | 0.776 [0.760, 0.792] |
| SnapMerge w/o delete union | 0.642 [0.611, 0.674] | +0.358 [+0.326, +0.389] | 59% | 0 | 0.666 [0.644, 0.688] |
| SnapMerge w/o bounded reads | 0.443 [0.415, 0.472] | +0.557 [+0.528, +0.585] | 92% | 0 | 0.459 [0.440, 0.479] |
| SnapMerge file-level rules only | - | - | - | - | 0.482 [0.458, 0.505] |
| Replay (#13959) | 0.393 [0.366, 0.421] | +0.607 [+0.579, +0.634] | - | 0 | 0.415 [0.395, 0.435] |

The shares do not add up to 100%: the rule families interact, because one scenario can need several of them.

| Mix | SnapMerge | SnapMerge w/o net-effect test | SnapMerge w/o compaction rebase | SnapMerge w/o delete union | SnapMerge w/o bounded reads | Replay (#13959) |
|---|---|---|---|---|---|---|
| Partition-disjoint ingest | 1.000 | 1.000 | 1.000 | 1.000 | 0.983 | 0.989 |
| Filter corrections | 1.000 | 1.000 | 1.000 | 0.714 | 0.643 | 0.714 |
| Upsert by key | 1.000 | 0.917 | 1.000 | 0.309 | 0.041 | 0.196 |
| Deletes (MOR) | 1.000 | 0.986 | 1.000 | 0.203 | 0.855 | 0.130 |
| Deletes (COW) | 1.000 | 0.905 | 1.000 | 0.175 | 0.143 | 0.127 |
| Compaction on main | 1.000 | 1.000 | 0.089 | 0.918 | 0.074 | 0.067 |
| Adversarial overlap | 1.000 | 0.679 | 0.765 | 0.506 | 0.185 | 0.222 |

## 9. Fallback without recorded metadata (`fallback_filelevel_v4`)

| Mix | Commuting | SnapMerge (file-level) | Replay | SnapMerge - replay (paired) | Replay missed conflicts | SnapMerge missed conflicts |
|---|---|---|---|---|---|---|
| All mixes | 442 | 0.380 [0.339, 0.422] | 0.443 [0.407, 0.481] | -0.063 [-0.119, -0.009] | 63 | 0 |
| Partition-disjoint ingest | 123 | 0.317 [0.235, 0.403] | 1.000 [1.000, 1.000] | -0.683 [-0.765, -0.597] | 6 | 0 |
| Filter corrections | 5 | 0.000 [0.000, 0.000] | 0.600 [0.000, 1.000] | -0.600 [-1.000, +0.000] | 5 | 0 |
| Upsert by key | 84 | 0.048 [0.011, 0.096] | 0.286 [0.192, 0.385] | -0.238 [-0.333, -0.150] | 0 | 0 |
| Deletes (MOR) | 52 | 0.885 [0.789, 0.963] | 0.288 [0.167, 0.417] | +0.596 [+0.439, +0.746] | 12 | 0 |
| Deletes (COW) | 41 | 0.000 [0.000, 0.000] | 0.171 [0.061, 0.294] | -0.171 [-0.294, -0.061] | 15 | 0 |
| Compaction on main | 87 | 0.655 [0.554, 0.753] | 0.115 [0.053, 0.187] | +0.540 [+0.410, +0.663] | 2 | 0 |
| Adversarial overlap | 50 | 0.440 [0.304, 0.581] | 0.280 [0.157, 0.407] | +0.160 [+0.000, +0.325] | 23 | 0 |

## 10. Counterexamples (`counterexamples_v3`)

| Case | Kind | Policy | Original size | Minimised size | True conflict | SnapMerge | Property holds |
|---|---|---|---|---|---|---|---|
| cherry_pick_unsound_fuzz1609 | unsound | Iterated cherry-pick | [5, 16, 9, 2, 1] | [2, 1, 2, 0, 1] | False | merged | True |
| cherry_pick_unsound_fuzz1847 | unsound | Iterated cherry-pick | [8, 150, 9, 2, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| cherry_pick_unsound_fuzz591 | unsound | Iterated cherry-pick | [8, 151, 8, 0, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| cherry_pick_unsound_fuzz604 | unsound | Iterated cherry-pick | [5, 3, 7, 7, 2] | [3, 1, 2, 4, 1] | True | conflict | True |
| cherry_pick_unsound_fuzz660 | unsound | Iterated cherry-pick | [5, 7, 14, 1, 3] | [2, 1, 2, 0, 1] | True | conflict | True |
| cherry_pick_unsound_fuzz753 | unsound | Iterated cherry-pick | [2, 33, 4, 0, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| replay_overconservative_delete_fuzz31 | overconservative | Replay (#13959) | [7, 151, 91, 9, 3] | [2, 1, 0, 3, 1] | False | merged | True |
| replay_overconservative_delete_fuzz39 | overconservative | Replay (#13959) | [7, 81, 15, 8, 3] | [2, 1, 0, 2, 1] | False | merged | True |
| replay_overconservative_overwrite_fuzz3 | overconservative | Replay (#13959) | [5, 153, 36, 4, 2] | [2, 1, 1, 1, 1] | False | merged | True |
| replay_overconservative_overwrite_fuzz5 | overconservative | Replay (#13959) | [3, 48, 1, 4, 3] | [2, 1, 1, 1, 1] | False | merged | True |
| replay_overconservative_replace_fuzz11 | overconservative | Replay (#13959) | [6, 82, 71, 5, 2] | [2, 1, 0, 2, 1] | False | merged | True |
| replay_overconservative_replace_fuzz7 | overconservative | Replay (#13959) | [4, 150, 5, 5, 1] | [2, 1, 0, 2, 1] | False | merged | True |
| replay_unsound_fuzz1609 | unsound | Replay (#13959) | [5, 16, 9, 2, 1] | [2, 1, 2, 0, 1] | False | merged | True |
| replay_unsound_fuzz1847 | unsound | Replay (#13959) | [8, 150, 9, 2, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| replay_unsound_fuzz2684 | unsound | Replay (#13959) | [4, 7, 8, 3, 3] | [2, 1, 3, 1, 1] | True | conflict | True |
| replay_unsound_fuzz3188 | unsound | Replay (#13959) | [6, 45, 5, 5, 1] | [2, 1, 2, 0, 1] | True | conflict | True |
| replay_unsound_fuzz3649 | unsound | Replay (#13959) | [5, 1, 7, 3, 3] | [2, 1, 2, 1, 1] | False | merged | True |
| replay_unsound_fuzz439 | unsound | Replay (#13959) | [6, 40, 13, 4, 1] | [3, 1, 2, 1, 1] | False | merged | True |
| replay_unsound_fuzz591 | unsound | Replay (#13959) | [8, 151, 8, 0, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| replay_unsound_fuzz604 | unsound | Replay (#13959) | [5, 3, 7, 7, 2] | [3, 1, 2, 4, 1] | True | conflict | True |
| replay_unsound_fuzz660 | unsound | Replay (#13959) | [5, 7, 14, 1, 3] | [2, 1, 2, 0, 1] | True | conflict | True |
| replay_unsound_fuzz753 | unsound | Replay (#13959) | [2, 33, 4, 0, 3] | [2, 1, 2, 0, 1] | False | merged | True |
| replay_unsound_identical_new_key_upserts | unsound | Replay (#13959) | [2, 24, 2, 0, 2] | [2, 1, 2, 0, 1] | False | merged | True |
| rowlevel_unsound_fuzz1901 | unsound | Row-level 3-way | [3, 15, 8, 0, 1] | [2, 1, 2, 0, 1] | False | merged | True |
| rowlevel_unsound_fuzz2744 | unsound | Row-level 3-way | [7, 45, 46, 2, 2] | [2, 1, 2, 1, 1] | True | conflict | True |
| rowlevel_unsound_fuzz2812 | unsound | Row-level 3-way | [3, 45, 27, 2, 3] | [2, 1, 3, 1, 1] | True | conflict | True |
| rowlevel_unsound_fuzz2979 | unsound | Row-level 3-way | [7, 150, 23, 3, 1] | [2, 1, 2, 1, 1] | False | merged | True |
| rowlevel_unsound_fuzz3212 | unsound | Row-level 3-way | [5, 30, 22, 4, 3] | [2, 1, 2, 1, 1] | True | conflict | True |
| rowlevel_unsound_fuzz3376 | unsound | Row-level 3-way | [6, 30, 5, 3, 2] | [4, 1, 2, 2, 1] | True | conflict | True |
| rowlevel_unsound_fuzz3621 | unsound | Row-level 3-way | [2, 1, 4, 1, 2] | [2, 1, 2, 1, 1] | False | merged | True |
| rowlevel_unsound_fuzz3995 | unsound | Row-level 3-way | [4, 40, 4, 0, 1] | [2, 1, 2, 0, 1] | False | merged | True |

Size = (operations, base rows, inserted rows, predicate atoms, base chunks). The minimiser never drops the last base row.

### Counterexample box (minimised)

- **replay_unsound_fuzz439** (unsound, Replay (#13959); layout identity_k). Base {rid 39}. A: DELETE[mor] where rid > 1; APPEND rows {rid 10002}. C: UPSERT[cow] keys {10002} rows {rid 10002}. Oracle (A;C) = {rid 10002}, (C;A) = {rid 10002}. Replay (#13959): {rid 10002 x2}. SnapMerge: {rid 10002}.
- **replay_unsound_identical_new_key_upserts** (unsound, Replay (#13959); layout month). Base {rid 23}. A: UPSERT[mor] keys {990} rows {rid 990}. C: UPSERT[cow] keys {990} rows {rid 990}. Oracle (A;C) = {rid 23, rid 990}, (C;A) = {rid 23, rid 990}. Replay (#13959): {rid 23, rid 990 x2}. SnapMerge: {rid 23, rid 990}.
- **cherry_pick_unsound_fuzz1609** (unsound, Iterated cherry-pick; layout none). Base {rid 14}. A: UPSERT[mor] keys {10003} rows {rid 10003}. C: UPSERT[cow] keys {10003} rows {rid 10003}. Oracle (A;C) = {rid 10003, rid 14}, (C;A) = {rid 10003, rid 14}. Iterated cherry-pick: {rid 10003 x2, rid 14}. SnapMerge: {rid 10003, rid 14}.
- **rowlevel_unsound_fuzz1901** (unsound, Row-level 3-way; layout identity_k). Base {rid 14}. A: APPEND rows {rid 10004}. C: APPEND rows {rid 10004}. Oracle (A;C) = {rid 10004 x2, rid 14}, (C;A) = {rid 10004 x2, rid 14}. Row-level 3-way: {rid 10004, rid 14}. SnapMerge: {rid 10004 x2, rid 14}.
- **replay_overconservative_delete_fuzz39** (overconservative, Replay (#13959); layout identity_k). Base {rid 5}. A: REPLACE where k = 1. C: DELETE[mor] where q IN (1.0). Oracle (A;C) = {}, (C;A) = {}. Replay (#13959): conflict (partition-overlap:delete). SnapMerge: {}.
- **replay_overconservative_overwrite_fuzz3** (overconservative, Replay (#13959); layout identity_k). Base {rid 26}. A: REPLACE where k = 1. C: UPSERT[mor] keys {10005} rows {rid 10005}. Oracle (A;C) = {rid 10005, rid 26}, (C;A) = {rid 10005, rid 26}. Replay (#13959): conflict (partition-overlap:overwrite). SnapMerge: {rid 10005, rid 26}.
- **replay_overconservative_replace_fuzz11** (overconservative, Replay (#13959); layout none). Base {rid 50}. A: OVERWRITE[cow] where TRUE. C: REPLACE where TRUE. Oracle (A;C) = {}, (C;A) = {}. Replay (#13959): conflict (partition-overlap:replace). SnapMerge: {}.

## 11. Hypotheses

Verdict rules: *supported* means the point estimate meets the criterion and the 95% interval lies on the claimed side. For zero-event claims this means 0 events were observed, and the exact upper bound is reported. *Not supported* means the estimate contradicts the criterion and the interval excludes the claimed side. *Inconclusive* means the interval straddles the criterion or the data were not collected.

- **H1 (SUPPORTED)**: C2/C3: every merge SnapMerge emits equals a serial order of the two lineages (0 soundness violations). difftest_main_tpch_v5: 0 of 1341 SnapMerge merges; difftest_main_nyc_v5: 0 of 1258 SnapMerge merges; ablation_v5: 0 of 639 SnapMerge merges; fallback_filelevel_v5: 0 of 168 SnapMerge merges; all SnapMerge configurations in all 15 difftest campaigns together: 0 of 18497 merges (not independent trials; the append-rule reproduction of the submitted version is excluded); scale_v5_tpch: 0 of 12; counterexamples_v3: SnapMerge sound on all 31 minimised cases. Exact 95% upper bound on the per-merge violation rate in the main campaign: 0.00275 (0 of 1341). *Caveat:* Empirical check under the frozen scenario generator; the theorem itself is a paper proof, not shown by the data.
- **H2 (SUPPORTED)**: C3: SnapMerge's conflict recall is 100% (every scenario whose serial orders differ is reported). difftest_main_tpch_v5: 1662/1662 (exact 95% CI [0.99778, 1.0]); difftest_main_nyc_v5: 1745/1745 (exact 95% CI [0.99789, 1.0]); ablation_v5: 866/866 (exact 95% CI [0.99575, 1.0]); fallback_filelevel_v5: 559/559 (exact 95% CI [0.99342, 1.0]).
- **H3 (SUPPORTED)**: C4 (primary): SnapMerge auto-merges a larger share of commuting scenarios soundly than partition-level replay. main campaign, all mixes: SnapMerge 1.000 [1.000, 1.000] vs replay 0.415 [0.395, 0.435]; paired difference +0.585 [+0.565, +0.605]. Per mix (difference [95% CI]): partition_disjoint_ingest +0.000 [+0.000, +0.000]; filter_corrections +0.210 [+0.089, +0.345]; upsert_keys +0.794 [+0.741, +0.846]; deletes_mor +0.854 [+0.791, +0.913]; deletes_cow +0.801 [+0.736, +0.865]; compaction_on_main +0.929 [+0.897, +0.957]; adversarial_overlap +0.696 [+0.624, +0.765]. CI excludes 0 on 6 of 7 mixes. *Caveat:* On partition_disjoint_ingest both reach 1.0 (difference 0): replay is already complete there.
- **H4 (SUPPORTED)**: Day-10 gate: SnapMerge's auto-merge advantage over replay is at least 20 points on at least one mix including compaction (otherwise reframe as E&A). advantage >= 20 points (point estimate) on 6 of 7 mixes (filter_corrections, upsert_keys, deletes_mor, deletes_cow, compaction_on_main, adversarial_overlap); with the 95% CI lower bound >= 20 points on 5 of 7 (upsert_keys, deletes_mor, deletes_cow, compaction_on_main, adversarial_overlap); compaction_on_main +0.929 [+0.897, +0.957]. Gate not triggered; no E&A reframing. *Caveat:* Points are absolute percentage points of the sound auto-merge rate of commuting scenarios.
- **H5 (SUPPORTED)**: C4: partition-level replay is over-conservative (false conflicts) and also misses true conflicts. main campaign replay: TP 1471, FP 785, FN 191, TN 556; recall 0.885 [0.870, 0.900], precision 0.652 [0.634, 0.670]; false-conflict rate on commuting scenarios 0.585 [0.565, 0.605].
- **H6 (SUPPORTED)**: C4: replay can emit merges equal to NEITHER serial order (unsound). 11 minimised counterexamples (counterexamples_v3; 7 of them on commuting scenarios); but 0 such merges in the main campaign (0 of 747 replay merges; exact 95% upper bound 0.0049). *Caveat:* Existence only: the unsound replay cases come from the review fuzzer's tiny value domains, not from the main workload.
- **H7 (SUPPORTED)**: C4: FORCE / source-wins merges silently lose or add rows. Nessie FORCE (main): 2756 of 3003 merges equal neither serial order; 239665 rows lost and 650978 extra against the closer order; 2052 merges lose rows against both orders.
- **H8 (SUPPORTED)**: C4: SnapMerge's merge latency is far below row-level three-way merge (which reads all data). SF 0.1 disjoint: row-level/SnapMerge 68.6 [65.8, 73.0]x (3/3 reps slower); SF 0.1 overlap: row-level/SnapMerge 23.9 [22.4, 25.0]x (3/3 reps slower); SF 1 disjoint: row-level/SnapMerge 413.2 [369.3, 462.9]x (3/3 reps slower); SF 1 overlap: row-level/SnapMerge 49.5 [48.7, 50.0]x (3/3 reps slower); SF 10 disjoint: row-level/SnapMerge 2261.5 [2137.6, 2447.2]x (3/3 reps slower); SF 10 overlap: row-level/SnapMerge 215.6 [114.8, 492.5]x (3/3 reps slower). *Caveat:* 3 paired reps per point; prototype implementations on a shared host (load average recorded per record).
- **H9 (SUPPORTED)**: C4: on disjoint merges SnapMerge costs no more than replay (both metadata-only). SF 0.1: replay/SnapMerge 1.328 [1.263, 1.392] (10/10 reps replay slower) -> faster; SF 1: replay/SnapMerge 1.316 [1.230, 1.387] (10/10 reps replay slower) -> faster; SF 10: replay/SnapMerge 1.327 [1.239, 1.424] (10/10 reps replay slower) -> faster. *Caveat:* n = 10 paired reps per size; prototype on a shared host.
- **H10 (SUPPORTED)**: C1: bounded reads touch only the contended partition, a small fraction (criterion: < 2% in every rep) of the data a full-data row-level merge reads. SF 0.1: SnapMerge reads 0.38 MB vs row-level 41 MB (0.93%); SF 1: SnapMerge reads 4.21 MB vs row-level 453 MB (0.93%); SF 10: SnapMerge reads 39.95 MB vs row-level 4983 MB (0.80%). *Caveat:* SnapMerge's bytes grow with the contended partition (one month of lineitem), i.e. about 10x per 10x scale factor; they are bounded by the partition, not constant.
- **H11 (SUPPORTED)**: C1: each rule family (net-effect test, compaction rebase, delete union, bounded reads) contributes auto-merges. ablation_v4: snapmerge[no_net_effect]: full minus variant +0.064 [+0.046, +0.083]; snapmerge[no_compaction_rebase]: full minus variant +0.222 [+0.199, +0.246]; snapmerge[no_delete_union]: full minus variant +0.358 [+0.326, +0.389]; snapmerge[no_bounded_reads]: full minus variant +0.557 [+0.528, +0.585].
- **H12 (NOT SUPPORTED)**: Without recorded logical metadata (file-level rules only) SnapMerge still auto-merges at least as many commuting scenarios as replay. fallback_filelevel_v4: SnapMerge 0.380 [0.339, 0.422] vs replay 0.443 [0.407, 0.481]; difference -0.063 [-0.119, -0.009]; replay misses 63 true conflicts, SnapMerge 0. *Caveat:* Negative result: SnapMerge's advantage depends on the recorded filters/keys.
- **H13 (NOT SUPPORTED)**: Plan Section 7: row-level three-way merge is a (sound) completeness upper bound. main campaign row-level: 196 merges equal neither order (45 on commuting, 151 on true conflicts); sound auto-merge rate of commuting 0.965 [0.956, 0.974] vs SnapMerge 1.000 [1.000, 1.000]; recall 0.389 [0.371, 0.408].
- **H14 (SUPPORTED)**: Plan Section 7: every emitted snapshot is read identically by PyIceberg and DuckDB. difftest campaigns: 0 disagreements over 41005 checked merges (all policies) and 0 over 17024 writer checks; scale_v5_tpch: 0 over 42 checked merges. *Caveat:* Spark and Trino readability is untested.
- **H15 (NOT SUPPORTED)**: Plan Section 6: the SF 10 oracle plus scans stay below about 16 GB. scale_v5_tpch process peak RSS 22.6 GB (max over records; per size: SF 0.1 1.5 GB, SF 1 7.0 GB, SF 10 22.6 GB); within the memory guard's limit (48 GB; 80 GB for the cost steps re-run on 2026-10-08).
- **H16 (SUPPORTED)**: Plan Section 6: the differential-test results (soundness, recall, advantage over replay) hold on a second data source (NYC TLC yellow taxi 2023-01, day partitions; same generator, mixes, seed base and policies as the main campaign). difftest_main_nyc_v4 (3003 scenarios, 1745 true conflicts): SnapMerge TP 1745, FP 0, FN 0, TN 1258, 0 of 1258 merges unsound (exact 95% upper bound 0.00293), recall 1745/1745 (exact 95% CI [0.99789, 1.0]); sound auto-merge of commuting SnapMerge 1.000 [1.000, 1.000] vs replay 0.402 [0.382, 0.423]; paired difference +0.598 [+0.577, +0.618] (TPC-H main +0.585 [+0.565, +0.605]). Replay TP 1518, FP 752, FN 227, TN 506, 0 of 733 merges equal neither order. Per mix: partition_disjoint_ingest +0.006 [+0.000, +0.015] (TPC-H +0.000); filter_corrections +0.375 [+0.212, +0.543] (TPC-H +0.210); upsert_keys +0.817 [+0.764, +0.866] (TPC-H +0.794); deletes_mor +0.826 [+0.759, +0.887] (TPC-H +0.854); deletes_cow +0.909 [+0.854, +0.957] (TPC-H +0.801); compaction_on_main +0.901 [+0.864, +0.935] (TPC-H +0.929); adversarial_overlap +0.722 [+0.644, +0.796] (TPC-H +0.696). CI excludes 0 on 6 of 7 mixes (TPC-H: 6 of 7). *Caveat:* Same scenario generator and mixes on a different source table (timestamps, day partitions); the NYC scale points (1/3/12 months) were not run, because only the 2023-01 download was approved, so the cost results (H8-H10) remain TPC-H-only.

## 12. Negative findings (kept)

- Without recorded filters and keys (file-level rules only), SnapMerge soundly auto-merges fewer commuting scenarios than replay: 0.380 [0.339, 0.422] vs 0.443 [0.407, 0.481] (paired difference -0.063 [-0.119, -0.009]). Replay pays for this with 63 missed true conflicts. By mix (paired 95% CI excluding 0), the file-level rules lose on Partition-disjoint ingest, Upsert by key, Deletes (COW) and win on Deletes (MOR), Compaction on main (Section 9).
- Row-level three-way merge is not a sound upper bound: 196 of 2354 main-campaign merges equal neither serial order (45 on commuting scenarios), and its recall is 0.389.
- No advantage on partition-disjoint ingest: SnapMerge and replay both reach 1.000 (difference +0.000). On filter corrections the advantage is only +0.210.
- In the main campaign SnapMerge's time to decision is higher than replay's: median 18.4 ms vs 9.7 ms; replay/SnapMerge geometric-mean ratio 0.52 [0.51, 0.53] over all scenarios and 1.31 [1.28, 1.35] on the 556 scenarios both merged soundly. Replay often stops early with a (false) conflict. SnapMerge also reads 63.9 KB of data per scenario on average, and replay reads none.
- At SF 10, a SnapMerge overlap merge (compaction vs delete+append in one partition) takes 1482 ms (median; range 327-1512) and reads 40.0 MB (range 6.8-41.3 MB, depending on the seeded partition). Replay answers in 108 ms, but only with a false conflict. The bounded-read cost grows with partition size.
- The plan's memory estimate was too low: the scale runner (oracle checks included) peaks at 22.6 GB process RSS at SF 10, against a plan estimate of at most 16 GB.
- Isolated merge processes at SF 10 (max over reps and both shapes): Recompute 13.1 GB peak RSS, 32.5 GB DuckDB temporary files; Replay (#13959) 0.2 GB peak RSS, 0.0 GB DuckDB temporary files; Row-level 3-way 12.9 GB peak RSS, 66.3 GB DuckDB temporary files; SnapMerge 1.2 GB peak RSS, 0.0 GB DuckDB temporary files. The row-level baseline's spill exceeds the plan's 60 GB disk estimate.
- Without bounded reads, SnapMerge stays close to replay (0.443 [0.415, 0.472] vs 0.393 [0.366, 0.421] in ablation_v4): most of the advantage needs data reads, so the merge is not metadata-only in those cases.
- Replay silently merges 191 true conflicts in the main campaign (it picks one serial order without reporting the conflict). Iterated cherry-pick misses 159.
- Known SnapMerge limitation (fails closed): a second merge between the same branches is unsupported (F9). Since the revision, a commit on another branch (including the source) during a merge is re-sequenced and retried, and a moved target returns 'retryable' (F10; workflow_v2 races).
- The difftest runner's RSS grows over a campaign (8.1 GB guard peak over 3,003 scenarios in the submitted version's Experiment stage), probably from catalog engines that are never released.
- The NYC campaign ran under a heavier host load (1-min load average median 7, max 10, 32 logical CPUs) than the TPC-H main campaign (median 10); its latencies are not comparable with TPC-H's.
- The second source (NYC TLC 2023-01) repeats the weak spots: on partition-disjoint ingest the advantage over replay is +0.006 [+0.000, +0.015], on filter corrections +0.375 [+0.212, +0.543] (only 32 commuting scenarios); replay silently merges 227 true conflicts and row-level merge emits 183 merges equal to neither order. The NYC scale points were not run.

## 13. What the data does NOT show

- It does not prove soundness. Zero violations in 1341 main-campaign merges bound the per-merge violation rate only for this generator (exact 95% upper bound 0.00275). The proof of the theorem is separate.
- It covers a second data source only for the differential test and only under the same generator. difftest_main_nyc_v5 (3003 scenarios, NYC TLC yellow taxi 2023-01, four consecutive day partitions per scenario) replicates the main campaign; the NYC scale points (1/3/12 months) were not run because only the 2023-01 download was approved, so every cost and scale result and the ablation, fallback, sensitivity, single-snapshot-writer and partial-metadata campaigns are on TPC-H lineitem only.
- It does not measure real-world conflict rates. The 55% (TPC-H) and 58% (NYC) shares of true conflicts are properties of the frozen scenario generator (4 partitions per scenario, deletes mixes dominated by APPEND x DELETE; F8). GitLake reports about 10 conflicts per 100k attempts at table granularity. The nine one-factor sensitivity campaigns (section 21) vary partition span, history length, selectivity and compaction frequency under the same generator; real branch histories are not covered.
- It does not show production performance. Prototype Python/PyIceberg code ran on a shared host (1-min load averages 3-67 across the accepted campaigns, 32 logical CPUs). Difftest latencies are single-process wall clock with a seeded random policy order per scenario and in-process caches cleared; scale latencies come from a fresh process per timed run with a warm page cache, in random order.
- It does not compare against upstream implementations. Replay implements the Iceberg #13959 proposal as we read it (the proposal was never merged). lakeFS and Nessie are emulations of their documented merge semantics, and row-level merge is our own DuckDB implementation in the Decibel/Dolt style.
- It does not show readability by Spark or Trino. Only DuckDB 1.5.6 (iceberg extension) and PyIceberg 0.12.0 read the snapshots.
- It does not cover equality deletes, deletion vectors, schema or partition-spec evolution, repeated merges between the same branches (F9); they are unsupported and fail closed. Concurrent commits are covered only by the injected races of workflow_v2.
- It does not show scale behaviour beyond TPC-H SF 10 (60M rows, 2,520 files), with many branches, or with workloads other than the fixed scale shapes (disjoint, overlap, contended-K, history-L). Scale points have 10 reps (3 for row-level and recompute), so the heavy policies' intervals are wide.
- Its metadata byte counts are approximate (F12: about 0.05% run-to-run variation). Per-policy memory comes from isolated child processes (peak minus baseline RSS, sampled), not from a kernel-enforced measurement.
- Minimised counterexamples are minimal only down to one base row, because the minimiser never removes the last one. The unsound replay cases come from the review fuzzer's tiny value domains: the main campaign has 0 unsound replay merges.
- The per-mix 95% intervals are bootstrap intervals over 429 (main, NYC), 215 (ablation), 143 (fallback, single-snapshot writer, partial metadata) or 60 (sensitivity) scenarios per mix. Intervals at a rate of exactly 0 or 1 are degenerate in the bootstrap; use the exact intervals given beside them.

## 14. Resource telemetry (guard state files; not fingerprinted, for context only)

| Guard state | Status | rc | Attempts | Batch scales | Peak tree RSS GB | Started (UTC) | Finished (UTC) |
|---|---|---|---|---|---|---|---|
| experiment_ablation_v2_inv1.json | completed | 0 | 1 | [1.0] | 4.48 | 2026-10-02T08:31:45Z | 2026-10-02T08:50:30Z |
| experiment_batch2.json | completed | 0 | 1 | [1.0] | 16.13 | 2026-10-02T10:37:25Z | 2026-10-02T10:51:39Z |
| experiment_difftest_main_nyc_v2_inv3.json | completed | 0 | 1 | [1.0] | 6.04 | 2026-10-03T17:21:50Z | 2026-10-03T18:04:23Z |
| experiment_difftest_main_tpch_v2_inv1.json | completed | 0 | 1 | [1.0] | 8.13 | 2026-10-02T07:32:25Z | 2026-10-02T08:28:40Z |
| experiment_fallback_filelevel_v2_inv1.json | completed | 0 | 1 | [1.0] | 3.98 | 2026-10-02T08:56:58Z | 2026-10-02T09:14:37Z |
| experiment_pilot_v2.json | completed | 0 | 1 | [1.0] | 2.1 | 2026-10-02T07:27:20Z | 2026-10-02T07:32:08Z |
| experiment_scale_v3_tpch_inv1.json | completed | 0 | 1 | [1.0] | 23.56 | 2026-10-02T11:58:39Z | 2026-10-02T13:09:10Z |
| experiment_scale_v3_tpch_inv2.json | completed | 0 | 1 | [1.0] | 25.3 | 2026-10-02T15:42:42Z | 2026-10-02T16:27:16Z |
| revision_ALL_inv1.json | running |  | 1 | [1.0] | 7.68 | 2026-10-05T20:32:52Z |  |
| revision_ALL_inv2.json | running |  | 1 | [1.0] | 2.51 | 2026-10-06T07:05:42Z |  |
| revision_ALL_inv3.json | completed | 0 | 1 | [1.0] | 37.63 | 2026-10-06T11:54:35Z | 2026-10-06T18:44:49Z |

Analysis run: 71.5 s, peak RSS 1.623 GB.

## 15. Figures (drawn from results/summary.json only)

- `paper/figures/fig1_automerge_by_mix.pdf`: Figure 1. Share of commuting scenarios each policy merges soundly, per mix (difftest_main_tpch_v4, TPC-H SF 0.1, 3,003 scenarios). Whiskers: 95% bootstrap intervals over scenarios. n = commuting scenarios. Row-level counts only its serial-equivalent merges. Hollow squares: replay on the NYC taxi source (difftest_main_nyc_v5, 3003 scenarios); SnapMerge merges 100% of the NYC commuting scenarios as well. n counts the TPC-H commuting scenarios.
- `paper/figures/fig2_scale_latency.pdf`: Figure 2. Merge latency versus table size (scale_v6_tpch; TPC-H lineitem SF 0.1/1/10 = 0.6M/6M/60M rows). Markers: mean of seeded repetitions (3/10 per point); whiskers: Student-t 95% intervals (clipped at the observed minimum). Every timed run is a fresh process with a warm page cache, in seeded random order. Hollow markers, dashed: replay reports a (false) partition conflict on the overlap scenario, so its time is the time to the conflict.
- `paper/figures/fig3_ablation.pdf`: Figure 3. Ablation of SnapMerge's rule families: sound auto-merge rate of commuting scenarios with one family disabled (blue) versus replay (orange). Filled: ablation_v4 (seed base 20261003); hollow: the same variants in the main campaign (seed base 20261002); diamonds: the same variants on the NYC taxi source (difftest_main_nyc_v5, seed base 20261002). Whiskers: 95% bootstrap intervals. Every variant stays sound with recall 1.0.
- `paper/figures/fig4_scale_bytes_read.pdf`: Figure 4. Bytes read per merge (data, delete and metadata files) versus table size (scale_v5_tpch). Mean over repetitions with t 95% intervals. SnapMerge and replay read only metadata on disjoint merges, and SnapMerge reads only the contended partition on overlap. Row-level merge reads the whole table.
- `paper/figures/fig5_conflict_detection.pdf`: Figure 5. Conflict detection quality per policy in the main campaign (1,662 true conflicts among 3,003 scenarios). Whiskers: 95% bootstrap intervals. Nessie NORMAL and fast-forward overlap at recall 100%, because they report a conflict whenever both sides changed the table. Nessie FORCE never reports a conflict (recall 0) and is omitted; 2756 of its 3003 merges equal neither serial order (Table 2).
- `paper/figures/fig6_controls_by_mix.pdf`: Figure 6. Information-matched controls on the main campaign (difftest_main_tpch_v5): every policy uses the same recorded operations and insertion provenance except partition-level replay. Blue = composition of the physical lineages, orange = physical replay; filled triangle/circle = exact net-effect test, hollow = pairwise validation. Whiskers: 95% bootstrap intervals over scenarios (stratified by mix); n = commuting scenarios.
- `paper/figures/fig7_sensitivity.pdf`: Figure 7. Sensitivity of the sound auto-merge rate of commuting scenarios to the generator's partition span, history length, predicate selectivity and compaction frequency (one factor at a time; * = default, taken from the first 60 scenarios per mix of the main campaign; other levels 420 scenarios each). Whiskers: 95% bootstrap intervals.
- `paper/figures/fig8_contended.pdf`: Figure 8. SnapMerge's cost grows with the rows of the contended partitions (x: preflight estimate from manifest metadata). Each point is the median of isolated runs (fresh process, warm page cache); right: peak resident memory of the merge process above its baseline after loading the table.

## 16. Environment, data provenance, code size, workload and mechanisms

Recorded in each accepted campaign's environment.json (host, versions), data/*/provenance.json (data), the guard state files in runs/ (guard; not fingerprinted), static line counts of the current code, and the accepted raw records of the main campaign (workload, mechanisms).

- Host: macOS-26.5.2-arm64-arm-64bit, arm64, 32 logical CPUs, 512.0 GiB memory (campaigns: ablation_v5, contended_sf10_v3, contended_unpart_v3, contended_v3, counterexamples_v4, difftest_main_nyc_v5, difftest_main_tpch_v5, fallback_filelevel_v5, history_v3, mixed_metadata_v3, physical_enum_v3, scale_v6_tpch, sens_compact05_v3, sens_compact1_v3, sens_hist2_v3, sens_hist4_v3, sens_parts16_v3, sens_parts32_v3, sens_parts8_v3, sens_sel025_v3, sens_sel4_v3, workflow_v3, writer_single_v3).
- Python 3.12.12 (campaigns: ablation_v5, contended_sf10_v3, contended_unpart_v3, contended_v3, counterexamples_v4, difftest_main_nyc_v5, difftest_main_tpch_v5, fallback_filelevel_v5, history_v3, mixed_metadata_v3, physical_enum_v3, scale_v6_tpch, sens_compact05_v3, sens_compact1_v3, sens_hist2_v3, sens_hist4_v3, sens_parts16_v3, sens_parts32_v3, sens_parts8_v3, sens_sel025_v3, sens_sel4_v3, workflow_v3, writer_single_v3).
- Packages: cachetools 7.2.0, duckdb 1.5.6, numpy 2.5.3, pandas 3.0.6, pyarrow 25.0.1, pyiceberg 0.12.0, pyiceberg-core 0.10.1, sqlalchemy 2.1.2.
- DuckDB v1.5.6; extensions: avro f9d5902, iceberg 890b78a9c, parquet v1.5.6, tpch v1.5.6.

| Data dir | Source | SF | File | Rows | Bytes | sha256 | License | Retrieved |
|---|---|---|---|---|---|---|---|---|
| data/tpch/sf0.01 | TPC-H (DuckDB tpch extension, dbgen) | 0.01 | lineitem | 60175 | 1336914 | 8c4dfeeef89c51a2f3fe630e150930e77fc272d52b6abefce6e86639060e526c | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/tpch/sf0.01 | TPC-H (DuckDB tpch extension, dbgen) | 0.01 | orders | 15000 | 246921 | bf8bb34c2bb414f260b7c026a0875deeaa6b274bef916b97a975662b82e73828 | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/tpch/sf0.1 | TPC-H (DuckDB tpch extension, dbgen) | 0.1 | lineitem | 600572 | 14089868 | 38f60e1cbb655b4d02f810f859d960b3f2730945ae9b40ca1c06ef3188acc8d0 | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/tpch/sf0.1 | TPC-H (DuckDB tpch extension, dbgen) | 0.1 | orders | 150000 | 2407628 | 7c5992a8d29998b70744ff7456b2b4fea8841eb77732d78d26e9b6524375826d | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/tpch/sf1 | TPC-H (DuckDB tpch extension, dbgen) | 1.0 | lineitem | 6001215 | 157506238 | 47555fab7d1d1f4adeddfc519eb187680b5754ea3ac45db535ad79a31d891346 | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/tpch/sf10 | TPC-H (DuckDB tpch extension, dbgen) | 10.0 | lineitem | 59986052 | 1726751146 | 026c280a6d4c4afd405e68117e7cce1d15a7716a80f5de93e18eac0bffc4e6bc | TPC EULA / fair use; locally generated, not redistributed | 2026-10-02 |
| data/nyc | NYC TLC Trip Record Data (yellow taxi) |  | yellow_tripdata_2023-01.clean.parquet | 3066718 |  | 020d43ffbaa5ccbcf89289347e6692e89f061ea019a1004224422162b0669ea9 | NYC Open Data terms of use, https://www.nyc.gov/home/terms-of-use.page | 2026-10-04 |

Caveat: Generated locally with DuckDB's tpch extension (dbgen port). Not a TPC-H benchmark result; must not be compared with published TPC-H results (TPC fair use policy, https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf). URL: https://duckdb.org/docs/stable/core_extensions/tpch

| Guard state | Limit GB | Reserve GB | Host physical GiB | Duration min |
|---|---|---|---|---|
| experiment_ablation_v2_inv1.json | 48.0 | 32.0 | 512.0 | 18.8 |
| experiment_batch2.json | 48.0 | 32.0 | 512.0 | 14.2 |
| experiment_difftest_main_nyc_v2_inv3.json | 48.0 | 32.0 | 512.0 | 42.5 |
| experiment_difftest_main_tpch_v2_inv1.json | 48.0 | 32.0 | 512.0 | 56.2 |
| experiment_fallback_filelevel_v2_inv1.json | 48.0 | 32.0 | 512.0 | 17.6 |
| experiment_pilot_v2.json | 48.0 | 32.0 | 512.0 | 4.8 |
| experiment_scale_v3_tpch_inv1.json | 48.0 | 32.0 | 512.0 | 70.5 |
| experiment_scale_v3_tpch_inv2.json | 48.0 | 32.0 | 512.0 | 44.6 |
| revision_ALL_inv1.json | 48.0 | 32.0 | 512.0 |  |
| revision_ALL_inv2.json | 48.0 | 32.0 | 512.0 |  |
| revision_ALL_inv3.json | 48.0 | 32.0 | 512.0 | 410.2 |

Code (static line counts, current code): library 5084 lines in 15 modules (__init__.py 16, baselines.py 329, catalog.py 865, emit.py 105, envinfo.py 105, expr.py 360, intent.py 219, lineage.py 495, merge.py 150, oracle.py 144, provenance.py 147, rules.py 1116, scenarios.py 519, sources.py 167, writer.py 347); runners 1218; scripts 2006; analysis 2707; tests 4130 lines in 19 files, 167 test methods.

Main-campaign workload: 3003 scenarios, 13907 operations; operations per side min 1, median 2.0, max 4. Operation kinds: APPEND[cow] 4504, DELETE[cow] 2830, DELETE[mor] 2603, OVERWRITE[cow] 1605, OVERWRITE[mor] 28, REPLACE[cow] 496, UPSERT[cow] 861, UPSERT[mor] 980. Adversarial templates: append_matches_delete 48, append_matches_remote_delete 48, both_cow_plus_match 48, compaction_vs_overwrite 67, duplicate_appends 55, masking_delete 57, overwrite_inserts_match 53, shared_upsert_keys 53.

SnapMerge mechanisms in the main campaign's 1341 merges (merges with the counter non-zero): pair:R2 614, contended-files 595, bounds:cannot-match 499, pair:R1 454, pair:R3 447, pair:R6 324, bounded-read 309, R3:cow-to-position 302, R6:compaction-rebase 300, bounded-read:clear 263, R3:delete-file-union 163, R4:key-check 142, pair:R4 142, pair:R5 117, net:masked-by-later-op 59, net:duplicates-removed 5, net:exact-equal 5; carrying_source_files 1103, with_contended_partitions 595, writing_new_position_delete_files 545.


## 17. Audit-derived facts (added in the Finalize stage; accepted raw records only)

Missed conflicts (merges of true conflicts) in the main campaign by the serial order they equal ((A;C) = target then source, the rebase order of replay and cherry-pick; (C;A) = source then target):

| Policy | merged_true_conflicts | equal_A_then_C_only | equal_C_then_A_only | equal_neither | equal_both |
|---|---|---|---|---|---|
| Iterated cherry-pick | 159 | 159 | 0 | 0 | 0 |
| Nessie FORCE | 1662 | 38 | 0 | 1624 | 0 |
| Rebase-OCC | 269 | 269 | 0 | 0 | 0 |
| Replay (#13959) | 191 | 182 | 9 | 0 | 0 |
| Row-level 3-way | 1015 | 386 | 478 | 151 | 0 |

Median time to a decision by outcome in the main campaign (ms; n in parentheses; p25-p75):

| Policy | TP | FP | FN | TN | error |
|---|---|---|---|---|---|
| Iterated cherry-pick | 8.86 (n=1503; 6.97-12.2) | 7.24 (n=959; 5.81-9.41) | 14.63 (n=159; 11.14-18.07) | 18.16 (n=382; 12.2-27.25) |  |
| Fast-forward only | 8.67 (n=1662; 6.83-11.92) | 7.1 (n=1341; 5.65-9.07) |  |  |  |
| lakeFS policy | 8.71 (n=1662; 6.77-11.88) | 7.39 (n=1156; 5.84-9.34) |  | 8.98 (n=185; 7.86-10.51) |  |
| Nessie FORCE |  |  | 12.11 (n=1662; 10.04-15.52) | 10.79 (n=1341; 9.09-13.07) |  |
| Nessie NORMAL | 8.65 (n=1662; 6.83-11.94) | 7.11 (n=1341; 5.69-9.16) |  |  |  |
| Rebase-OCC | 11.47 (n=1393; 9.22-15.03) | 7.88 (n=596; 6.24-10.87) | 16.97 (n=269; 13.61-22.37) | 18.68 (n=745; 13.62-26.68) |  |
| Recompute | 37.0 (n=1662; 30.46-46.65) |  |  | 45.37 (n=1341; 37.59-55.19) |  |
| Replay (#13959) | 8.97 (n=1471; 7.02-12.42) | 7.16 (n=785; 5.83-9.02) | 15.18 (n=191; 12.0-18.23) | 18.15 (n=556; 13.19-25.62) |  |
| Intent replay (pairwise) | 11.16 (n=1662; 9.0-14.66) | 8.12 (n=600; 6.42-11.55) |  | 20.05 (n=741; 14.31-26.92) |  |
| Exact replay | 16.62 (n=1662; 11.1-26.78) | 8.04 (n=595; 6.36-11.72) |  | 19.88 (n=746; 14.31-27.2) |  |
| Row-level 3-way | 33.29 (n=647; 29.28-37.3) | 26.69 (n=2; 26.37-27.01) | 43.43 (n=1015; 40.23-46.91) | 41.26 (n=1339; 37.95-45.1) |  |
| SnapMerge | 17.04 (n=1662; 11.49-27.26) |  |  | 19.62 (n=1341; 13.25-32.91) |  |
| SnapMerge, append-snapshot insert rule | 16.96 (n=1662; 11.41-27.23) |  |  | 19.81 (n=1341; 13.31-33.27) |  |
| SnapMerge file-level rules only | 8.84 (n=1662; 7.03-12.05) | 8.39 (n=695; 6.71-10.45) |  | 19.26 (n=646; 11.16-30.75) |  |
| SnapMerge w/o bounded reads | 9.61 (n=1662; 7.55-12.95) | 7.67 (n=725; 6.3-10.07) |  | 12.84 (n=616; 10.83-15.53) |  |
| SnapMerge w/o compaction rebase | 17.16 (n=1662; 11.4-27.51) | 6.75 (n=300; 5.61-8.05) |  | 16.42 (n=1041; 12.23-34.01) |  |
| SnapMerge w/o delete union | 16.95 (n=1662; 11.46-27.18) | 9.5 (n=448; 6.88-14.16) |  | 17.33 (n=893; 12.65-24.01) |  |
| SnapMerge w/o net-effect test | 11.57 (n=1662; 9.35-15.18) | 9.14 (n=64; 8.01-10.72) |  | 19.18 (n=1277; 13.09-31.62) |  |

Replay reports a conflict (stops before replaying) in 2256 of 3003 main scenarios (share 0.7512).
SnapMerge time to a decision without data/delete-file reads: median 13.01 ms (n=616); with reads: median 21.83 ms (n=2387; median 46029 bytes read).

Exact Clopper-Pearson bounds (one-sided 95% versus the two-sided 95% interval):

| Quantity | k | n | one-sided 95% upper | one-sided 95% lower | two-sided 95% |
|---|---|---|---|---|---|
| snapmerge_unsound_per_merge_main | 0 | 1341 | 0.00223 | 0.0 | [0.0, 0.00275] |
| snapmerge_recall_main | 1662 | 1662 | 1.0 | 0.9982 | [0.99778, 1.0] |
| replay_neither_order_per_merge_main | 0 | 747 | 0.004 | 0.0 | [0.0, 0.00493] |
| snapmerge_unsound_per_merge_nyc | 0 | 1258 | 0.00238 | 0.0 | [0.0, 0.00293] |
| snapmerge_recall_nyc | 1745 | 1745 | 1.0 | 0.99828 | [0.99789, 1.0] |
| replay_neither_order_per_merge_nyc | 0 | 733 | 0.00408 | 0.0 | [0.0, 0.00502] |
| snapmerge_all_variants_all_difftest_campaigns_unsound_per_merge | 0 | 18497 | 0.00016 | 0.0 | [0.0, 0.0002] |
| append_rule_reproduction_all_difftest_campaigns_neither_order_per_merge | 35 | 3139 | 0.01476 | 0.00825 | [0.00778, 0.01547] |

Unsound replay / cherry-pick counterexamples by writer encoding. encoding_dependent = the SOURCE side holds a copy-on-write UPSERT or OVERWRITE with inserted rows whose deletion predicate matched no live row of the source branch just before it; the project writer (PyIceberg 0.12.0 delete-then-append) then commits only an 'append' snapshot, which replay and iterated cherry-pick re-add without validation. This classifies the cases; it does not test replay under another encoding.

- Replay (#13959): 11 cases; encoding-dependent: minimised 10, original 10; minimised cases without such an op: replay_unsound_fuzz2684; unclassified: none.
- Iterated cherry-pick: 6 cases; encoding-dependent: minimised 6, original 6; minimised cases without such an op: none; unclassified: none.

| Case | Policy | True conflict | Layout | Encoding-dependent (minimised) | Source append-only COW ops (minimised) | Encoding-dependent (original) |
|---|---|---|---|---|---|---|
| cherry_pick_unsound_fuzz1609 | Iterated cherry-pick | False | none | True | UPSERT#0 (1 rows) | True |
| cherry_pick_unsound_fuzz1847 | Iterated cherry-pick | False | none | True | UPSERT#0 (1 rows) | True |
| cherry_pick_unsound_fuzz591 | Iterated cherry-pick | False | month | True | UPSERT#0 (1 rows) | True |
| cherry_pick_unsound_fuzz604 | Iterated cherry-pick | True | month | True | OVERWRITE#0 (1 rows) | True |
| cherry_pick_unsound_fuzz660 | Iterated cherry-pick | True | identity_k | True | UPSERT#0 (1 rows) | True |
| cherry_pick_unsound_fuzz753 | Iterated cherry-pick | False | identity_k | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz1609 | Replay (#13959) | False | none | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz1847 | Replay (#13959) | False | none | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz2684 | Replay (#13959) | True | identity_k | False | - | False |
| replay_unsound_fuzz3188 | Replay (#13959) | True | none | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz3649 | Replay (#13959) | False | identity_k | True | OVERWRITE#0 (1 rows) | True |
| replay_unsound_fuzz439 | Replay (#13959) | False | identity_k | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz591 | Replay (#13959) | False | month | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz604 | Replay (#13959) | True | month | True | OVERWRITE#0 (1 rows) | True |
| replay_unsound_fuzz660 | Replay (#13959) | True | identity_k | True | UPSERT#0 (1 rows) | True |
| replay_unsound_fuzz753 | Replay (#13959) | False | identity_k | True | UPSERT#0 (1 rows) | True |
| replay_unsound_identical_new_key_upserts | Replay (#13959) | False | month | True | UPSERT#0 (1 rows) | True |

## 18. Second data source: NYC TLC yellow taxi 2023-01 (`difftest_main_nyc_v4`)

Source `nyc_yellow`, months 2023-01, partitioned by day(tpep_pickup_datetime); base 2000 rows, 429 scenarios per mix, seed base 20261002, the same 13 policies, mixes and generator as the TPC-H main campaign (scenarios pick four consecutive day partitions instead of month partitions). 3003 scenarios, 1745 true conflicts (58.1%; TPC-H main 55.3%).

Writer checks 6006/6006 pass (0 reader disagreements); 17121 of 17121 emitted merges (all policies) compared with both orders in both readers. Load average (1 min): {'min': 3.16, 'median': 6.79, 'max': 10.3}. Cross-check: ok.

### 18.1 Table 1 (NYC): recall / precision / unsound merges per mix

| Mix | n | True conf. | Commuting | SnapMerge | Replay (#13959) | Iterated cherry-pick | Row-level 3-way | lakeFS policy | Nessie NORMAL | Nessie FORCE | Fast-forward only |
|---|---|---|---|---|---|---|---|---|---|---|---|
| All mixes | 3003 | 1745 | 1258 | 1.000 / 1.000 / 0 | 0.870 / 0.669 / 0 | 0.903 / 0.632 / 0 | 0.368 / 1.000 / 183 | 1.000 / 0.613 / 0 | 1.000 / 0.581 / 0 | 0.000 / - / 2785 | 1.000 / 0.581 / 0 |
| Partition-disjoint ingest | 429 | 79 | 350 | 1.000 / 1.000 / 0 | 0.658 / 0.963 / 0 | 0.734 / 0.349 / 0 | 0.063 / 1.000 / 7 | 1.000 / 0.184 / 0 | 1.000 / 0.184 / 0 | 0.000 / - / 428 | 1.000 / 0.184 / 0 |
| Filter corrections | 429 | 397 | 32 | 1.000 / 1.000 / 0 | 0.902 / 0.968 / 0 | 0.940 / 0.937 / 0 | 0.783 / 1.000 / 6 | 1.000 / 0.925 / 0 | 1.000 / 0.925 / 0 | 0.000 / - / 422 | 1.000 / 0.925 / 0 |
| Upsert by key | 429 | 211 | 218 | 1.000 / 1.000 / 0 | 1.000 / 0.542 / 0 | 1.000 / 0.542 / 0 | 1.000 / 1.000 / 0 | 1.000 / 0.492 / 0 | 1.000 / 0.492 / 0 | 0.000 / - / 429 | 1.000 / 0.492 / 0 |
| Deletes (MOR) | 429 | 297 | 132 | 1.000 / 1.000 / 0 | 0.852 / 0.699 / 0 | 0.875 / 0.670 / 0 | 0.000 / - / 47 | 1.000 / 0.692 / 0 | 1.000 / 0.692 / 0 | 0.000 / - / 427 | 1.000 / 0.692 / 0 |
| Deletes (COW) | 429 | 308 | 121 | 1.000 / 1.000 / 0 | 0.821 / 0.697 / 0 | 0.864 / 0.687 / 0 | 0.000 / - / 54 | 1.000 / 0.718 / 0 | 1.000 / 0.718 / 0 | 0.000 / - / 422 | 1.000 / 0.718 / 0 |
| Compaction on main | 429 | 157 | 272 | 1.000 / 1.000 / 0 | 0.975 / 0.384 / 0 | 0.981 / 0.376 / 0 | 0.172 / 1.000 / 3 | 1.000 / 0.547 / 0 | 1.000 / 0.366 / 0 | 0.000 / - / 287 | 1.000 / 0.366 / 0 |
| Adversarial overlap | 429 | 296 | 133 | 1.000 / 1.000 / 0 | 0.804 / 0.713 / 0 | 0.855 / 0.713 / 0 | 0.301 / 1.000 / 66 | 1.000 / 0.715 / 0 | 1.000 / 0.690 / 0 | 0.000 / - / 370 | 1.000 / 0.690 / 0 |

### 18.2 Sound auto-merge rate of commuting scenarios per mix, NYC vs TPC-H main (95% CI)

| Mix | NYC commuting | NYC SnapMerge | NYC replay | NYC cherry-pick | NYC row-level (sound only) | NYC SnapMerge - replay (paired) | TPC-H commuting | TPC-H replay | TPC-H SnapMerge - replay (paired) |
|---|---|---|---|---|---|---|---|---|---|
| All mixes | 1258 | 1.000 [1.000, 1.000] | 0.402 [0.382, 0.423] | 0.270 [0.249, 0.291] | 0.967 [0.958, 0.976] | +0.598 [+0.577, +0.618] | 1341 | 0.415 [0.395, 0.435] | +0.585 [+0.565, +0.605] |
| Partition-disjoint ingest | 350 | 1.000 [1.000, 1.000] | 0.994 [0.986, 1.000] | 0.691 [0.643, 0.739] | 1.000 [1.000, 1.000] | +0.006 [+0.000, +0.015] | 364 | 1.000 [1.000, 1.000] | +0.000 [+0.000, +0.000] |
| Filter corrections | 32 | 1.000 [1.000, 1.000] | 0.625 [0.457, 0.788] | 0.219 [0.083, 0.371] | 1.000 [1.000, 1.000] | +0.375 [+0.212, +0.543] | 38 | 0.789 [0.655, 0.911] | +0.210 [+0.089, +0.345] |
| Upsert by key | 218 | 1.000 [1.000, 1.000] | 0.183 [0.134, 0.236] | 0.183 [0.134, 0.236] | 1.000 [1.000, 1.000] | +0.817 [+0.764, +0.866] | 219 | 0.205 [0.154, 0.259] | +0.794 [+0.741, +0.846] |
| Deletes (MOR) | 132 | 1.000 [1.000, 1.000] | 0.174 [0.113, 0.241] | 0.030 [0.007, 0.062] | 1.000 [1.000, 1.000] | +0.826 [+0.759, +0.887] | 130 | 0.146 [0.087, 0.209] | +0.854 [+0.791, +0.913] |
| Deletes (COW) | 121 | 1.000 [1.000, 1.000] | 0.091 [0.043, 0.146] | 0.000 [0.000, 0.000] | 1.000 [1.000, 1.000] | +0.909 [+0.854, +0.957] | 151 | 0.199 [0.135, 0.264] | +0.801 [+0.736, +0.865] |
| Compaction on main | 272 | 1.000 [1.000, 1.000] | 0.099 [0.065, 0.137] | 0.059 [0.032, 0.088] | 1.000 [1.000, 1.000] | +0.901 [+0.864, +0.935] | 281 | 0.071 [0.043, 0.103] | +0.929 [+0.897, +0.957] |
| Adversarial overlap | 133 | 1.000 [1.000, 1.000] | 0.278 [0.204, 0.356] | 0.233 [0.163, 0.306] | 0.692 [0.611, 0.769] | +0.722 [+0.644, +0.796] | 158 | 0.304 [0.234, 0.376] | +0.696 [+0.624, +0.765] |

### 18.3 All mixes pooled: NYC vs TPC-H main

| Policy | NYC TP/FP/FN/TN | NYC unsound | NYC recall | NYC precision | NYC sound auto-merge of commuting | NYC median latency ms | NYC mean KB read | TPC-H TP/FP/FN/TN | TPC-H unsound | TPC-H sound auto-merge of commuting |
|---|---|---|---|---|---|---|---|---|---|---|
| SnapMerge | 1745/0/0/1258 | 0 | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 13.9 [13.6, 14.2] | 47.1 | 1662/0/0/1341 | 0 | 1.000 [1.000, 1.000] |
| SnapMerge w/o net-effect test | 1745/53/0/1205 | 0 | 1.000 [1.000, 1.000] | 0.971 [0.963, 0.978] | 0.958 [0.947, 0.968] | 10.8 [10.6, 11.1] | 29.6 | 1662/64/0/1277 | 0 | 0.952 [0.941, 0.963] |
| SnapMerge w/o compaction rebase | 1745/267/0/991 | 0 | 1.000 [1.000, 1.000] | 0.867 [0.856, 0.878] | 0.788 [0.771, 0.804] | 12.3 [12.0, 12.6] | 40.1 | 1662/300/0/1041 | 0 | 0.776 [0.760, 0.792] |
| SnapMerge w/o delete union | 1745/439/0/819 | 0 | 1.000 [1.000, 1.000] | 0.799 [0.783, 0.815] | 0.651 [0.629, 0.673] | 12.3 [12.1, 12.6] | 26.3 | 1662/448/0/893 | 0 | 0.666 [0.644, 0.688] |
| SnapMerge w/o bounded reads | 1745/680/0/578 | 0 | 1.000 [1.000, 1.000] | 0.720 [0.703, 0.736] | 0.460 [0.439, 0.480] | 8.2 [8.1, 8.4] | 0.0 | 1662/725/0/616 | 0 | 0.459 [0.440, 0.479] |
| SnapMerge file-level rules only | 1745/672/0/586 | 0 | 1.000 [1.000, 1.000] | 0.722 [0.706, 0.737] | 0.466 [0.441, 0.491] | 8.0 [7.9, 8.1] | 10.0 | 1662/695/0/646 | 0 | 0.482 [0.458, 0.505] |
| Replay (#13959) | 1518/752/227/506 | 0 | 0.870 [0.854, 0.886] | 0.669 [0.650, 0.686] | 0.402 [0.382, 0.423] | 8.2 [8.0, 8.3] | 0.0 | 1471/785/191/556 | 0 | 0.415 [0.395, 0.435] |
| Iterated cherry-pick | 1575/918/170/340 | 0 | 0.903 [0.889, 0.916] | 0.632 [0.614, 0.649] | 0.270 [0.249, 0.291] | 7.7 [7.6, 7.8] | 0.0 | 1503/959/159/382 | 0 | 0.285 [0.264, 0.305] |
| Row-level 3-way | 643/0/1102/1258 | 183 | 0.368 [0.351, 0.386] | 1.000 [1.000, 1.000] | 0.967 [0.958, 0.976] | 27.9 [27.8, 28.1] | 279.5 | 647/2/1015/1339 | 196 | 0.965 [0.956, 0.974] |
| lakeFS policy | 1745/1101/0/157 | 0 | 1.000 [1.000, 1.000] | 0.613 [0.597, 0.629] | 0.125 [0.109, 0.141] | 6.9 [6.8, 7.0] | 0.0 | 1662/1156/0/185 | 0 | 0.138 [0.122, 0.153] |
| Nessie NORMAL | 1745/1258/0/0 | 0 | 1.000 [1.000, 1.000] | 0.581 [0.565, 0.596] | 0.000 [0.000, 0.000] | 6.7 [6.6, 6.8] | 0.0 | 1662/1341/0/0 | 0 | 0.000 [0.000, 0.000] |
| Nessie FORCE | 0/0/1745/1258 | 2785 | 0.000 [0.000, 0.000] | n/a | 0.144 [0.127, 0.161] | 9.4 [9.3, 9.5] | 0.0 | 0/0/1662/1341 | 2756 | 0.156 [0.139, 0.173] |
| Fast-forward only | 1745/1258/0/0 | 0 | 1.000 [1.000, 1.000] | 0.581 [0.565, 0.596] | 0.000 [0.000, 0.000] | 6.8 [6.7, 6.9] | 0.0 | 1662/1341/0/0 | 0 | 0.000 [0.000, 0.000] |

### 18.4 Paired comparisons against SnapMerge (NYC, all mixes)

| Policy | SnapMerge - policy: sound auto-merge of commuting | SnapMerge - policy: recall | Latency policy/SnapMerge, both merged soundly (geomean) | Latency policy/SnapMerge, all scenarios |
|---|---|---|---|---|
| SnapMerge w/o net-effect test | +0.042 [+0.032, +0.053] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=1205) | 0.80 [0.79, 0.81] (n=3003) |
| SnapMerge w/o compaction rebase | +0.212 [+0.196, +0.229] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=991) | 0.89 [0.88, 0.90] (n=3003) |
| SnapMerge w/o delete union | +0.349 [+0.327, +0.371] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.02] (n=819) | 0.85 [0.83, 0.86] (n=3003) |
| SnapMerge w/o bounded reads | +0.540 [+0.520, +0.561] | +0.000 [+0.000, +0.000] | 0.99 [0.98, 1.00] (n=578) | 0.55 [0.54, 0.56] (n=3003) |
| SnapMerge file-level rules only | +0.534 [+0.509, +0.559] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=586) | 0.57 [0.56, 0.57] (n=3003) |
| Replay (#13959) | +0.598 [+0.577, +0.618] | +0.130 [+0.114, +0.146] | 1.35 [1.32, 1.39] (n=506) | 0.57 [0.56, 0.58] (n=3003) |
| Iterated cherry-pick | +0.730 [+0.709, +0.751] | +0.097 [+0.084, +0.111] | 1.41 [1.36, 1.47] (n=340) | 0.53 [0.52, 0.54] (n=3003) |
| Row-level 3-way | +0.033 [+0.024, +0.042] | +0.631 [+0.614, +0.649] | 1.66 [1.61, 1.70] (n=1217) | 1.85 [1.82, 1.88] (n=3003) |
| lakeFS policy | +0.875 [+0.859, +0.891] | +0.000 [+0.000, +0.000] | 0.39 [0.37, 0.41] (n=157) | 0.47 [0.46, 0.47] (n=3003) |
| Nessie NORMAL | +1.000 [+1.000, +1.000] | +0.000 [+0.000, +0.000] | n/a | 0.46 [0.45, 0.46] (n=3003) |
| Nessie FORCE | +0.856 [+0.839, +0.873] | +1.000 [+1.000, +1.000] | 0.37 [0.35, 0.39] (n=181) | 0.64 [0.63, 0.65] (n=3003) |
| Fast-forward only | +1.000 [+1.000, +1.000] | +0.000 [+0.000, +0.000] | n/a | 0.46 [0.45, 0.46] (n=3003) |
| Rebase-OCC | +0.446 [+0.424, +0.468] | +0.178 [+0.161, +0.196] | 1.37 [1.34, 1.40] (n=697) | 0.70 [0.68, 0.71] (n=3003) |
| Recompute | +0.000 [+0.000, +0.000] | +0.000 [+0.000, +0.000] | 1.90 [1.85, 1.95] (n=1258) | 1.91 [1.88, 1.93] (n=3003) |
| Intent replay (pairwise) | +0.447 [+0.425, +0.469] | +0.000 [+0.000, +0.000] | 1.42 [1.39, 1.45] (n=696) | 0.67 [0.66, 0.68] (n=3003) |
| Exact replay | +0.443 [+0.421, +0.465] | +0.000 [+0.000, +0.000] | 1.41 [1.39, 1.44] (n=701) | 0.83 [0.81, 0.85] (n=3003) |
| SnapMerge, append-snapshot insert rule | +0.000 [+0.000, +0.000] | +0.000 [+0.000, +0.000] | 1.00 [0.99, 1.01] (n=1258) | 1.00 [0.99, 1.00] (n=3003) |

Nessie FORCE (NYC): 2785 of 3003 merges equal neither order; 2073 lose rows against both orders; 228997 rows lost and 630993 extra against the closer order.
Row-level (NYC): 183 of 2360 merges equal neither order (41 on commuting, 142 on true conflicts).

### 18.5 SnapMerge variants (NYC vs TPC-H main): full minus variant, paired

| Variant | NYC sound auto-merge of commuting | NYC full - variant | NYC unsound | TPC-H main sound auto-merge of commuting | TPC-H main full - variant |
|---|---|---|---|---|---|
| SnapMerge w/o net-effect test | 0.958 [0.947, 0.968] | +0.042 [+0.032, +0.053] | 0 | 0.952 [0.941, 0.963] | +0.048 [+0.037, +0.059] |
| SnapMerge w/o compaction rebase | 0.788 [0.771, 0.804] | +0.212 [+0.196, +0.229] | 0 | 0.776 [0.760, 0.792] | +0.224 [+0.208, +0.239] |
| SnapMerge w/o delete union | 0.651 [0.629, 0.673] | +0.349 [+0.327, +0.371] | 0 | 0.666 [0.644, 0.688] | +0.334 [+0.312, +0.356] |
| SnapMerge w/o bounded reads | 0.460 [0.439, 0.480] | +0.540 [+0.520, +0.561] | 0 | 0.459 [0.440, 0.479] | +0.541 [+0.521, +0.560] |
| SnapMerge file-level rules only | 0.466 [0.441, 0.491] | +0.534 [+0.509, +0.559] | 0 | 0.482 [0.458, 0.505] | +0.518 [+0.495, +0.542] |

NYC workload: 3003 scenarios, 13945 operations; operations per side min 1, median 2.0, max 4. Operation kinds: APPEND[cow] 4563, DELETE[cow] 2942, DELETE[mor] 2541, OVERWRITE[cow] 1580, OVERWRITE[mor] 30, REPLACE[cow] 480, UPSERT[cow] 857, UPSERT[mor] 952.
SnapMerge mechanisms in the NYC campaign's 1258 merges: bounds:cannot-match 610, pair:R2 602, contended-files 554, pair:R3 431, pair:R1 416, pair:R6 304, bounded-read 294, R3:cow-to-position 293, R6:compaction-rebase 267, bounded-read:clear 261, R3:delete-file-union 156, R4:key-check 153, pair:R4 153, pair:R5 105, net:masked-by-later-op 40, net:duplicates-removed 13, net:exact-equal 13; carrying_source_files 1038, with_contended_partitions 554, writing_new_position_delete_files 518.

Missed conflicts (merges of true conflicts) in the NYC campaign by the serial order they equal:

| Policy | merged_true_conflicts | equal_A_then_C_only | equal_C_then_A_only | equal_neither | equal_both |
|---|---|---|---|---|---|
| Iterated cherry-pick | 170 | 170 | 0 | 0 | 0 |
| Nessie FORCE | 1745 | 37 | 0 | 1708 | 0 |
| Rebase-OCC | 311 | 311 | 0 | 0 | 0 |
| Replay (#13959) | 227 | 220 | 7 | 0 | 0 |
| Row-level 3-way | 1102 | 449 | 511 | 142 | 0 |

Median time to a decision by outcome in the NYC campaign (ms; n):

| Policy | TP | FP | FN | TN | error |
|---|---|---|---|---|---|
| Iterated cherry-pick | 7.66 (n=1575) | 5.96 (n=918) | 11.5 (n=170) | 14.48 (n=340) |  |
| Fast-forward only | 7.43 (n=1745) | 5.85 (n=1258) |  |  |  |
| lakeFS policy | 7.42 (n=1745) | 6.17 (n=1101) |  | 7.02 (n=157) |  |
| Nessie FORCE |  |  | 10.15 (n=1745) | 8.62 (n=1258) |  |
| Nessie NORMAL | 7.43 (n=1745) | 5.88 (n=1258) |  |  |  |
| Rebase-OCC | 9.51 (n=1434) | 6.56 (n=561) | 14.28 (n=311) | 15.12 (n=697) |  |
| Recompute | 25.4 (n=1745) |  |  | 30.99 (n=1258) |  |
| Replay (#13959) | 7.66 (n=1518) | 5.99 (n=752) | 12.66 (n=227) | 14.41 (n=506) |  |
| Intent replay (pairwise) | 9.25 (n=1745) | 6.72 (n=562) |  | 15.85 (n=696) |  |
| Exact replay | 12.97 (n=1745) | 6.86 (n=557) |  | 15.59 (n=701) |  |
| Row-level 3-way | 22.89 (n=643) |  | 30.15 (n=1102) | 27.63 (n=1258) |  |
| SnapMerge | 13.32 (n=1745) |  |  | 14.74 (n=1258) |  |
| SnapMerge, append-snapshot insert rule | 13.28 (n=1745) |  |  | 14.84 (n=1258) |  |
| SnapMerge file-level rules only | 7.55 (n=1745) | 7.02 (n=672) |  | 13.65 (n=586) |  |
| SnapMerge w/o bounded reads | 8.2 (n=1745) | 6.63 (n=680) |  | 10.12 (n=578) |  |
| SnapMerge w/o compaction rebase | 13.25 (n=1745) | 5.48 (n=267) |  | 13.08 (n=991) |  |
| SnapMerge w/o delete union | 13.22 (n=1745) | 8.01 (n=439) |  | 13.13 (n=819) |  |
| SnapMerge w/o net-effect test | 9.63 (n=1745) | 8.02 (n=53) |  | 14.39 (n=1205) |  |

Replay reports a conflict in 2270 of 3003 NYC scenarios (share 0.7559).
SnapMerge (NYC) without reads: median 10.12 ms (n=578); with reads: median 15.88 ms (n=2425).

NYC data: yellow_tripdata_2023-01.clean.parquet: 3,066,718 rows after cleaning (3,066,766 raw, 48 dropped), sha256 020d43ffbaa5ccbcf89289347e6692e89f061ea019a1004224422162b0669ea9.
Raw yellow_tripdata_2023-01.parquet: 47,673,370 bytes, sha256 32df6f67578fa86c484a6b5ef23a5281992ff085521082340b0f9e5889e9a572, retrieved 2026-10-04 from https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet. License: NYC Open Data terms of use, https://www.nyc.gov/home/terms-of-use.page.

H16 (SUPPORTED): difftest_main_nyc_v4 (3003 scenarios, 1745 true conflicts): SnapMerge TP 1745, FP 0, FN 0, TN 1258, 0 of 1258 merges unsound (exact 95% upper bound 0.00293), recall 1745/1745 (exact 95% CI [0.99789, 1.0]); sound auto-merge of commuting SnapMerge 1.000 [1.000, 1.000] vs replay 0.402 [0.382, 0.423]; paired difference +0.598 [+0.577, +0.618] (TPC-H main +0.585 [+0.565, +0.605]). Replay TP 1518, FP 752, FN 227, TN 506, 0 of 733 merges equal neither order. Per mix: partition_disjoint_ingest +0.006 [+0.000, +0.015] (TPC-H +0.000); filter_corrections +0.375 [+0.212, +0.543] (TPC-H +0.210); upsert_keys +0.817 [+0.764, +0.866] (TPC-H +0.794); deletes_mor +0.826 [+0.759, +0.887] (TPC-H +0.854); deletes_cow +0.909 [+0.854, +0.957] (TPC-H +0.801); compaction_on_main +0.901 [+0.864, +0.935] (TPC-H +0.929); adversarial_overlap +0.722 [+0.644, +0.796] (TPC-H +0.696). CI excludes 0 on 6 of 7 mixes (TPC-H: 6 of 7).

Reproduce:`cd <WORKSPACE> && .venv/bin/python 05_snapmerge_branch_merge/analyze.py`


# Revision analyses (2026-10-05)

Sections 19-30 answer the review package. Definitions are in `analysis_revision.py`.


## 19a. Contract-normalized outcomes (`difftest_main_tpch_v5`)

| Policy | Comm.: merged = both | Comm.: merged = neither | Comm.: rejected | TC: rejected | TC: merged = (A;C) only | TC: merged = (C;A) only | TC: merged = neither | Errors |
|---|---|---|---|---|---|---|---|---|
| SnapMerge | 1341 | 0 | 0 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge w/o net-effect test | 1277 | 0 | 64 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge w/o compaction rebase | 1041 | 0 | 300 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge w/o delete union | 893 | 0 | 448 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge w/o bounded reads | 616 | 0 | 725 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge file-level rules only | 646 | 0 | 695 | 1662 | 0 | 0 | 0 | 0 |
| Replay (#13959) | 556 | 0 | 785 | 1471 | 182 | 9 | 0 | 0 |
| Iterated cherry-pick | 382 | 0 | 959 | 1503 | 159 | 0 | 0 | 0 |
| Row-level 3-way | 1294 | 45 | 2 | 647 | 386 | 478 | 151 | 0 |
| lakeFS policy | 185 | 0 | 1156 | 1662 | 0 | 0 | 0 | 0 |
| Nessie NORMAL | 0 | 0 | 1341 | 1662 | 0 | 0 | 0 | 0 |
| Nessie FORCE | 209 | 1132 | 0 | 0 | 38 | 0 | 1624 | 0 |
| Fast-forward only | 0 | 0 | 1341 | 1662 | 0 | 0 | 0 | 0 |
| Rebase, one-direction validation (OCC) | 745 | 0 | 596 | 1393 | 269 | 0 | 0 | 0 |
| Recompute both orders (reference) | 1341 | 0 | 0 | 1662 | 0 | 0 | 0 | 0 |
| Replay + recorded predicates (pairwise) | 741 | 0 | 600 | 1662 | 0 | 0 | 0 | 0 |
| Replay + exact net-effect test | 746 | 0 | 595 | 1662 | 0 | 0 | 0 | 0 |
| SnapMerge, submitted insert rule | 1341 | 0 | 0 | 1662 | 0 | 0 | 0 | 0 |

## 20a. Information-matched controls and mechanism attribution (`difftest_main_tpch_v5`)

| Group | Replay (#13959) | Replay + recorded predicates (pairwise) | Replay + exact net-effect test | Rebase, one-direction validation (OCC) | SnapMerge w/o net-effect test | SnapMerge | Recompute both orders (reference) |
|---|---|---|---|---|---|---|---|
| all | 0.415 [0.395, 0.435] | 0.553 [0.532, 0.574] | 0.556 [0.535, 0.578] | 0.556 [0.535, 0.577] | 0.952 [0.941, 0.963] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| partition_disjoint_ingest | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| filter_corrections | 0.789 [0.653, 0.912] | 0.868 [0.750, 0.971] | 0.868 [0.750, 0.971] | 0.868 [0.750, 0.971] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| upsert_keys | 0.205 [0.155, 0.260] | 0.489 [0.425, 0.553] | 0.489 [0.425, 0.553] | 0.489 [0.425, 0.553] | 0.977 [0.956, 0.995] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| deletes_mor | 0.146 [0.087, 0.210] | 0.962 [0.925, 0.992] | 1.000 [1.000, 1.000] | 0.992 [0.975, 1.000] | 0.962 [0.925, 0.992] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| deletes_cow | 0.199 [0.138, 0.265] | 0.232 [0.167, 0.301] | 0.232 [0.167, 0.301] | 0.232 [0.167, 0.301] | 0.954 [0.916, 0.985] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| compaction_on_main | 0.071 [0.042, 0.103] | 0.085 [0.054, 0.120] | 0.085 [0.054, 0.120] | 0.085 [0.054, 0.120] | 0.993 [0.982, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| adversarial_overlap | 0.304 [0.234, 0.378] | 0.335 [0.262, 0.409] | 0.335 [0.262, 0.409] | 0.335 [0.262, 0.409] | 0.715 [0.644, 0.785] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |

Paired differences of the sound auto-merge rate of commuting scenarios:

| Group | recorded intent (replay_intent - replay) | exact net-effect test under replay (replay_intent_exact - replay_intent) | composition under pairwise decisions (snapmerge[no_net_effect] - replay_intent) | composition under exact decisions (snapmerge - replay_intent_exact) | exact test under composition (snapmerge - snapmerge[no_net_effect]) | total (snapmerge - replay) |
|---|---|---|---|---|---|---|
| all | 0.138 [0.122, 0.155] | 0.004 [0.001, 0.007] | 0.400 [0.379, 0.420] | 0.444 [0.422, 0.465] | 0.048 [0.037, 0.059] | 0.585 [0.565, 0.605] |
| partition_disjoint_ingest | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] |
| filter_corrections | 0.079 [0.000, 0.175] | 0.000 [0.000, 0.000] | 0.132 [0.029, 0.250] | 0.132 [0.029, 0.250] | 0.000 [0.000, 0.000] | 0.210 [0.088, 0.347] |
| upsert_keys | 0.283 [0.226, 0.344] | 0.000 [0.000, 0.000] | 0.489 [0.424, 0.552] | 0.511 [0.447, 0.575] | 0.023 [0.005, 0.044] | 0.794 [0.740, 0.845] |
| deletes_mor | 0.815 [0.746, 0.880] | 0.038 [0.008, 0.075] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.038 [0.008, 0.075] | 0.854 [0.789, 0.913] |
| deletes_cow | 0.033 [0.007, 0.064] | 0.000 [0.000, 0.000] | 0.722 [0.649, 0.792] | 0.768 [0.699, 0.833] | 0.046 [0.015, 0.084] | 0.801 [0.735, 0.862] |
| compaction_on_main | 0.014 [0.003, 0.029] | 0.000 [0.000, 0.000] | 0.907 [0.872, 0.940] | 0.915 [0.880, 0.946] | 0.007 [0.000, 0.018] | 0.929 [0.897, 0.958] |
| adversarial_overlap | 0.032 [0.006, 0.062] | 0.000 [0.000, 0.000] | 0.380 [0.306, 0.456] | 0.665 [0.591, 0.738] | 0.285 [0.215, 0.356] | 0.696 [0.622, 0.766] |

Conflict reasons, Replay + recorded predicates (pairwise): true conflict: logical: R2: 1052; true conflict: logical: R5: 361; commuting: physical: source removed a file the target no longer has: 254; commuting: physical: source delete file references a file the target removed: 254; true conflict: logical: R4: 249; commuting: logical: R2: 55; commuting: physical: target delete file references a file the source removed: 28; commuting: logical: R4: 5

Conflict reasons, Replay + exact net-effect test: true conflict: logical: R2: 1041; true conflict: logical: R5: 371; commuting: physical: source removed a file the target no longer has: 283; commuting: physical: source delete file references a file the target removed: 265; true conflict: logical: R4: 250; commuting: physical: target delete file references a file the source removed: 42; commuting: physical: physical:duplicate-reconciliation: 5

Conflict reasons, Rebase, one-direction validation (OCC): true conflict: logical: R2: 679; true conflict: logical: R5: 282; commuting: physical: source removed a file the target no longer has: 263; commuting: physical: source delete file references a file the target removed: 259; true conflict: logical: R4: 249; true conflict: physical: source removed a file the target no longer has: 139; true conflict: physical: source delete file references a file the target removed: 44; commuting: logical: R2: 38

## 19b. Contract-normalized outcomes (`difftest_main_nyc_v5`)

| Policy | Comm.: merged = both | Comm.: merged = neither | Comm.: rejected | TC: rejected | TC: merged = (A;C) only | TC: merged = (C;A) only | TC: merged = neither | Errors |
|---|---|---|---|---|---|---|---|---|
| SnapMerge | 1258 | 0 | 0 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge w/o net-effect test | 1205 | 0 | 53 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge w/o compaction rebase | 991 | 0 | 267 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge w/o delete union | 819 | 0 | 439 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge w/o bounded reads | 578 | 0 | 680 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge file-level rules only | 586 | 0 | 672 | 1745 | 0 | 0 | 0 | 0 |
| Replay (#13959) | 506 | 0 | 752 | 1518 | 220 | 7 | 0 | 0 |
| Iterated cherry-pick | 340 | 0 | 918 | 1575 | 170 | 0 | 0 | 0 |
| Row-level 3-way | 1217 | 41 | 0 | 643 | 449 | 511 | 142 | 0 |
| lakeFS policy | 157 | 0 | 1101 | 1745 | 0 | 0 | 0 | 0 |
| Nessie NORMAL | 0 | 0 | 1258 | 1745 | 0 | 0 | 0 | 0 |
| Nessie FORCE | 181 | 1077 | 0 | 0 | 37 | 0 | 1708 | 0 |
| Fast-forward only | 0 | 0 | 1258 | 1745 | 0 | 0 | 0 | 0 |
| Rebase, one-direction validation (OCC) | 697 | 0 | 561 | 1434 | 311 | 0 | 0 | 0 |
| Recompute both orders (reference) | 1258 | 0 | 0 | 1745 | 0 | 0 | 0 | 0 |
| Replay + recorded predicates (pairwise) | 696 | 0 | 562 | 1745 | 0 | 0 | 0 | 0 |
| Replay + exact net-effect test | 701 | 0 | 557 | 1745 | 0 | 0 | 0 | 0 |
| SnapMerge, submitted insert rule | 1258 | 0 | 0 | 1745 | 0 | 0 | 0 | 0 |

## 20b. Information-matched controls and mechanism attribution (`difftest_main_nyc_v5`)

| Group | Replay (#13959) | Replay + recorded predicates (pairwise) | Replay + exact net-effect test | Rebase, one-direction validation (OCC) | SnapMerge w/o net-effect test | SnapMerge | Recompute both orders (reference) |
|---|---|---|---|---|---|---|---|
| all | 0.402 [0.382, 0.423] | 0.553 [0.531, 0.576] | 0.557 [0.535, 0.579] | 0.554 [0.531, 0.576] | 0.958 [0.947, 0.968] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| partition_disjoint_ingest | 0.994 [0.986, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| filter_corrections | 0.625 [0.448, 0.789] | 0.719 [0.556, 0.872] | 0.719 [0.556, 0.872] | 0.719 [0.556, 0.872] | 0.969 [0.897, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| upsert_keys | 0.183 [0.133, 0.236] | 0.445 [0.379, 0.509] | 0.445 [0.379, 0.509] | 0.445 [0.379, 0.509] | 0.963 [0.937, 0.986] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| deletes_mor | 0.174 [0.111, 0.241] | 0.962 [0.927, 0.992] | 1.000 [1.000, 1.000] | 0.970 [0.938, 0.993] | 0.962 [0.927, 0.992] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| deletes_cow | 0.091 [0.042, 0.145] | 0.132 [0.074, 0.195] | 0.132 [0.074, 0.195] | 0.132 [0.074, 0.195] | 0.975 [0.942, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| compaction_on_main | 0.099 [0.065, 0.136] | 0.118 [0.080, 0.158] | 0.118 [0.080, 0.158] | 0.118 [0.080, 0.158] | 0.996 [0.989, 1.000] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |
| adversarial_overlap | 0.278 [0.203, 0.356] | 0.384 [0.303, 0.467] | 0.384 [0.303, 0.467] | 0.384 [0.303, 0.467] | 0.737 [0.661, 0.811] | 1.000 [1.000, 1.000] | 1.000 [1.000, 1.000] |

Paired differences of the sound auto-merge rate of commuting scenarios:

| Group | recorded intent (replay_intent - replay) | exact net-effect test under replay (replay_intent_exact - replay_intent) | composition under pairwise decisions (snapmerge[no_net_effect] - replay_intent) | composition under exact decisions (snapmerge - replay_intent_exact) | exact test under composition (snapmerge - snapmerge[no_net_effect]) | total (snapmerge - replay) |
|---|---|---|---|---|---|---|
| all | 0.151 [0.133, 0.170] | 0.004 [0.001, 0.008] | 0.405 [0.383, 0.427] | 0.443 [0.421, 0.465] | 0.042 [0.032, 0.053] | 0.598 [0.577, 0.618] |
| partition_disjoint_ingest | 0.006 [0.000, 0.014] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.006 [0.000, 0.014] |
| filter_corrections | 0.094 [0.000, 0.207] | 0.000 [0.000, 0.000] | 0.250 [0.103, 0.409] | 0.281 [0.128, 0.444] | 0.031 [0.000, 0.103] | 0.375 [0.210, 0.552] |
| upsert_keys | 0.262 [0.203, 0.319] | 0.000 [0.000, 0.000] | 0.518 [0.455, 0.584] | 0.555 [0.491, 0.621] | 0.037 [0.014, 0.063] | 0.817 [0.764, 0.867] |
| deletes_mor | 0.788 [0.717, 0.856] | 0.038 [0.008, 0.073] | 0.000 [0.000, 0.000] | 0.000 [0.000, 0.000] | 0.038 [0.008, 0.073] | 0.826 [0.759, 0.889] |
| deletes_cow | 0.041 [0.009, 0.080] | 0.000 [0.000, 0.000] | 0.843 [0.777, 0.906] | 0.868 [0.805, 0.926] | 0.025 [0.000, 0.058] | 0.909 [0.855, 0.958] |
| compaction_on_main | 0.018 [0.004, 0.035] | 0.000 [0.000, 0.000] | 0.879 [0.838, 0.916] | 0.882 [0.842, 0.920] | 0.004 [0.000, 0.011] | 0.901 [0.864, 0.935] |
| adversarial_overlap | 0.105 [0.055, 0.159] | 0.000 [0.000, 0.000] | 0.353 [0.273, 0.436] | 0.617 [0.533, 0.697] | 0.263 [0.189, 0.339] | 0.722 [0.644, 0.797] |

Conflict reasons, Replay + recorded predicates (pairwise): true conflict: logical: R2: 1150; true conflict: logical: R5: 339; commuting: physical: source removed a file the target no longer has: 263; true conflict: logical: R4: 256; commuting: physical: source delete file references a file the target removed: 210; commuting: logical: R2: 38; commuting: physical: target delete file references a file the source removed: 36; commuting: logical: R4: 13

Conflict reasons, Replay + exact net-effect test: true conflict: logical: R2: 1136; true conflict: logical: R5: 353; commuting: physical: source removed a file the target no longer has: 289; true conflict: logical: R4: 256; commuting: physical: source delete file references a file the target removed: 216; commuting: physical: target delete file references a file the source removed: 39; commuting: physical: physical:duplicate-reconciliation: 13

Conflict reasons, Rebase, one-direction validation (OCC): true conflict: logical: R2: 711; commuting: physical: source removed a file the target no longer has: 272; true conflict: logical: R5: 265; true conflict: logical: R4: 256; commuting: physical: source delete file references a file the target removed: 214; true conflict: physical: source removed a file the target no longer has: 150; true conflict: physical: source delete file references a file the target removed: 48; commuting: physical: target delete file references a file the source removed: 36

## 21. Sensitivity to partition span, history length, selectivity and compaction

| Knob | Level | n | Commuting | TC share | Ops/side | SnapMerge | Replay+exact | Replay+intent | Replay | SnapMerge - replay | SnapMerge - replay+exact | SnapMerge unsound | Recall |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| all |  | 420 | 206 | 0.5095 | 2.29 | 1.000 [1.000, 1.000] | 0.505 [0.454, 0.558] | 0.500 [0.448, 0.554] | 0.398 [0.351, 0.449] | 0.602 [0.551, 0.649] | 0.495 [0.442, 0.546] | 0 | 214/214 |
| n_parts | 8 | 420 | 208 | 0.5048 | 2.37 | 1.000 [1.000, 1.000] | 0.548 [0.496, 0.602] | 0.548 [0.496, 0.602] | 0.423 [0.372, 0.476] | 0.577 [0.524, 0.628] | 0.452 [0.398, 0.504] | 0 | 212/212 |
| n_parts | 16 | 420 | 200 | 0.5238 | 2.3 | 1.000 [1.000, 1.000] | 0.550 [0.497, 0.606] | 0.550 [0.497, 0.606] | 0.430 [0.381, 0.481] | 0.570 [0.519, 0.619] | 0.450 [0.394, 0.502] | 0 | 220/220 |
| n_parts | 32 | 420 | 241 | 0.4262 | 2.34 | 1.000 [1.000, 1.000] | 0.573 [0.525, 0.621] | 0.569 [0.520, 0.617] | 0.398 [0.354, 0.445] | 0.602 [0.555, 0.646] | 0.427 [0.379, 0.475] | 0 | 179/179 |
| hist | 2 | 420 | 131 | 0.6881 | 4.13 | 1.000 [1.000, 1.000] | 0.565 [0.497, 0.633] | 0.550 [0.484, 0.618] | 0.466 [0.407, 0.530] | 0.534 [0.470, 0.593] | 0.435 [0.367, 0.503] | 0 | 289/289 |
| hist | 4 | 420 | 86 | 0.7952 | 7.45 | 1.000 [1.000, 1.000] | 0.616 [0.533, 0.706] | 0.605 [0.520, 0.696] | 0.558 [0.478, 0.646] | 0.442 [0.354, 0.522] | 0.384 [0.294, 0.467] | 0 | 334/334 |
| sel | 0.25 | 420 | 218 | 0.481 | 2.28 | 1.000 [1.000, 1.000] | 0.509 [0.458, 0.560] | 0.505 [0.453, 0.556] | 0.395 [0.349, 0.442] | 0.606 [0.558, 0.651] | 0.491 [0.440, 0.542] | 0 | 202/202 |
| sel | 4.0 | 420 | 194 | 0.5381 | 2.28 | 1.000 [1.000, 1.000] | 0.515 [0.462, 0.571] | 0.505 [0.451, 0.561] | 0.412 [0.363, 0.465] | 0.588 [0.535, 0.637] | 0.484 [0.429, 0.538] | 0 | 226/226 |
| compact_p | 0.5 | 420 | 206 | 0.5095 | 2.55 | 1.000 [1.000, 1.000] | 0.447 [0.391, 0.502] | 0.442 [0.386, 0.497] | 0.374 [0.323, 0.425] | 0.626 [0.575, 0.677] | 0.553 [0.498, 0.609] | 0 | 214/214 |
| compact_p | 1.0 | 420 | 206 | 0.5095 | 2.79 | 1.000 [1.000, 1.000] | 0.369 [0.315, 0.425] | 0.369 [0.315, 0.425] | 0.335 [0.282, 0.389] | 0.665 [0.611, 0.718] | 0.631 [0.575, 0.685] | 0 | 214/214 |

default level = the first 60 scenarios per mix of the main campaign (same seeds and generator defaults as the sensitivity campaigns)

## 22. Single-snapshot copy-on-write writer (`writer_single_v3`)

1001 scenarios, 523 true conflicts, 370 with at least one mixed snapshot; snapshot roles {'delete': 1782, 'insert': 1838, 'mixed': 783, 'replace': 163, 'marker': 21}.

| Policy | Sound merges | Conflicts reported | Missed (one order) | Neither order | False conflicts | Merges = (A;C) |
|---|---|---|---|---|---|---|
| Iterated cherry-pick | 144 | 465 | 58 | 0 | 334 | 202 |
| Rebase, one-direction validation (OCC) | 266 | 432 | 91 | 0 | 212 | 357 |
| Recompute both orders (reference) | 478 | 523 | 0 | 0 | 0 | 478 |
| Replay (#13959) | 199 | 452 | 71 | 0 | 279 | 267 |
| Replay + recorded predicates (pairwise) | 265 | 523 | 0 | 0 | 213 | 265 |
| Replay + exact net-effect test | 266 | 523 | 0 | 0 | 212 | 266 |
| Row-level 3-way | 462 | 199 | 263 | 77 | 0 | 580 |
| SnapMerge | 478 | 523 | 0 | 0 | 0 | 478 |
| SnapMerge, submitted insert rule | 477 | 460 | 28 | 35 | 1 | 492 |
| SnapMerge w/o net-effect test | 460 | 523 | 0 | 0 | 18 | 460 |

SnapMerge one-sided 95% bound on merges not equal to both orders: {'k': 0, 'n': 478, 'level': 0.95, 'upper_one_sided': 0.00625, 'lower_one_sided': 0.0, 'two_sided_95': [0.0, 0.00769]}; submitted insert rule: 63 merges of true conflicts (28 equal to one order only, 35 to neither order).

## 23 (main). Provenance-aware writer checks

3003 scenarios, 13907 operations, 16438 snapshots; operations with violations: 0; roles {'delete': 7914, 'insert': 7974, 'replace': 496, 'marker': 54}; violations none.

## 23 (writer_single). Provenance-aware writer checks

1001 scenarios, 4587 operations, 4587 snapshots; operations with violations: 0; roles {'delete': 1782, 'insert': 1838, 'mixed': 783, 'replace': 163, 'marker': 21}; violations none.

## 24 (main). SnapMerge conflict categories and witnesses

Categories {'logical-conflict': 1662}; logical witness kinds {'net-effect': 1662}; rules {'R2': 1041, 'R5': 371, 'R4': 250}; order difference (A;C)(v)-(C;A)(v) of the witness value {'-1': 1202, '1': 460} (non-zero in 1662).

## 25 (main). Latency by outcome, SnapMerge phases, order position

| Policy | Sound merge ms | Any merge ms | Rejection ms | Ran first ms | Ran later ms |
|---|---|---|---|---|---|
| Iterated cherry-pick | 18.16 | 16.56 | 8.2 | 9.8 | 9.05 |
| Fast-forward only |  |  | 7.93 | 8.41 | 7.89 |
| lakeFS policy | 8.98 | 8.98 | 8.12 | 8.06 | 8.18 |
| Nessie FORCE | 9.37 | 11.48 |  | 11.27 | 11.5 |
| Nessie NORMAL |  |  | 7.94 | 7.97 | 7.94 |
| Rebase, one-direction validation (OCC) | 18.68 | 18.07 | 10.44 | 13.76 | 12.49 |
| Recompute both orders (reference) | 45.37 | 45.37 | 37.0 | 41.43 | 40.75 |
| Replay (#13959) | 18.15 | 17.04 | 8.21 | 10.11 | 9.68 |
| Replay + recorded predicates (pairwise) | 20.05 | 20.05 | 10.58 | 11.55 | 11.9 |
| Replay + exact net-effect test | 19.88 | 19.88 | 13.83 | 16.3 | 15.37 |
| Row-level 3-way | 41.4 | 42.25 | 33.29 | 41.73 | 40.77 |
| SnapMerge | 19.62 | 19.62 | 17.04 | 17.55 | 18.41 |
| SnapMerge, submitted insert rule | 19.81 | 19.81 | 16.96 | 19.08 | 18.35 |
| SnapMerge file-level rules only | 19.26 | 19.26 | 8.64 | 10.14 | 9.6 |
| SnapMerge w/o bounded reads | 12.84 | 12.84 | 9.01 | 9.95 | 9.87 |
| SnapMerge w/o compaction rebase | 16.42 | 16.42 | 14.86 | 16.49 | 15.36 |
| SnapMerge w/o delete union | 17.33 | 17.33 | 15.03 | 14.41 | 15.83 |
| SnapMerge w/o net-effect test | 19.18 | 19.18 | 11.45 | 14.22 | 13.54 |

SnapMerge phase medians (ms): {"rejection": {"lineage": {"n": 1662, "median_ms": 8.66}, "logical": {"n": 1662, "median_ms": 6.96}}, "merge": {"compose": {"n": 1341, "median_ms": 0.04}, "emit": {"n": 1341, "median_ms": 5.96}, "lineage": {"n": 1341, "median_ms": 7.08}, "logical": {"n": 1341, "median_ms": 0.04}, "reconcile": {"n": 1341, "median_ms": 0.0}}}

## 24 (nyc). SnapMerge conflict categories and witnesses

Categories {'logical-conflict': 1745}; logical witness kinds {'net-effect': 1745}; rules {'R2': 1136, 'R5': 353, 'R4': 256}; order difference (A;C)(v)-(C;A)(v) of the witness value {'-1': 1221, '1': 524} (non-zero in 1745).

## 25 (nyc). Latency by outcome, SnapMerge phases, order position

| Policy | Sound merge ms | Any merge ms | Rejection ms | Ran first ms | Ran later ms |
|---|---|---|---|---|---|
| Iterated cherry-pick | 14.48 | 13.43 | 7.07 | 7.73 | 7.71 |
| Fast-forward only |  |  | 6.77 | 6.75 | 6.77 |
| lakeFS policy | 7.02 | 7.02 | 6.9 | 6.94 | 6.93 |
| Nessie FORCE | 7.36 | 9.43 |  | 9.31 | 9.43 |
| Nessie NORMAL |  |  | 6.74 | 6.7 | 6.75 |
| Rebase, one-direction validation (OCC) | 15.12 | 14.78 | 8.74 | 10.32 | 10.12 |
| Recompute both orders (reference) | 30.99 | 30.99 | 25.4 | 26.98 | 27.97 |
| Replay (#13959) | 14.41 | 13.64 | 7.09 | 8.63 | 8.16 |
| Replay + recorded predicates (pairwise) | 15.85 | 15.85 | 8.71 | 9.84 | 9.67 |
| Replay + exact net-effect test | 15.59 | 15.59 | 11.09 | 12.12 | 12.13 |
| Row-level 3-way | 27.72 | 28.89 | 22.89 | 28.08 | 27.94 |
| SnapMerge | 14.74 | 14.74 | 13.32 | 13.69 | 13.95 |
| SnapMerge, submitted insert rule | 14.84 | 14.84 | 13.28 | 12.91 | 13.91 |
| SnapMerge file-level rules only | 13.65 | 13.65 | 7.35 | 8.05 | 7.97 |
| SnapMerge w/o bounded reads | 10.12 | 10.12 | 7.75 | 7.88 | 8.24 |
| SnapMerge w/o compaction rebase | 13.08 | 13.08 | 11.81 | 12.41 | 12.29 |
| SnapMerge w/o delete union | 13.13 | 13.13 | 12.04 | 13.52 | 12.32 |
| SnapMerge w/o net-effect test | 14.39 | 14.39 | 9.57 | 10.48 | 10.9 |

SnapMerge phase medians (ms): {"rejection": {"lineage": {"n": 1745, "median_ms": 7.43}, "logical": {"n": 1745, "median_ms": 4.79}}, "merge": {"compose": {"n": 1258, "median_ms": 0.03}, "emit": {"n": 1258, "median_ms": 4.58}, "lineage": {"n": 1258, "median_ms": 5.88}, "logical": {"n": 1258, "median_ms": 0.07}, "reconcile": {"n": 1258, "median_ms": 0.0}}}

## 26. Exhaustive physical-state enumeration (`physical_enum_v3`)

{"cases": 1568, "bases": 8, "menu": 14, "true_conflicts": 160, "merged": 1408, "merged_sound": 1408, "positions_match_formula": 1408, "bad_delete_files": 0, "merged_on_true_conflict": 0, "rejected_true_conflict": 160, "category:logical-conflict": 160, "unsound_bound": {"k": 0, "n": 1408, "level": 0.95, "upper_one_sided": 0.00213, "lower_one_sided": 0.0, "two_sided_95": [0.0, 0.00262]}}

## 27. One-shot workflow trace and merge races (`workflow_v3`)

| Step | Category | Rule | Re-sequenced | Check | Target unchanged |
|---|---|---|---|---|---|
| create branch fix pinned at the base snapshot |  |  |  |  |  |
| record a COW correction and a MOR upsert on fix |  |  |  |  |  |
| main ingests into another partition and compacts the corrected partition |  |  |  |  |  |
| merge fix into main while main receives a concurrent commit | retryable | target-moved | 0 |  |  |
| re-plan against the moved target and retry | merged |  | 0 | {'eq_ac': True, 'eq_ca': True, 'readers_agree': True, 'true_conflict': False} |  |
| recreate fix from the merge snapshot, edit, merge again | merged |  | 0 | {'eq_ac': True, 'eq_ca': True, 'readers_agree': True, 'true_conflict': False} |  |
| a true conflict is reported with a net-effect witness | logical-conflict | R2 | 0 |  |  |
| merge after history was removed fails closed | unsupported | missing-history | 0 |  | True |

Races: 210 scenarios, 97 with the injected commit fired; by target {"other": {"fired": 32, "first_attempt": {"merged": 32}, "final": {"merged": 32}, "attempts_max": 1}, "source": {"fired": 30, "first_attempt": {"merged": 30}, "final": {"merged": 30}, "attempts_max": 1}, "target": {"fired": 35, "first_attempt": {"retryable": 35}, "final": {"merged": 31, "logical-conflict": 4}, "attempts_max": 2}}; checked merges 93 (both orders 93, readers agree 93); re-sequenced counts {1: 62, 0: 31}.

## 28. Contended-fraction scaling (isolated processes)

| Campaign | Size | Kind | Policy | Reps | Verdicts | Median ms | Untimed ms | Bytes read | Estimate bytes | Estimate rows | Peak RSS GB | RSS incr GB | Temp bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| contended_v3 | tpch:1 | contended-1 | replay | 3 | {'conflict': 3} | 14.244 |  | 0.0 |  |  | 0.184 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-1 | snapmerge | 3 | {'merged': 3} | 156.948 | 0.284 | 4263635.0 | 4263635.0 | 160407.0 | 0.265 | 0.082 | 0.0 |
| contended_v3 | tpch:1 | contended-2 | replay | 3 | {'conflict': 3} | 14.247 |  | 0.0 |  |  | 0.186 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-2 | snapmerge | 3 | {'merged': 3} | 262.46 | 0.352 | 8007961.0 | 8007961.0 | 300352.0 | 0.3 | 0.115 | 0.0 |
| contended_v3 | tpch:1 | contended-4 | replay | 3 | {'conflict': 3} | 14.442 |  | 0.0 |  |  | 0.186 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-4 | snapmerge | 3 | {'merged': 3} | 483.579 | 0.506 | 15562150.0 | 15562150.0 | 582936.0 | 0.334 | 0.148 | 0.0 |
| contended_v3 | tpch:1 | contended-8 | replay | 3 | {'conflict': 3} | 16.614 |  | 0.0 |  |  | 0.193 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-8 | snapmerge | 3 | {'merged': 3} | 922.408 | 0.797 | 30128787.0 | 30128787.0 | 1127670.0 | 0.343 | 0.152 | 0.0 |
| contended_v3 | tpch:1 | contended-16 | replay | 3 | {'conflict': 3} | 15.492 |  | 0.0 |  |  | 0.198 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-16 | snapmerge | 3 | {'merged': 3} | 1860.669 | 1.48 | 62355229.0 | 62355229.0 | 2334242.0 | 0.359 | 0.171 | 0.0 |
| contended_v3 | tpch:1 | contended-32 | replay | 3 | {'conflict': 3} | 17.015 |  | 0.0 |  |  | 0.201 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-32 | snapmerge | 3 | {'merged': 3} | 3612.653 | 3.182 | 124385742.0 | 124385742.0 | 4654418.0 | 0.422 | 0.222 | 0.0 |
| contended_v3 | tpch:1 | contended-64 | replay | 3 | {'conflict': 3} | 19.767 |  | 0.0 |  |  | 0.217 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-64 | snapmerge | 3 | {'merged': 3} | 7300.191 | 7.935 | 249228376.0 | 249228376.0 | 9322880.0 | 0.505 | 0.287 | 0.0 |
| contended_v3 | tpch:1 | contended-all | replay | 3 | {'conflict': 3} | 20.977 |  | 0.0 |  |  | 0.221 | 0.0 | 0.0 |
| contended_v3 | tpch:1 | contended-all | snapmerge | 3 | {'merged': 3} | 9397.867 | 11.949 | 321083937.0 | 321083937.0 | 12008370.0 | 0.5 | 0.282 | 0.0 |
| contended_unpart_v3 | tpch-unpart:1 | overlap | recompute | 3 | {'merged': 3} | 8333.163 |  | 122249136.0 |  |  | 5.286 | 5.067 | 0.0 |
| contended_unpart_v3 | tpch-unpart:1 | overlap | replay | 3 | {'conflict': 3} | 4.476 |  | 0.0 |  |  | 0.22 | 0.0 | 0.0 |
| contended_unpart_v3 | tpch-unpart:1 | overlap | snapmerge | 3 | {'merged': 3} | 11258.249 | 0.218 | 244396387.0 | 244396387.0 | 12008431.0 | 7.126 | 6.907 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-1 | replay | 3 | {'conflict': 3} | 115.87 |  | 0.0 |  |  | 0.227 | 0.008 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-1 | snapmerge | 3 | {'merged': 3} | 1454.097 | 2.533 | 41006848.0 | 41006848.0 | 1561982.0 | 1.163 | 0.945 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-4 | replay | 3 | {'conflict': 3} | 108.811 |  | 0.0 |  |  | 0.225 | 0.008 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-4 | snapmerge | 3 | {'merged': 3} | 5491.431 | 9.702 | 159405486.0 | 159405486.0 | 6050422.0 | 1.706 | 1.488 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-16 | replay | 3 | {'conflict': 3} | 118.496 |  | 0.0 |  |  | 0.235 | 0.009 | 0.0 |
| contended_sf10_v3 | tpch:10 | contended-16 | snapmerge | 3 | {'merged': 3} | 20354.687 | 44.63 | 591173347.0 | 591173347.0 | 22344504.0 | 1.91 | 1.691 | 0.0 |

Untimed ms: median latency outside the timed phases (lineage, logical, compose, reconcile, emit); for SnapMerge essentially the preflight read estimate, which evaluates every insert-role file's bounds against the other side's predicates before the logical phase evaluates them again.

## 29. History-length scaling

| Campaign | Size | Kind | Policy | Reps | Verdicts | Median ms | Untimed ms | Bytes read | Estimate bytes | Estimate rows | Peak RSS GB | RSS incr GB | Temp bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| history_v3 | tpch:1 | history-1 | replay | 3 | {'merged': 3} | 15.871 |  | 0.0 |  |  | 0.18 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-1 | replay_intent | 3 | {'merged': 3} | 15.595 |  | 0.0 |  |  | 0.18 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-1 | snapmerge | 3 | {'merged': 3} | 14.278 | 0.188 | 0.0 | 0.0 | 0.0 | 0.179 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-4 | replay | 3 | {'merged': 3} | 37.177 |  | 0.0 |  |  | 0.18 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-4 | replay_intent | 3 | {'merged': 3} | 39.363 |  | 0.0 |  |  | 0.18 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-4 | snapmerge | 3 | {'merged': 3} | 24.417 | 1.467 | 0.0 | 0.0 | 0.0 | 0.18 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-16 | replay | 3 | {'merged': 3} | 214.084 |  | 0.0 |  |  | 0.183 | 0.002 | 0.0 |
| history_v3 | tpch:1 | history-16 | replay_intent | 3 | {'merged': 3} | 221.539 |  | 0.0 |  |  | 0.183 | 0.002 | 0.0 |
| history_v3 | tpch:1 | history-16 | snapmerge | 3 | {'merged': 3} | 74.107 | 14.118 | 0.0 | 0.0 | 0.0 | 0.182 | 0.0 | 0.0 |
| history_v3 | tpch:1 | history-64 | replay | 3 | {'merged': 3} | 1582.481 |  | 0.0 |  |  | 0.193 | 0.006 | 0.0 |
| history_v3 | tpch:1 | history-64 | replay_intent | 3 | {'merged': 3} | 1889.089 |  | 0.0 |  |  | 0.195 | 0.009 | 0.0 |
| history_v3 | tpch:1 | history-64 | snapmerge | 3 | {'merged': 3} | 567.097 | 229.434 | 0.0 | 0.0 | 0.0 | 0.187 | 0.001 | 0.0 |

Untimed ms: median latency outside the timed phases (lineage, logical, compose, reconcile, emit); for SnapMerge essentially the preflight read estimate, which evaluates every insert-role file's bounds against the other side's predicates before the logical phase evaluates them again.

Preflight estimate vs actual SnapMerge data/delete bytes read: {'points': 15, 'actual_over_estimate_median': 1.0, 'actual_over_estimate_range': [1.0, 1.0], 'log_log_correlation': 1.0}

## 30. Regression against superseded campaigns (same seeds; verdict and serial equivalence per policy)

- difftest_main_tpch_v5 vs difftest_main_tpch_v4: 3003 scenarios; identical by policy {'cherry_pick': 3003, 'fast_forward': 3003, 'lakefs': 3003, 'nessie_force': 3003, 'nessie_normal': 3003, 'rebase_occ': 3003, 'recompute': 3003, 'replay': 3003, 'replay_intent': 3003, 'replay_intent_exact': 3003, 'rowlevel': 3003, 'snapmerge': 3003, 'snapmerge[append_rule]': 3003, 'snapmerge[filelevel]': 3003, 'snapmerge[no_bounded_reads]': 3003, 'snapmerge[no_compaction_rebase]': 3003, 'snapmerge[no_delete_union]': 3003, 'snapmerge[no_net_effect]': 3003}; 0 differences; examples []
- difftest_main_tpch_v5 vs difftest_main_tpch_v3: 3003 scenarios; identical by policy {'cherry_pick': 3003, 'fast_forward': 3003, 'lakefs': 3003, 'nessie_force': 3003, 'nessie_normal': 3003, 'rebase_occ': 3003, 'recompute': 3003, 'replay': 3003, 'replay_intent': 3003, 'replay_intent_exact': 3003, 'rowlevel': 3003, 'snapmerge': 3003, 'snapmerge[append_rule]': 3003, 'snapmerge[filelevel]': 3003, 'snapmerge[no_bounded_reads]': 3003, 'snapmerge[no_compaction_rebase]': 3003, 'snapmerge[no_delete_union]': 3003, 'snapmerge[no_net_effect]': 3003}; 0 differences; examples []
- difftest_main_tpch_v5 vs difftest_main_tpch_v2: 3003 scenarios; identical by policy {'cherry_pick': 3003, 'fast_forward': 3003, 'lakefs': 3003, 'nessie_force': 3003, 'nessie_normal': 3003, 'replay': 3003, 'rowlevel': 3003, 'snapmerge': 3003, 'snapmerge[filelevel]': 3003, 'snapmerge[no_bounded_reads]': 3003, 'snapmerge[no_compaction_rebase]': 3003, 'snapmerge[no_delete_union]': 3003, 'snapmerge[no_net_effect]': 3003}; 0 differences; examples []
- difftest_main_nyc_v5 vs difftest_main_nyc_v4: 3003 scenarios; identical by policy {'cherry_pick': 3003, 'fast_forward': 3003, 'lakefs': 3003, 'nessie_force': 3003, 'nessie_normal': 3003, 'rebase_occ': 3003, 'recompute': 3003, 'replay': 3003, 'replay_intent': 3003, 'replay_intent_exact': 3003, 'rowlevel': 3003, 'snapmerge': 3003, 'snapmerge[append_rule]': 3003, 'snapmerge[filelevel]': 3003, 'snapmerge[no_bounded_reads]': 3003, 'snapmerge[no_compaction_rebase]': 3003, 'snapmerge[no_delete_union]': 3003, 'snapmerge[no_net_effect]': 3003}; 0 differences; examples []
- difftest_main_nyc_v5 vs difftest_main_nyc_v3: 3003 scenarios; identical by policy {'cherry_pick': 3003, 'fast_forward': 3003, 'lakefs': 3003, 'nessie_force': 3003, 'nessie_normal': 3003, 'rebase_occ': 3003, 'recompute': 3003, 'replay': 3003, 'replay_intent': 3003, 'replay_intent_exact': 3003, 'rowlevel': 3003, 'snapmerge': 3003, 'snapmerge[append_rule]': 3003, 'snapmerge[filelevel]': 3003, 'snapmerge[no_bounded_reads]': 3003, 'snapmerge[no_compaction_rebase]': 3003, 'snapmerge[no_delete_union]': 3003, 'snapmerge[no_net_effect]': 3003}; 0 differences; examples []
- writer_single_v3 vs writer_single_v2: 1001 scenarios; identical by policy {'cherry_pick': 1001, 'rebase_occ': 1001, 'recompute': 1001, 'replay': 1001, 'replay_intent': 1001, 'replay_intent_exact': 1001, 'rowlevel': 1001, 'snapmerge': 1001, 'snapmerge[append_rule]': 1001, 'snapmerge[no_net_effect]': 1001}; 0 differences; examples []
- writer_single_v3 vs writer_single_v1: 1001 scenarios; identical by policy {'cherry_pick': 1001, 'rebase_occ': 1001, 'recompute': 1001, 'replay': 1001, 'replay_intent': 1001, 'replay_intent_exact': 1001, 'rowlevel': 1001, 'snapmerge': 1001, 'snapmerge[append_rule]': 1001, 'snapmerge[no_net_effect]': 1001}; 0 differences; examples []
- ablation_v5 vs ablation_v4: 1505 scenarios; identical by policy {'replay': 1505, 'replay_intent': 1505, 'replay_intent_exact': 1505, 'snapmerge': 1505, 'snapmerge[no_bounded_reads]': 1505, 'snapmerge[no_compaction_rebase]': 1505, 'snapmerge[no_delete_union]': 1505, 'snapmerge[no_net_effect]': 1505}; 0 differences; examples []
- ablation_v5 vs ablation_v3: 1505 scenarios; identical by policy {'replay': 1505, 'replay_intent': 1505, 'replay_intent_exact': 1505, 'snapmerge': 1505, 'snapmerge[no_bounded_reads]': 1505, 'snapmerge[no_compaction_rebase]': 1505, 'snapmerge[no_delete_union]': 1505, 'snapmerge[no_net_effect]': 1505}; 0 differences; examples []
- fallback_filelevel_v5 vs fallback_filelevel_v4: 1001 scenarios; identical by policy {'cherry_pick': 1001, 'fast_forward': 1001, 'lakefs': 1001, 'nessie_force': 1001, 'nessie_normal': 1001, 'replay': 1001, 'rowlevel': 1001, 'snapmerge': 1001, 'snapmerge[filelevel]': 1001}; 0 differences; examples []
- fallback_filelevel_v5 vs fallback_filelevel_v3: 1001 scenarios; identical by policy {'cherry_pick': 1001, 'fast_forward': 1001, 'lakefs': 1001, 'nessie_force': 1001, 'nessie_normal': 1001, 'replay': 1001, 'rowlevel': 1001, 'snapmerge': 1001, 'snapmerge[filelevel]': 1001}; 0 differences; examples []
- mixed_metadata_v3 vs mixed_metadata_v2: 1001 scenarios; identical by policy {'replay': 1001, 'replay_intent': 1001, 'snapmerge': 1001}; 0 differences; examples []
- mixed_metadata_v3 vs mixed_metadata_v1: 1001 scenarios; identical by policy {'replay': 1001, 'replay_intent': 1001, 'snapmerge': 1001}; 0 differences; examples []
- sens_parts8_v3 vs sens_parts8_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_parts8_v3 vs sens_parts8_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_parts16_v3 vs sens_parts16_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_parts16_v3 vs sens_parts16_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_parts32_v3 vs sens_parts32_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_parts32_v3 vs sens_parts32_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_hist2_v3 vs sens_hist2_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_hist2_v3 vs sens_hist2_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_hist4_v3 vs sens_hist4_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_hist4_v3 vs sens_hist4_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_sel025_v3 vs sens_sel025_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_sel025_v3 vs sens_sel025_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_sel4_v3 vs sens_sel4_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_sel4_v3 vs sens_sel4_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_compact05_v3 vs sens_compact05_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_compact05_v3 vs sens_compact05_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_compact1_v3 vs sens_compact1_v2: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []
- sens_compact1_v3 vs sens_compact1_v1: 420 scenarios; identical by policy {'rebase_occ': 420, 'replay': 420, 'replay_intent': 420, 'replay_intent_exact': 420, 'snapmerge': 420, 'snapmerge[no_net_effect]': 420}; 0 differences; examples []

## 31. Cost campaigns: timed runs, merges and oracle-checked merges

| Campaign | Timed runs | Distinct scenarios | Merges | Checked merges | Checked = both orders | By policy (runs/merges/checked) | Guard GB | DuckDB MB | Batch scale | Runs by invocation |
|---|---|---|---|---|---|---|---|---|---|---|
| scale_v6_tpch | 156 | 60 | 126 | 42 | 42 | recompute 18/18/12; replay 60/30/6; rowlevel 18/18/12; snapmerge 60/60/12 | 80 | 8192 | 1.0 | minor_ALL_inv1 156 |
| contended_v3 | 48 | 24 | 24 | 8 | 8 | replay 24/0/0; snapmerge 24/24/8 | 80 | 8192 | 1.0 | minor_ALL_inv1 48 |
| contended_unpart_v3 | 9 | 3 | 6 | 2 | 2 | recompute 3/3/1; replay 3/0/0; snapmerge 3/3/1 | 80 | 8192 | 1.0 | minor_ALL_inv1 9 |
| contended_sf10_v3 | 18 | 9 | 9 | 3 | 3 | replay 9/0/0; snapmerge 9/9/3 | 80 | 8192 | 1.0 | minor_ALL_inv1 18 |
| history_v3 | 36 | 12 | 36 | 12 | 12 | replay 12/12/4; replay_intent 12/12/4; snapmerge 12/12/4 | 80 | 8192 | 1.0 | minor_ALL_inv1 36 |

Guard invocations of `minor_ALL` (current campaigns): 1; tree limits [80.0] GB, host limit [490.0] GB; stops: none.

Guard invocations of `rereview_ALL` (superseded re-review campaigns): 5; tree limits [48.0, 80.0] GB, host limit [490.0] GB; stops: rereview_ALL_inv1: project_rss_limit (tree 48.3 GB, host 272.7 GB, batch scale 1.0, step history); rereview_ALL_inv1: interrupted (tree 24.1 GB, host 253.7 GB, batch scale 0.5, step history); rereview_ALL_inv2: host_used_limit (tree 44.5 GB, host 491.5 GB, batch scale 1.0, step contended_sf10); rereview_ALL_inv3: host_used_limit (tree 22.0 GB, host 492.7 GB, batch scale 1.0, step contended_sf10).

Each repetition is a different seeded scenario; only the first --check-reps repetitions of each merging cell are oracle-checked. A run is listed iff its record was written atomically by a completed child process; a guard stop leaves no record for the interrupted run, which the next invocation repeats with the same settings. Per-run manifest (settings, absolute RSS before and at peak, increment, spill, check outcome): results/coverage_cost_campaigns.json.

## 32. Preflight read estimate vs bytes actually read, per phase (all SnapMerge decisions, differential campaigns)

63133 decisions; phase-bound violations (actual > estimate): 0; actual bytes by phase {'logical': 1020404321, 'compose': 1393265247, 'reconcile': 14044989, 'recovery': 200700044}; estimated {'compose': 7159533987, 'logical': 1508531205, 'recovery': 224848702, 'reconcile': 23951289}; examples [].

Validator (re-review G1): 0 unsupported outcomes in 63133 SnapMerge decisions of the differential campaigns; identity-guard rejections 0, phase-order rejections 0; by reason {}.

## 33. Matched histories, split vs single-snapshot (mixed) encoding: SnapMerge reads by phase

1001 matched scenarios (370 with mixed snapshots); same verdict in 1001; mean total bytes split 64979.7 vs mixed 164393.6; median latency ms {'split': 18.59, 'mixed': 19.62}; geometric-mean latency ratio mixed/split 1.122; mean metadata bytes read {'split': 161683.0, 'mixed': 128590.8}; median phase timings (ms) {'split': {'compose': 0.045, 'emit': 5.97, 'lineage': 7.89, 'logical': 2.734, 'reconcile': 0.001}, 'mixed': {'compose': 0.045, 'emit': 5.572, 'lineage': 5.955, 'logical': 4.538, 'reconcile': 0.001}}.
| Phase | Split mean B | Split share > 0 | Mixed mean B | Mixed share > 0 | Mixed p90 B | Mixed max B |
|---|---|---|---|---|---|---|
| compose | 38857.1 | 0.2118 | 33092.9 | 0.2118 | 102745.0 | 451392.0 |
| logical | 25789.5 | 0.6264 | 18667.8 | 0.5764 | 54115.0 | 180851.0 |
| reconcile | 333.2 | 0.002 | 336.9 | 0.002 | 0.0 | 171884.0 |
| recovery | 0.0 | 0.0 | 112296.0 | 0.3696 | 389048.0 | 1597807.0 |

## 34. Recording overhead vs harness validation cost

| Writer | Scenarios | snapmerge.* B / snapshot (median, max) | Share of metadata file | Metadata file B | Snapshots / scenario | Writer ms / op | Provenance check ms / op | Writer check ms / scenario |
|---|---|---|---|---|---|---|---|---|
| recorded (main) | 3003 | 394.0, 1041.0 | 0.1749 | 11886.0 | 5.0 | 24.8 | 13.81 | 84.7 |
| unrecorded (fallback) | 1001 | 0.0, 10.0 | 0.0 | 9166.0 | 5.0 | 21.5 |  | 75.0 |
