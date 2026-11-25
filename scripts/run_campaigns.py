#!/usr/bin/env python3
"""Re-run the 23 reference campaigns in a separate, checkpointed output tree."""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
V7 = "full,no_delete_union,no_compaction_rebase,no_bounded_reads,no_net_effect,filelevel,append_rule"
B11 = "replay,lakefs,nessie_normal,nessie_force,fast_forward,cherry_pick,rowlevel,replay_intent,replay_intent_exact,rebase_occ,recompute"
DT = ["difftest.py", "--source", "tpch_lineitem", "--sf", "0.1", "--base-rows", "2000"]
SC = ["scale.py", "--isolate", "--warm", "--order", "random", "--check-threads", "12"]
SENS = ["--per-mix", "60", "--variants", "full,no_net_effect", "--policies",
        "replay,replay_intent,replay_intent_exact,rebase_occ"]


def campaigns():
    commands = {
        "difftest_main_tpch_v5": DT + ["--per-mix", "429", "--variants", V7, "--policies", B11, "--provenance-check"],
        "writer_single_v3": DT + ["--per-mix", "143", "--writer", "single", "--variants", "full,no_net_effect,append_rule",
            "--policies", "replay,cherry_pick,rowlevel,replay_intent,replay_intent_exact,rebase_occ,recompute", "--provenance-check"],
        "physical_enum_v3": ["scripts/physical_enum.py"],
        "workflow_v3": ["scripts/workflow.py", "--per-mix", "30"],
        "counterexamples_v4": ["scripts/minimize_counterexamples.py"],
    }
    for name, option, value in (
        ("parts8", "--n-parts", "8"), ("parts16", "--n-parts", "16"), ("parts32", "--n-parts", "32"),
        ("hist2", "--hist", "2"), ("hist4", "--hist", "4"), ("sel025", "--sel", "0.25"),
        ("sel4", "--sel", "4"), ("compact05", "--compact-p", "0.5"), ("compact1", "--compact-p", "1.0"),
    ):
        commands[f"sens_{name}_v3"] = DT + SENS + [option, value]
    commands.update({
        "ablation_v5": DT + ["--seed-base", "20261003", "--per-mix", "215", "--variants",
            "full,no_delete_union,no_compaction_rebase,no_bounded_reads,no_net_effect",
            "--policies", "replay,replay_intent,replay_intent_exact"],
        "fallback_filelevel_v5": DT + ["--seed-base", "20261004", "--per-mix", "143", "--no-record",
            "--variants", "full,filelevel", "--policies", "replay,lakefs,nessie_normal,nessie_force,fast_forward,cherry_pick,rowlevel"],
        "mixed_metadata_v3": DT + ["--seed-base", "20261004", "--per-mix", "143", "--record-sides", "A",
            "--variants", "full", "--policies", "replay,replay_intent"],
        "scale_v6_tpch": SC + ["--sizes", "tpch:0.1,tpch:1,tpch:10", "--kinds", "disjoint,overlap",
            "--policies", "snapmerge,replay,rowlevel,recompute", "--reps", "10", "--reps-policy", "rowlevel=3,recompute=3", "--check-reps", "2"],
        "contended_v3": SC + ["--sizes", "tpch:1", "--kinds",
            "contended-1,contended-2,contended-4,contended-8,contended-16,contended-32,contended-64,contended-all",
            "--policies", "snapmerge,replay", "--reps", "3", "--check-reps", "1"],
        "contended_unpart_v3": SC + ["--sizes", "tpch-unpart:1", "--kinds", "overlap", "--policies",
            "snapmerge,replay,recompute", "--reps", "3", "--check-reps", "1"],
        "history_v3": SC + ["--sizes", "tpch:1", "--kinds", "history-1,history-4,history-16,history-64",
            "--policies", "snapmerge,replay,replay_intent", "--reps", "3", "--check-reps", "1"],
        "difftest_main_nyc_v5": ["difftest.py", "--source", "nyc_yellow", "--months", "2023-01", "--base-rows", "2000",
            "--per-mix", "429", "--variants", V7, "--policies", B11],
        "contended_sf10_v3": SC + ["--sizes", "tpch:10", "--kinds", "contended-1,contended-4,contended-16",
            "--policies", "snapmerge,replay", "--reps", "3", "--check-reps", "1"],
    })
    return commands


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", action="append", choices=list(campaigns()), help="campaign to run; repeat to select several")
    parser.add_argument("--results-root", type=Path, default=ROOT / "results" / "reproduced")
    parser.add_argument("--work-root", type=Path, default=ROOT / "work" / "reproduced")
    parser.add_argument("--list", action="store_true", help="print commands without running them")
    args = parser.parse_args()
    results = args.results_root.resolve()
    if results == (ROOT / "results").resolve():
        parser.error("choose a separate results directory to preserve the reference records")
    for name, command in campaigns().items():
        if args.only and name not in args.only:
            continue
        command = [sys.executable] + command + ["--campaign", name, "--results-root", str(results),
                   "--work-root", str(args.work_root.resolve())]
        print(shlex.join(command), flush=True)
        if not args.list:
            subprocess.run(command, cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
