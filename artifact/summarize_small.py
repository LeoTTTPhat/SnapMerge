#!/usr/bin/env python3
"""Contract-normalized outcomes and information-matched attribution for a small difftest campaign.

Usage: python artifact/summarize_small.py results/<campaign>
Reads the raw per-scenario records directly (the full analysis, with the fingerprint gate, is analyze.py).
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import analysis_revision as R  # noqa: E402


def main() -> int:
    rdir = Path(sys.argv[1])
    recs = [json.loads(p.read_text()) for p in sorted((rdir / "raw").glob("*.json"))]
    tc = sum(r["oracle"]["true_conflict"] for r in recs)
    print(f"{len(recs)} scenarios, {tc} true conflicts, {len(recs) - tc} commuting")
    ct = R.contract_table(recs)
    print(f"\n{'policy':28s} {'sound':>6s} {'rejTC':>6s} {'AC-only':>8s} {'CA-only':>8s} {'neither':>8s} {'falseC':>7s}")
    for p, v in sorted(ct.items()):
        t = v["two_order"]
        c = v["counts"]
        print(f"{p:28s} {t['sound_merges']:6d} {t['reported_conflicts']:6d} {c.get('true_conflict:merged_ac_only', 0):8d} "
              f"{c.get('true_conflict:merged_ca_only', 0):8d} {t['neither_order']:8d} {t['false_conflicts']:7d}")
    m = R.paired_rates(recs, R.CONTROL, R.EFFECTS, 2000, 7, groups=False)["all"]
    print("\nSound auto-merge rate of commuting scenarios [95% bootstrap]:")
    for p, e in m["rates"].items():
        print(f"  {p:28s} {R._ci(e)}")
    print("\nPaired differences:")
    for name, e in m["differences"].items():
        print(f"  {name:80s} {R._ci(e)}")
    prov = R.provenance_stats(recs)
    if prov:
        print(f"\nProvenance checks: {prov['operations_checked']} operations, {prov['operations_with_violations']} with violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
