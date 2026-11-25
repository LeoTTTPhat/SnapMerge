"""Adversarial review (stage Review, 2026-10-02): the project's own scenario generator on a synthetic
timestamp / day-partitioned source shaped like NYC TLC (the real NYC data are not downloaded yet),
with adversarial values the real data contain: negative fares/tips (refunds), values close to zero
so that the generator's correction round(v * f + 0.01, 2) can produce -0.0, and duplicated trips.

Hard properties for SnapMerge and its variants: no soundness violation, no missed true conflict,
no exception, readers agree, writer check passes.  False conflicts are counted and printed
(a -0.0 / 0.0 mismatch in exact row matching would show up as a false conflict, not as a
soundness violation).
"""
import os
import unittest

import numpy as np
import pyarrow as pa

from helpers import nyc_like
from snapmerge.scenarios import MIXES, SourcePool
from snapmerge.sources import NYC
from test_difftest import run_many


def adversarial_nyc(n=6000, seed=11) -> pa.Table:
    t = nyc_like(n, seed)
    rng = np.random.default_rng(seed)
    fare = t.column("fare_amount").to_numpy().copy()
    tip = t.column("tip_amount").to_numpy().copy()
    idx = rng.choice(n, size=n // 10, replace=False)
    fare[idx[: n // 20]] = -np.round(rng.uniform(0.0, 0.02, n // 20), 2)  # -0.0, -0.01, -0.02
    tip[idx[n // 20:]] = -0.01
    t = t.set_column(t.schema.get_field_index("fare_amount"), "fare_amount", pa.array(fare))
    t = t.set_column(t.schema.get_field_index("tip_amount"), "tip_amount", pa.array(tip))
    # duplicated trips (identical except rid)
    dup = t.slice(0, 200)
    dup = dup.set_column(0, "rid", pa.array(np.arange(200, dtype=np.int64) + 3_000_000_000))
    return pa.concat_tables([t, dup]).sort_by("rid")


class TimestampSource(unittest.TestCase):
    def test_generator_on_adversarial_nyc_like_source(self):
        per_mix = int(os.environ.get("ADV_NYC_PER_MIX", "4"))
        pool = SourcePool(NYC, adversarial_nyc())
        pols = ["snapmerge", "snapmerge[no_net_effect]", "snapmerge[no_bounded_reads]", "replay"]
        recs = run_many(pool, "nyc_yellow", NYC.partition, per_mix, 31337, 600, pols)
        self.assertEqual(len(recs), per_mix * len(MIXES))
        bad, fp = [], 0
        for r in recs:
            sid = r["scenario"]["scenario_id"]
            tc = r["oracle"]["true_conflict"]
            for side in ("A", "C"):
                if not (r["writer_check"][side]["ok"] and r["writer_check"][side]["readers_agree"]):
                    bad.append((sid, "writer", side))
            for p in pols[:3]:
                x = r["policies"][p]
                if x["verdict"] == "error":
                    bad.append((sid, p, x.get("error")))
                elif x["verdict"] == "merged":
                    if not (x["readers_agree"] and x["eq_ac"] and x["eq_ca"]):
                        bad.append((sid, p, "unsound"))
                elif p == "snapmerge" and not tc:
                    fp += 1
            if tc and r["policies"]["snapmerge"]["verdict"] != "conflict":
                bad.append((sid, "snapmerge", "missed conflict"))
        print(f"\n[timestamp source] scenarios={len(recs)} true_conflicts={sum(r['oracle']['true_conflict'] for r in recs)}"
              f" snapmerge_false_conflicts={fp}")
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main()
