import datetime as dt
import unittest

import duckdb
import numpy as np
import pyarrow as pa

from helpers import C, Env, X, rows


def random_table(n=400, seed=0):
    rng = np.random.default_rng(seed)
    return pa.table({
        "rid": pa.array(np.arange(n, dtype=np.int64)),
        "i": pa.array(rng.integers(-5, 20, n).astype(np.int64)),
        "f": pa.array(np.round(rng.uniform(0, 1, n), 2)),
        "d": pa.array([dt.date(2024, 1, 1) + dt.timedelta(days=int(x)) for x in rng.integers(0, 90, n)], pa.date32()),
        "t": pa.array([dt.datetime(2023, 1, 1) + dt.timedelta(seconds=int(x)) for x in rng.integers(0, 10**6, n)],
                      pa.timestamp("us")),
        "s": pa.array([f"v{x}" for x in rng.integers(0, 6, n)]),
    })


def random_filter(rng, t: pa.Table, depth=0):
    r = rng.random()
    if depth < 2 and r < 0.3:
        k = int(rng.integers(2, 4))
        args = [random_filter(rng, t, depth + 1) for _ in range(k)]
        return X.and_(*args) if rng.random() < 0.5 else X.or_(*args)
    if depth < 2 and r < 0.4:
        return X.not_(random_filter(rng, t, depth + 1))
    col = str(rng.choice(["i", "f", "d", "t", "s"]))
    vals = t.column(col).to_pylist()
    v = vals[int(rng.integers(len(vals)))]
    if rng.random() < 0.2:
        return X.isin(col, [vals[int(rng.integers(len(vals)))] for _ in range(3)])
    return X.cmp(col, str(rng.choice(["<", "<=", ">", ">=", "=", "!="])), v)


class ExprBackends(unittest.TestCase):
    def test_arrow_and_sql_agree_on_random_filters(self):
        t = random_table()
        con = duckdb.connect()
        con.register("t", t)
        rng = np.random.default_rng(42)
        for _ in range(300):
            f = random_filter(rng, t)
            mask = X.eval_arrow(f, t).to_numpy(zero_copy_only=False)
            arrow_ids = set(t.column("rid").to_numpy()[mask].tolist())
            sql_ids = {r[0] for r in con.execute(f"SELECT rid FROM t WHERE {X.to_sql(f, t.schema)}").fetchall()}
            self.assertEqual(arrow_ids, sql_ids, X.describe(f))
            self.assertEqual(X.loads(X.dumps(f)), f)

    def test_iceberg_backend_matches_arrow_through_a_scan(self):
        t = random_table(300, seed=5)
        env = Env(base=t, partition=("d", "month"))
        try:
            rng = np.random.default_rng(7)
            sid = env.base_id
            for _ in range(25):
                f = random_filter(rng, t)
                got = env.table.scan(snapshot_id=sid, row_filter=X.to_iceberg(f, t.schema)).to_arrow()
                mask = X.eval_arrow(f, t).to_numpy(zero_copy_only=False)
                self.assertEqual(sorted(got.column("rid").to_pylist()),
                                 sorted(t.column("rid").to_numpy()[mask].tolist()), X.describe(f))
        finally:
            env.close()

    def test_constructors_simplify(self):
        self.assertEqual(X.and_(X.TRUE, X.cmp("a", "=", 1)), X.cmp("a", "=", 1))
        self.assertTrue(X.is_false(X.and_(X.FALSE, X.cmp("a", "=", 1))))
        self.assertTrue(X.is_true(X.or_(X.TRUE, X.cmp("a", "=", 1))))
        self.assertTrue(X.is_false(X.isin("a", [])))
        self.assertEqual(X.columns(X.and_(X.cmp("a", "=", 1), X.not_(X.isin("b", [1])))), {"a", "b"})


if __name__ == "__main__":
    unittest.main()
