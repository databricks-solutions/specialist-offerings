"""Unit tests for asof_spark: window as-of join and quote-density bucketing.

Timestamps are integers here (seconds) to keep the fixtures readable; the code
only relies on ordering and comparison, which behave identically for TIMESTAMP.
"""

from __future__ import annotations

import random

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from asof_spark import as_of_join, bucket_by_quote_density

KEYS = ["sym"]
TIME = "time"
VALUES = ["ask", "bid"]


def _trades(spark, rows):
    return spark.createDataFrame([Row(sym=s, time=t, price=p, size=z) for s, t, p, z in rows])


def _quotes(spark, rows):
    return spark.createDataFrame([Row(sym=s, time=t, ask=a, bid=b) for s, t, a, b in rows])


def _as_dict(df, cols=("sym", "time", "price", "size", "ask", "bid")):
    return {(r["sym"], r["time"]): tuple(r[c] for c in cols) for r in df.select(*cols).collect()}


@pytest.fixture(scope="module")
def small(spark):
    quotes = _quotes(
        spark,
        [
            ("AAA", 1, 10.1, 10.0),
            ("AAA", 3, 10.3, 10.2),
            ("AAA", 5, 10.5, 10.4),
            ("BBB", 2, 20.1, 20.0),
        ],
    )
    trades = _trades(
        spark,
        [
            ("AAA", 0, 10.05, 100),  # before any quote
            ("AAA", 2, 10.15, 200),  # between quotes @1 and @3
            ("AAA", 3, 10.25, 300),  # exactly at quote @3
            ("AAA", 6, 10.45, 400),  # after the last quote
            ("BBB", 9, 20.05, 500),  # other key
            ("CCC", 1, 30.00, 600),  # key with no quotes at all
        ],
    )
    return trades, quotes


def test_latest_quote_at_or_before_trade(small):
    trades, quotes = small
    out = _as_dict(as_of_join(trades, quotes, KEYS, TIME, VALUES))
    assert out[("AAA", 0)][4:] == (None, None)
    assert out[("AAA", 2)][4:] == (10.1, 10.0)
    assert out[("AAA", 3)][4:] == (10.3, 10.2)  # equal timestamp is eligible by default
    assert out[("AAA", 6)][4:] == (10.5, 10.4)
    assert out[("BBB", 9)][4:] == (20.1, 20.0)
    assert out[("CCC", 1)][4:] == (None, None)


def test_trade_columns_pass_through_and_row_count_preserved(small):
    trades, quotes = small
    result = as_of_join(trades, quotes, KEYS, TIME, VALUES)
    assert result.count() == trades.count()
    assert set(result.columns) == set(trades.columns) | set(VALUES)
    out = _as_dict(result)
    assert out[("AAA", 2)][:4] == ("AAA", 2, 10.15, 200)


def test_strictly_earlier_quotes_when_equal_time_not_allowed(small):
    trades, quotes = small
    out = _as_dict(as_of_join(trades, quotes, KEYS, TIME, VALUES, allow_equal_time=False))
    assert out[("AAA", 3)][4:] == (10.1, 10.0)  # the @3 quote is no longer eligible
    assert out[("AAA", 2)][4:] == (10.1, 10.0)  # unchanged where no tie exists


def test_quote_value_column_clash_is_rejected(small):
    trades, quotes = small
    with pytest.raises(ValueError, match="already present"):
        as_of_join(trades.withColumn("ask", F.lit(0.0)), quotes, KEYS, TIME, VALUES)


def test_bucket_assignment(spark):
    quotes = _quotes(spark, [("AAA", t, float(t), float(t) - 0.1) for t in (10, 20, 30, 40, 50, 60)])
    trades = _trades(
        spark,
        [
            ("AAA", 5, 1.0, 1),  # before first quote -> null bucket
            ("AAA", 10, 1.0, 1),  # exactly at bucket 1 start
            ("AAA", 29, 1.0, 1),  # last instant of bucket 1 (quotes 10, 20)
            ("AAA", 30, 1.0, 1),  # first instant of bucket 2 (quotes 30, 40)
            ("AAA", 99, 1.0, 1),  # after the last quote -> last bucket (open-ended)
            ("ZZZ", 10, 1.0, 1),  # key without quotes -> null bucket
        ],
    )
    t_b, q_b = bucket_by_quote_density(trades, quotes, KEYS, TIME, num_buckets=3)

    q_counts = {r["bucket"]: r["n"] for r in q_b.groupBy("bucket").agg(F.count("*").alias("n")).collect()}
    assert q_counts == {1: 2, 2: 2, 3: 2}

    buckets = {(r["sym"], r["time"]): r["bucket"] for r in t_b.collect()}
    assert buckets == {
        ("AAA", 5): None,
        ("AAA", 10): 1,
        ("AAA", 29): 1,
        ("AAA", 30): 2,
        ("AAA", 99): 3,
        ("ZZZ", 10): None,
    }
    assert t_b.count() == trades.count()  # left join never drops or duplicates trades


def test_bucketed_join_matches_plain_join(spark):
    """The bucketed path is an optimisation only: same answer as the single window."""
    rng = random.Random(7)
    quotes_rows, trades_rows = [], []
    for sym, n_quotes in (("SKEW", 400), ("THIN", 15), ("ONE", 1)):
        for _ in range(n_quotes):
            t = rng.randint(0, 10_000)
            quotes_rows.append((sym, t, 100 + rng.random(), 99 + rng.random()))
        for _ in range(120):
            trades_rows.append((sym, rng.randint(-50, 10_050), 100.0, rng.randint(1, 500)))
    # Deliberate ties: trades at the exact time of some quotes.
    trades_rows += [(s, t, 1.0, 1) for s, t, _, _ in quotes_rows[:25]]

    quotes = _quotes(spark, quotes_rows)
    trades = _trades(spark, trades_rows).withColumn("trade_id", F.monotonically_increasing_id())

    plain = as_of_join(trades, quotes, KEYS, TIME, VALUES)
    t_b, q_b = bucket_by_quote_density(trades, quotes, KEYS, TIME, num_buckets=7)
    bucketed = as_of_join(t_b, q_b, KEYS, TIME, VALUES, bucket_col="bucket").drop("bucket")

    cols = ["trade_id", "sym", "time", "price", "size", "ask", "bid"]
    assert plain.count() == trades.count() == bucketed.count()
    assert sorted(plain.select(*cols).collect()) == sorted(bucketed.select(*cols).collect())
