"""Native Spark as-of join helpers for trades / quotes style time-series data.

An *as-of join* attaches to every row of a left table (trades) the most recent
row of a right table (quotes) with the same key whose timestamp is at or before
the left row's timestamp. It is the `aj` of kdb+ and the `ASOF JOIN` of some
warehouses, expressed here with plain Spark window functions so it runs on any
Databricks compute without an external tick database.

Two entry points:

* :func:`as_of_join` -- one window per key. Right for keys that carry up to
  roughly a million quotes; above that the per-key window becomes the long pole.
* :func:`bucket_by_quote_density` + :func:`as_of_join` with ``bucket_col`` --
  splits every key into ``num_buckets`` time ranges that hold about the same
  number of quotes, assigns each trade to the range it falls in, and windows over
  ``(key, bucket)`` instead of ``key``. Bounded window sizes, no skew.

Both paths are pure DataFrame transformations and are unit-tested in
``tests/test_asof_spark.py`` on a local SparkSession.
"""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

INDICATOR_COL = "_asof_indicator"
QUOTE = -1  # sorts before a trade with the same timestamp -> quote "as of" that instant is visible
TRADE = 1


def _null_like(df: DataFrame, cols: Sequence[str]):
    """``lit(None)`` columns typed like ``cols`` in ``df`` so the two sides union cleanly."""
    types = dict(df.dtypes)
    return [F.lit(None).cast(types[c]).alias(c) for c in cols]


def as_of_join(
    trades_df: DataFrame,
    quotes_df: DataFrame,
    key_cols: Sequence[str],
    time_col: str,
    quote_value_cols: Sequence[str],
    bucket_col: str | None = None,
    allow_equal_time: bool = True,
) -> DataFrame:
    """Attach the latest quote values at or before each trade.

    Args:
        trades_df: left side; every row is kept, all its columns pass through.
        quotes_df: right side; must carry ``key_cols``, ``time_col`` and
            ``quote_value_cols`` (other quote columns are ignored).
        key_cols: join / partition keys, e.g. ``["sym"]``.
        time_col: event-time column present on both sides, same type.
        quote_value_cols: quote columns to carry onto the trade, e.g. ``["ask", "bid"]``.
            They must not already exist on ``trades_df``.
        bucket_col: optional column present on both sides (see
            :func:`bucket_by_quote_density`); when given, the window is
            partitioned by ``key_cols + [bucket_col]``.
        allow_equal_time: when True (default, kdb+ ``aj`` semantics) a quote with
            exactly the trade's timestamp is eligible; when False only strictly
            earlier quotes are.

    Returns:
        ``trades_df`` columns plus ``quote_value_cols``; null where no eligible
        quote exists for the key (or bucket).
    """
    key_cols = list(key_cols)
    quote_value_cols = list(quote_value_cols)
    clash = set(quote_value_cols) & set(trades_df.columns)
    if clash:
        raise ValueError(f"quote_value_cols already present on trades_df: {sorted(clash)}")
    partition_cols = key_cols + ([bucket_col] if bucket_col else [])
    shared = partition_cols + [time_col]
    trade_only = [c for c in trades_df.columns if c not in shared]

    # Make both sides carry the same columns in the same order, then union.
    quotes_side = quotes_df.select(
        *shared,
        F.lit(QUOTE).alias(INDICATOR_COL),
        *_null_like(trades_df, trade_only),
        *quote_value_cols,
    )
    trades_side = trades_df.select(
        *shared,
        F.lit(TRADE).alias(INDICATOR_COL),
        *trade_only,
        *_null_like(quotes_df, quote_value_cols),
    )
    combined = quotes_side.unionByName(trades_side)

    indicator_order = F.col(INDICATOR_COL) if allow_equal_time else F.col(INDICATOR_COL).desc()
    window = (
        Window.partitionBy(*partition_cols)
        .orderBy(F.col(time_col), indicator_order)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # For each quote value: the last non-null value seen on a *quote* row so far.
    for c in quote_value_cols:
        combined = combined.withColumn(
            c, F.last(F.when(F.col(INDICATOR_COL) == QUOTE, F.col(c)), ignorenulls=True).over(window)
        )

    return combined.filter(F.col(INDICATOR_COL) == TRADE).drop(INDICATOR_COL)


def bucket_by_quote_density(
    trades_df: DataFrame,
    quotes_df: DataFrame,
    key_cols: Sequence[str],
    time_col: str,
    num_buckets: int = 500,
    bucket_col: str = "bucket",
) -> tuple[DataFrame, DataFrame]:
    """Split each key's timeline into ``num_buckets`` ranges of equal quote count.

    Quotes get ``ntile(num_buckets)`` over ``(key order by time)``. Each bucket's
    range starts at its first quote and ends just before the next bucket's first
    quote (open-ended for the last bucket). A trade is assigned the bucket whose
    range contains its timestamp, so the bucket always holds at least one quote
    at or before the trade. Trades earlier than the key's first quote, or for
    keys without quotes, get a null bucket and therefore null quote values from
    :func:`as_of_join`, which is the correct answer.

    Returns:
        ``(trades_with_bucket, quotes_with_bucket)`` ready for
        ``as_of_join(..., bucket_col=bucket_col)``. Persist them (ideally
        ``CLUSTER BY (key, time, bucket)``) when the join is run more than once.
    """
    key_cols = list(key_cols)
    if bucket_col in trades_df.columns or bucket_col in quotes_df.columns:
        raise ValueError(f"bucket_col {bucket_col!r} already exists on an input")

    quotes_with_bucket = quotes_df.withColumn(
        bucket_col, F.ntile(num_buckets).over(Window.partitionBy(*key_cols).orderBy(time_col))
    )

    next_start = F.lead("_bucket_start").over(Window.partitionBy(*key_cols).orderBy(bucket_col))
    bounds = (
        quotes_with_bucket.groupBy(*key_cols, bucket_col)
        .agg(F.min(time_col).alias("_bucket_start"))
        .withColumn("_next_bucket_start", next_start)
    )

    t, b = trades_df.alias("t"), bounds.alias("b")
    cond = [F.col(f"t.{k}") == F.col(f"b.{k}") for k in key_cols]
    cond.append(F.col(f"t.{time_col}") >= F.col("b._bucket_start"))
    cond.append((F.col(f"t.{time_col}") < F.col("b._next_bucket_start")) | F.col("b._next_bucket_start").isNull())
    joined = t.join(b, cond, "left")
    trades_with_bucket = joined.select(*[F.col(f"t.{c}") for c in trades_df.columns], F.col(f"b.{bucket_col}"))

    return trades_with_bucket, quotes_with_bucket
