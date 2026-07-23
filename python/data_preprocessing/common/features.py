"""Shared feature helpers for the preprocessing jobs (z-score standardization + assembly).

These produce an ENGINE-NEUTRAL result: features stay plain doubles packed into an
`array<double>` column, never a Spark ML VectorUDT, so both Spark and Flink can read the
output. `compute_stats` + `standardize` together are the manual equivalent of a Spark ML
StandardScaler (mean-centering + unit variance, sample stddev), without the Vector type.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_stats(df: DataFrame, feature_cols: list[str]):
    """One pass: per-feature mean/std + total row count (`__n`).

    Returns a Row with `__n`, and `<col>__m` / `<col>__s` for each feature — all computed
    in a single scan instead of one pass per aggregate.
    """
    agg = [F.count(F.lit(1)).alias("__n")]
    agg += [F.mean(c).alias(f"{c}__m") for c in feature_cols]
    agg += [F.stddev(c).alias(f"{c}__s") for c in feature_cols]
    return df.select(*agg).first()


def standardize(df: DataFrame, feature_cols: list[str], stats) -> DataFrame:
    """Z-score each feature using precomputed `stats`: (c - mean) / std."""
    scaled = []
    for c in feature_cols:
        mean = stats[f"{c}__m"]
        std = stats[f"{c}__s"]
        std = std if std and std > 0 else 1.0  # guard constant columns
        scaled.append(((F.col(c) - F.lit(mean)) / F.lit(std)).alias(c))
    return df.select(*scaled)


def to_feature_array(df: DataFrame, feature_cols: list[str], name: str = "features") -> DataFrame:
    """Pack the feature columns into a single engine-neutral `array<double>` column."""
    return df.select(F.array(*[F.col(c) for c in feature_cols]).alias(name))
