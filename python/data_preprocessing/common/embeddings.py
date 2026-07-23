"""Shared preprocessing for embedding datasets (Cohere, MongoDB, Monet, ...).

They all do the same thing: read Parquet, keep one embedding column, drop
null/wrong-length/NaN vectors, and write an ENGINE-NEUTRAL `array<float>` (never a Spark
ML VectorUDT). Only the column name, dimensionality and paths differ.
"""

from pyspark.sql import functions as F

from data_preprocessing.common.spark import build_session
from utils.logger import get_logger

logger = get_logger(__name__)


def preprocess_embeddings(
    app_name: str,
    input_path: str,
    output_path: str,
    emb_col: str,
    emb_dim: int,
) -> None:
    """Clean a single embedding column into `emb : array<float>` Parquet.

    Narrow transform (project + filter), no shuffle. `df[emb_col]` (not `F.col`) so column
    names with hyphens/dots resolve correctly.
    """
    spark = build_session(app_name)

    raw = spark.read.parquet(input_path)
    raw_count = raw.count()

    cleaned = (
        raw.select(raw[emb_col].alias("emb"))
        .where(F.col("emb").isNotNull())
        # Exactly emb_dim elements (size(null) is -1, so this also rejects nulls).
        .where(F.size("emb") == emb_dim)
        # No NaN / Inf anywhere in the vector — makes "cleaned" a real guarantee.
        .where(F.forall("emb", lambda x: ~(F.isnan(x) | F.isnull(x))))
    )

    cleaned.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

    # Re-read the output for an honest post-write count (cleaned is lazy / not cached).
    kept = spark.read.parquet(output_path).count()
    logger.info("%s: embedding preprocessing done", app_name)
    logger.info("  input rows : %s", f"{raw_count:,}")
    logger.info("  kept rows  : %s", f"{kept:,}")
    logger.info("  dropped    : %s", f"{raw_count - kept:,}")
    logger.info("  dim        : %d", emb_dim)
    logger.info("  output     : %s", output_path)

    spark.stop()
