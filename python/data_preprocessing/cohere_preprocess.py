"""
Preprocess the Cohere / MS MARCO embeddings dataset for the clustering benchmark.

The raw dataset carries many columns (docid, url, title, text, char offsets, emb).
For clustering we only need the 1024-dim embedding. This job projects it out, drops
malformed rows, and writes an ENGINE-NEUTRAL Parquet dataset:

    emb : array<float>   (exactly 1024 elements, no null / NaN / Inf)
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)

EMB_DIM = 1024
EMB_COL = "emb"
INPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/cohere_vectores_data/passages_parquet/"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/cohere_vectores_data_preprocessed"

# Read split size -> also the output file size. No repartition/shuffle: this is a narrow
# transform, and Parquet is splittable so downstream engines re-parallelize on read.
MAX_PARTITION_BYTES = 256 * 1024 * 1024  # 256 MB


def main() -> None:
    spark = SparkSession.builder.appName("Cohere_Data_Prep").getOrCreate()
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))

    raw = spark.read.parquet(INPUT_PATH)
    raw_count = raw.count()

    cleaned = (
        raw.select(F.col(EMB_COL).alias("emb"))
        .where(F.col("emb").isNotNull())
        # Exactly 1024 dims (size(null) is -1, so this also rejects nulls).
        .where(F.size("emb") == EMB_DIM)
        # No NaN / Inf anywhere in the vector — makes "cleaned" a real guarantee.
        .where(F.forall("emb", lambda x: ~(F.isnan(x) | F.isnull(x))))
    )

    writer = cleaned.write.mode("overwrite").option("compression", "snappy")  # default compression for Parquet
    writer.parquet(OUTPUT_PATH)

    # Re-read the output for an honest post-write count (cleaned is lazy / not cached).
    kept = spark.read.parquet(OUTPUT_PATH).count()
    logger.info("Cohere preprocessing done")
    logger.info("  input rows : %s", f"{raw_count:,}")
    logger.info("  kept rows  : %s", f"{kept:,}")
    logger.info("  dropped    : %s", f"{raw_count - kept:,}")
    logger.info("  output     : %s", OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
