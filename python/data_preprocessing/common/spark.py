"""Shared Spark-session setup for the preprocessing jobs."""

from pyspark.sql import SparkSession

# 256 MB read split -> also the output file size (no repartition/shuffle in these jobs).
DEFAULT_MAX_PARTITION_BYTES = 256 * 1024 * 1024


def build_session(app_name: str, max_partition_bytes: int = DEFAULT_MAX_PARTITION_BYTES) -> SparkSession:
    """Create the SparkSession and pin the read split size."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(max_partition_bytes))
    return spark
