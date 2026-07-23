"""
Preprocess the ESA Gaia DR3 data for clustering.

The raw release has 155 columns; for clustering we keep astrometry + proper motion +
GSP-Phot stellar parameters, turn the sky position into 3D Cartesian coordinates, and
z-score standardize. The output is an ENGINE-NEUTRAL Parquet dataset:

    features : array<double>   (8 standardized features, see FEATURE_COLS for the order)

Stored as a plain double array on purpose (NOT a Spark ML VectorUDT), so both Spark and
Flink can read it; each engine wraps `features` in its own vector type at load time.

Feature layout (index -> meaning):
    0 X, 1 Y, 2 Z               3D position (parsecs), from parallax distance + ra/dec
    3 pmra, 4 pmdec             proper motion (mas/yr)
    5 teff_gspphot              effective temperature (K)
    6 logg_gspphot              surface gravity (log cgs)
    7 mh_gspphot                metallicity [M/H]

Aggressive cleaning: rows with any NULL/NaN in the used columns are dropped, and only
parallax > 0 is kept (negative parallax is noise). Requiring the GSP-Phot columns keeps
only stars that have astrophysical parameters, which is a large but sane subset.
"""

import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)

INPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/gaia_dr3_data/gaia_dr3_dataset/"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/gaia_data_preprocessed"

# Raw columns to read (ra/dec/parallax feed the Cartesian transform; the rest are kept).
RAW_COLS = ["ra", "dec", "parallax", "pmra", "pmdec", "teff_gspphot", "logg_gspphot", "mh_gspphot"]

# Final vector layout. Order defines the index of each feature in `features`.
FEATURE_COLS = [
    "X",
    "Y",
    "Z",
    "pmra",
    "pmdec",
    "teff_gspphot",
    "logg_gspphot",
    "mh_gspphot",
]

MAX_PARTITION_BYTES = 256 * 1024 * 1024  # 256 MB read split -> output file size


def _read_raw(spark: SparkSession) -> DataFrame:
    """Read the needed columns, cast everything to double for a uniform schema."""
    return spark.read.parquet(INPUT_PATH).select(
        *[F.col(c).cast("double").alias(c) for c in RAW_COLS]
    )


def _filter_valid(df: DataFrame) -> DataFrame:
    """Aggressive clean: drop any NULL/NaN in the used columns, keep parallax > 0."""
    return df.na.drop(subset=RAW_COLS).where(F.col("parallax") > 0)


def _to_cartesian(df: DataFrame) -> DataFrame:
    """Sky position (ra, dec, parallax) -> 3D Cartesian X/Y/Z in parsecs.

    distance[pc] = 1000 / parallax[mas]; ra/dec converted to radians first.
    """
    dist = F.lit(1000.0) / F.col("parallax")
    ra = F.radians("ra")
    dec = F.radians("dec")
    return (
        df.withColumn("X", dist * F.cos(dec) * F.cos(ra))
        .withColumn("Y", dist * F.cos(dec) * F.sin(ra))
        .withColumn("Z", dist * F.sin(dec))
    )


def compute_stats(df: DataFrame):
    """One pass: per-feature mean/std + row count (for standardization + logging)."""
    agg = [F.count(F.lit(1)).alias("__n")]
    agg += [F.mean(c).alias(f"{c}__m") for c in FEATURE_COLS]
    agg += [F.stddev(c).alias(f"{c}__s") for c in FEATURE_COLS]
    return df.select(*agg).first()


def standardize(df: DataFrame, stats) -> DataFrame:
    """Z-score each feature: (c - mean) / std. Engine-neutral, no ML VectorUDT."""
    scaled = []
    for c in FEATURE_COLS:
        mean = stats[f"{c}__m"]
        std = stats[f"{c}__s"]
        std = std if std and std > 0 else 1.0  # guard constant columns
        scaled.append(((F.col(c) - F.lit(mean)) / F.lit(std)).alias(c))
    return df.select(*scaled)


def main() -> None:
    spark = SparkSession.builder.appName("Gaia_Data_Prep").getOrCreate()
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))

    raw = _read_raw(spark)
    raw_count = raw.count()  # pre-filter total (cheap: Parquet footers, no filter yet)

    features = _to_cartesian(_filter_valid(raw)).select(*FEATURE_COLS)

    # Two lazy passes (no cache): pass 1 = stats, pass 2 = write. Deterministic pipeline,
    # so both passes see identical rows.
    stats = compute_stats(features)  # pass 1
    kept = stats["__n"]

    scaled = standardize(features, stats)
    out = scaled.select(F.array(*[F.col(c) for c in FEATURE_COLS]).alias("features"))
    out.write.mode("overwrite").option("compression", "snappy").parquet(OUTPUT_PATH)  # pass 2

    logger.info("Gaia preprocessing done")
    logger.info("  input rows : %s", f"{raw_count:,}")
    logger.info("  kept rows  : %s", f"{kept:,}")
    logger.info("  dropped    : %s", f"{raw_count - kept:,}")
    logger.info("  features   : %s", ", ".join(FEATURE_COLS))
    logger.info("  output     : %s", OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
