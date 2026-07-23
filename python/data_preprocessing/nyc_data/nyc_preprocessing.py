"""
Preprocess the NYC TLC Yellow Taxi trip data for spatio-temporal clustering.

Raw records have 19 columns; for clustering we build an 8-dim feature vector mixing
time, space and trip attributes, then z-score standardize it. The output is an
ENGINE-NEUTRAL Parquet dataset:

    features : array<double>   (8 standardized features, see FEATURE_COLS for the order)

The feature vector is stored as a plain double array on purpose — NOT a Spark ML
VectorUDT. The benchmark runs on both Spark and Flink, so the cleaned data must be
readable by both; each engine wraps `features` in its own vector type at load time.

Feature layout (index -> meaning):
    0 hour_sin, 1 hour_cos      cyclic hour-of-day  (00:00 == 24:00)
    2 dow_sin,  3 dow_cos       cyclic day-of-week  (Sun adjacent to Sat)
    4 pickup_latitude           pickup zone centroid  (spatial -> data skew on Manhattan)
    5 pickup_longitude
    6 trip_distance             trip attributes -> heterogeneous vector
    7 fare_amount

TLC trips (since 2015) carry no raw coordinates, only PULocationID; latitude/longitude
come from a broadcast join with the taxi-zone lookup table (one centroid per zone).
"""

import glob
import math
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.logger import get_logger

logger = get_logger(__name__)

# Raw trips. All 169 files (2011-01 .. 2025-01) carry the needed columns, but their
# PHYSICAL types drift across years (PULocationID int32/int64, passenger_count
# int64/double, ...). A single directory read fails because Spark can't reconcile those
# types, so we read each file on its own and cast to canonical types (see _read_trips).
INPUT_DIR = "/net/pr2/projects/plgrid/plggclustering25/nyc_data"
INPUT_GLOB = "yellow_tripdata_*.parquet"
ZONES_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/nyc_data/taxi_zone_lookup_with_latlon.csv"
OUTPUT_PATH = "file:///net/pr2/projects/plgrid/plggclustering25/nyc_data_preprocessed"

# Zone-lookup CSV column names — produced by build_zone_centroids.py.
ZONE_ID_COL = "LocationID"
ZONE_LAT_COL = "lat"
ZONE_LON_COL = "lon"

# Final vector layout. Order defines the index of each feature in `features`.
FEATURE_COLS = [
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "pickup_latitude",
    "pickup_longitude",
    "trip_distance",
    "fare_amount",
]

MAX_PARTITION_BYTES = 256 * 1024 * 1024  # 256 MB read split -> output file size

_TWO_PI = 2 * math.pi


def _read_trips(spark: SparkSession) -> DataFrame:
    """Read every parquet file individually and cast to canonical types, then union.

    Reading the whole directory at once fails: physical types differ across years and
    neither reader reconciles them. Read per file (each has one consistent schema, read
    natively) and cast long/int -> double/long in Catalyst AFTER the scan, so the union
    sees identical schemas.
    """
    paths = sorted(glob.glob(os.path.join(INPUT_DIR, INPUT_GLOB)))
    if not paths:
        raise SystemExit(f"No parquet files under {INPUT_DIR}/{INPUT_GLOB}")
    logger.info("Reading %d parquet files", len(paths))

    def read_one(path: str) -> DataFrame:
        return spark.read.parquet("file://" + path).select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
            F.col("PULocationID").cast("long").alias("pu_location_id"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("passenger_count").cast("double").alias("passenger_count"),
        )

    return reduce(DataFrame.unionByName, (read_one(p) for p in paths))


def build_features(spark: SparkSession):
    """Read raw trips + zones, engineer + join features (NOT yet standardized).

    Returns (features_df, raw_row_count). The raw count is the pre-filter total (cheap:
    Spark reads only Parquet footers since there's no filter yet).
    """
    trips = _read_trips(spark)
    raw_count = trips.count()

    # Drop nulls + physically impossible / outlier rows.
    trips = (
        trips.na.drop(subset=["pickup_ts", "pu_location_id", "trip_distance", "fare_amount", "passenger_count"])
        .where((F.col("trip_distance") > 0) & (F.col("trip_distance") < 100))
        .where((F.col("fare_amount") > 0) & (F.col("fare_amount") < 500))
        .where(F.col("passenger_count") > 0)
    )

    # Cyclic time features (both hour and day-of-week are periodic).
    hour = F.hour("pickup_ts")
    dow = F.dayofweek("pickup_ts")  # 1 (Sun) .. 7 (Sat)
    trips = (
        trips.withColumn("hour_sin", F.sin(hour * (_TWO_PI / 24)))
        .withColumn("hour_cos", F.cos(hour * (_TWO_PI / 24)))
        .withColumn("dow_sin", F.sin((dow - 1) * (_TWO_PI / 7)))
        .withColumn("dow_cos", F.cos((dow - 1) * (_TWO_PI / 7)))
    )

    # Spatial features: join zone centroids (tiny table -> broadcast, no shuffle).
    zones = spark.read.option("header", "true").csv(ZONES_PATH).select(
        F.col(ZONE_ID_COL).cast("long").alias("zone_id"),
        F.col(ZONE_LAT_COL).cast("double").alias("pickup_latitude"),
        F.col(ZONE_LON_COL).cast("double").alias("pickup_longitude"),
    ).where(F.col("pickup_latitude").isNotNull() & F.col("pickup_longitude").isNotNull())

    joined = trips.join(
        F.broadcast(zones), trips["pu_location_id"] == zones["zone_id"], "inner"
    )
    return joined.select(*FEATURE_COLS), raw_count


def compute_stats(df):
    """One pass: per-feature mean/std + row count (for standardization + logging)."""
    agg = [F.count(F.lit(1)).alias("__n")]
    agg += [F.mean(c).alias(f"{c}__m") for c in FEATURE_COLS]
    agg += [F.stddev(c).alias(f"{c}__s") for c in FEATURE_COLS]
    return df.select(*agg).first()


def standardize(df, stats):
    """Z-score each feature: (c - mean) / std. Engine-neutral, no ML VectorUDT."""
    scaled = []
    for c in FEATURE_COLS:
        mean = stats[f"{c}__m"]
        std = stats[f"{c}__s"]
        std = std if std and std > 0 else 1.0  # guard constant columns
        scaled.append(((F.col(c) - F.lit(mean)) / F.lit(std)).alias(c))
    return df.select(*scaled)


def main() -> None:
    spark = SparkSession.builder.appName("NYC_Data_Prep").getOrCreate()
    spark.conf.set("spark.sql.files.maxPartitionBytes", str(MAX_PARTITION_BYTES))

    # Two lazy passes over the (small) raw data — no cache, so nothing spills to the tiny
    # local disk: pass 1 = stats, pass 2 = write. build_features() is deterministic so both
    # passes see identical rows.
    features, raw_count = build_features(spark)
    stats = compute_stats(features)  # pass 1
    kept = stats["__n"]

    scaled = standardize(features, stats)
    out = scaled.select(F.array(*[F.col(c) for c in FEATURE_COLS]).alias("features"))
    out.write.mode("overwrite").option("compression", "snappy").parquet(OUTPUT_PATH)  # pass 2

    logger.info("NYC preprocessing done")
    logger.info("  input rows : %s", f"{raw_count:,}")
    logger.info("  kept rows  : %s", f"{kept:,}")
    logger.info("  dropped    : %s", f"{raw_count - kept:,}")
    logger.info("  features   : %s", ", ".join(FEATURE_COLS))
    logger.info("  output     : %s", OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
