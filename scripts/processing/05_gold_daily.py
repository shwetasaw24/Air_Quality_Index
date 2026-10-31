from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    min,
    max,
    count,
    round as spark_round
)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(
    r"C:\Users\Shweeta\OneDrive\Documents\projects\Air Quality Index"
)

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "processed" / "gold" / "daily"


# ============================================================
# BARE LOCAL FILESYSTEM
# ============================================================

BARE_FS_JAR = Path(
    r"C:\Users\Shweeta\.ivy2.5.2\jars"
    r"\com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirStream-Gold-Daily")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Configure local filesystem
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)


print("=" * 60)
print("AirStream - Gold Daily Aggregation")
print("=" * 60)

print(f"Spark version: {spark.version}")
print(f"Silver path: {SILVER_PATH}")
print(f"Gold path:   {GOLD_PATH}")


# ============================================================
# LOAD SILVER
# ============================================================

print("\nLoading Silver data...")

silver_df = spark.read.parquet(str(SILVER_PATH))

silver_count = silver_df.count()

print(f"Silver records: {silver_count:,}")


# ============================================================
# DAILY AGGREGATION
# ============================================================

print("\nPerforming daily aggregation...")

daily_df = (
    silver_df
    .groupBy(
        "location_id",
        "location",
        "date",
        "parameter"
    )
    .agg(
        spark_round(avg("value"), 4).alias("avg_value"),
        spark_round(min("value"), 4).alias("min_value"),
        spark_round(max("value"), 4).alias("max_value"),
        count("*").alias("measurement_count")
    )
)


# ============================================================
# SELECT FINAL COLUMNS
# ============================================================

daily_df = daily_df.select(
    "location_id",
    "location",
    "date",
    "parameter",
    "avg_value",
    "min_value",
    "max_value",
    "measurement_count"
)


# ============================================================
# SHOW SAMPLE
# ============================================================

print("\nSample Gold daily records:")

daily_df.orderBy(
    "date",
    "location_id",
    "parameter"
).show(20, truncate=False)


# ============================================================
# COUNT
# ============================================================

daily_count = daily_df.count()

print(f"\nGold daily records: {daily_count:,}")


# ============================================================
# WRITE PARQUET
# ============================================================

print("\nWriting Gold daily Parquet...")

(
    daily_df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(str(GOLD_PATH))
)

print("Gold daily Parquet write completed.")


# ============================================================
# VALIDATION
# ============================================================

print("\nValidating Gold daily data...")

gold_df = spark.read.parquet(str(GOLD_PATH))

gold_read_count = gold_df.count()

print(f"Gold daily records read back: {gold_read_count:,}")


if gold_read_count != daily_count:
    raise RuntimeError(
        f"Validation failed: "
        f"written={daily_count:,}, "
        f"read_back={gold_read_count:,}"
    )


print("\nVALIDATION PASSED")
print("=" * 60)


spark.stop()