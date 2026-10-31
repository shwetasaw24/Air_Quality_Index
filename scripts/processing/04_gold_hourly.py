from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    min,
    max,
    count,
    col,
    round as spark_round
)


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path(
    r"C:\Users\Shweeta\OneDrive\Documents\projects\Air Quality Index"
)

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "hourly"


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
    .appName("AirStream-Gold-Hourly")
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
print("AirStream - Gold Hourly Aggregation")
print("=" * 60)

print(f"Spark version: {spark.version}")
print(f"Silver path: {SILVER_PATH}")
print(f"Gold path:   {GOLD_PATH}")


# ============================================================
# LOAD SILVER DATA
# ============================================================

print("\nLoading Silver data...")

silver_df = spark.read.parquet(str(SILVER_PATH))

silver_count = silver_df.count()

print(f"Silver records: {silver_count:,}")

print("\nSilver schema:")
silver_df.printSchema()


# ============================================================
# HOURLY AGGREGATION
# ============================================================

print("\nPerforming hourly aggregation...")

hourly_df = (
    silver_df
    .groupBy(
        "location_id",
        "location",
        "date",
        "hour",
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
# ORDER COLUMNS
# ============================================================

hourly_df = hourly_df.select(
    "location_id",
    "location",
    "date",
    "hour",
    "parameter",
    "avg_value",
    "min_value",
    "max_value",
    "measurement_count"
)


# ============================================================
# DISPLAY SAMPLE
# ============================================================

print("\nSample Gold records:")

hourly_df.orderBy(
    "date",
    "hour",
    "location_id",
    "parameter"
).show(20, truncate=False)


# ============================================================
# COUNT GOLD RECORDS
# ============================================================

gold_count = hourly_df.count()

print(f"\nGold hourly records: {gold_count:,}")


# ============================================================
# WRITE GOLD PARQUET
# ============================================================

print("\nWriting Gold Parquet...")

(
    hourly_df
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(str(GOLD_PATH))
)

print("Gold Parquet write completed.")


# ============================================================
# VALIDATION
# ============================================================

print("\nValidating Gold data...")

gold_df = spark.read.parquet(str(GOLD_PATH))

gold_read_count = gold_df.count()

print(f"Gold records read back: {gold_read_count:,}")

if gold_read_count != gold_count:
    raise RuntimeError(
        f"Gold validation failed: "
        f"written={gold_count:,}, "
        f"read_back={gold_read_count:,}"
    )

print("\nVALIDATION PASSED")
print("=" * 60)


spark.stop()