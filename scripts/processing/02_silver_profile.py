from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min,
    max,
    avg,
    stddev,
    sum as spark_sum
)


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path.cwd()

BRONZE_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "bronze"
    / "openaq_measurements"
)

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirStream-Silver-Data-Quality-Profile")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext._jsc.hadoopConfiguration()

conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)


# ============================================================
# LOAD BRONZE
# ============================================================

print("=" * 70)
print("AirStream - Silver Data Quality Profiling")
print("=" * 70)

print(f"Bronze directory: {BRONZE_DIR}")

df = spark.read.parquet(str(BRONZE_DIR))

print("\nBronze schema:")
df.printSchema()


# ============================================================
# 1. RECORD COUNT
# ============================================================

print("\n" + "=" * 70)
print("1. RECORD COUNT")
print("=" * 70)

total_records = df.count()

print(f"Total records: {total_records:,}")


# ============================================================
# 2. DATE RANGE
# ============================================================

print("\n" + "=" * 70)
print("2. DATE RANGE")
print("=" * 70)

df.select(
    min("datetime").alias("earliest_datetime"),
    max("datetime").alias("latest_datetime")
).show(truncate=False)


# ============================================================
# 3. STATION COUNT
# ============================================================

print("\n" + "=" * 70)
print("3. STATION COVERAGE")
print("=" * 70)

station_count = df.select("location_id").distinct().count()

print(f"Distinct stations: {station_count:,}")


print("\nMeasurements per station:")

(
    df.groupBy("location_id", "location")
    .agg(
        count("*").alias("measurements"),
        min("datetime").alias("first_measurement"),
        max("datetime").alias("last_measurement"),
        countDistinct("parameter").alias("parameter_count")
    )
    .orderBy(col("measurements").desc())
    .show(20, truncate=False)
)


# ============================================================
# 4. PARAMETER QUALITY
# ============================================================

print("\n" + "=" * 70)
print("4. PARAMETER DISTRIBUTION")
print("=" * 70)

(
    df.groupBy("parameter")
    .agg(
        count("*").alias("records"),
        countDistinct("location_id").alias("stations"),
        min("value").alias("minimum"),
        max("value").alias("maximum"),
        avg("value").alias("average"),
        stddev("value").alias("stddev")
    )
    .orderBy(col("records").desc())
    .show(50, truncate=False)
)


# ============================================================
# 5. NULL CHECK
# ============================================================

print("\n" + "=" * 70)
print("5. NULL VALUE CHECK")
print("=" * 70)

null_checks = df.select(
    spark_sum(col("location_id").isNull().cast("int")).alias("null_location_id"),
    spark_sum(col("sensors_id").isNull().cast("int")).alias("null_sensors_id"),
    spark_sum(col("location").isNull().cast("int")).alias("null_location"),
    spark_sum(col("datetime").isNull().cast("int")).alias("null_datetime"),
    spark_sum(col("lat").isNull().cast("int")).alias("null_lat"),
    spark_sum(col("lon").isNull().cast("int")).alias("null_lon"),
    spark_sum(col("parameter").isNull().cast("int")).alias("null_parameter"),
    spark_sum(col("units").isNull().cast("int")).alias("null_units"),
    spark_sum(col("value").isNull().cast("int")).alias("null_value")
)

null_checks.show(truncate=False)


# ============================================================
# 6. INVALID / NON-FINITE VALUES
# ============================================================

print("\n" + "=" * 70)
print("6. INVALID MEASUREMENT VALUES")
print("=" * 70)

invalid_values = (
    df
    .filter(
        col("value").isNull()
        | (col("value") != col("value"))
        | (col("value") == float("inf"))
        | (col("value") == float("-inf"))
    )
    .count()
)

print(f"Null / NaN / infinite values: {invalid_values:,}")


# ============================================================
# 7. NEGATIVE VALUES
# ============================================================

print("\n" + "=" * 70)
print("7. NEGATIVE VALUES")
print("=" * 70)

negative_values = (
    df
    .filter(col("value") < 0)
    .groupBy("parameter")
    .count()
    .orderBy(col("count").desc())
)

negative_values.show(50, truncate=False)


# ============================================================
# 8. DUPLICATE MEASUREMENTS
# ============================================================

print("\n" + "=" * 70)
print("8. DUPLICATE MEASUREMENTS")
print("=" * 70)

duplicate_keys = [
    "location_id",
    "sensors_id",
    "datetime",
    "parameter"
]

duplicate_groups = (
    df
    .groupBy(*duplicate_keys)
    .count()
    .filter(col("count") > 1)
)

duplicate_group_count = duplicate_groups.count()

duplicate_extra_records = (
    duplicate_groups
    .select(
        spark_sum(col("count") - 1).alias("duplicate_extra_records")
    )
    .collect()[0]["duplicate_extra_records"]
)

print(f"Duplicate groups: {duplicate_group_count:,}")
print(
    f"Extra duplicate records: "
    f"{duplicate_extra_records or 0:,}"
)


# ============================================================
# 9. UNIT DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("9. UNITS")
print("=" * 70)

(
    df
    .groupBy("parameter", "units")
    .count()
    .orderBy("parameter", col("count").desc())
    .show(100, truncate=False)
)


# ============================================================
# 10. COORDINATE CHECK
# ============================================================

print("\n" + "=" * 70)
print("10. COORDINATE QUALITY")
print("=" * 70)

invalid_coordinates = (
    df
    .filter(
        col("lat").isNull()
        | col("lon").isNull()
        | (col("lat") < -90)
        | (col("lat") > 90)
        | (col("lon") < -180)
        | (col("lon") > 180)
    )
    .count()
)

print(f"Invalid coordinates: {invalid_coordinates:,}")


# ============================================================
# 11. YEAR DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("11. YEAR DISTRIBUTION")
print("=" * 70)

(
    df
    .groupBy("year")
    .count()
    .orderBy("year")
    .show(100, truncate=False)
)


# ============================================================
# FINISHED
# ============================================================

print("\n" + "=" * 70)
print("DATA QUALITY PROFILING COMPLETE")
print("=" * 70)

spark.stop()