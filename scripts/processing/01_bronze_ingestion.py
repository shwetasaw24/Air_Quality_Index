from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path.cwd()

INPUT_DIR = PROJECT_ROOT / "data" / "raw" / "openaq" / "historical"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "bronze" / "openaq_measurements"

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirStream-Bronze-Ingestion")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.hadoop.io.native.lib.available", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Use pure Java local filesystem.
# This avoids the Windows winutils.exe requirement.
conf = spark.sparkContext._jsc.hadoopConfiguration()

conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)

print("=" * 70)
print("AirStream - Bronze Ingestion")
print("=" * 70)
print(f"Input directory : {INPUT_DIR}")
print(f"Output directory: {OUTPUT_DIR}")
print(f"Bare FS JAR     : {BARE_FS_JAR}")
print(f"Filesystem      : {conf.get('fs.file.impl')}")
print("=" * 70)


# ============================================================
# EXPLICIT SCHEMA
# ============================================================

schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("sensors_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("parameter", StringType(), True),
    StructField("units", StringType(), True),
    StructField("value", DoubleType(), True),
])


# ============================================================
# DISCOVER ALL CSV.GZ FILES
# ============================================================

print("\nDiscovering input files...")

files = [
    str(path)
    for path in INPUT_DIR.rglob("*.csv.gz")
]

print(f"CSV.gz files discovered: {len(files):,}")

if not files:
    raise RuntimeError("No .csv.gz files found!")


# ============================================================
# READ RAW DATA
# ============================================================

print("\nReading OpenAQ data with Spark...")

df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(files)
)

raw_count = df.count()

print(f"Records read: {raw_count:,}")


# ============================================================
# TRANSFORM TO BRONZE
# ============================================================

print("\nCreating Bronze columns...")

bronze_df = (
    df
    .withColumn(
        "datetime",
        to_timestamp(col("datetime"))
    )
    .withColumn(
        "year",
        year(col("datetime"))
    )
    .withColumn(
        "month",
        month(col("datetime"))
    )
)


# ============================================================
# BASIC VALIDATION
# ============================================================

print("\nBronze schema:")

bronze_df.printSchema()

print("\nSample records:")

bronze_df.show(10, truncate=False)


print("\nParameter distribution:")

(
    bronze_df
    .groupBy("parameter")
    .count()
    .orderBy(col("count").desc())
    .show(50, truncate=False)
)


print("\nChecking null timestamps:")

null_datetime = (
    bronze_df
    .filter(col("datetime").isNull())
    .count()
)

print(f"Null datetime records: {null_datetime:,}")


# ============================================================
# WRITE BRONZE PARQUET
# ============================================================

print("\nWriting Bronze Parquet...")
print("This may take several minutes for ~30.9M records.")

(
    bronze_df
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(str(OUTPUT_DIR))
)

print("\n" + "=" * 70)
print("BRONZE WRITE SUCCESS")
print("=" * 70)


# ============================================================
# READ BACK FOR VALIDATION
# ============================================================

print("\nReading Bronze Parquet back...")

bronze_check = spark.read.parquet(str(OUTPUT_DIR))

bronze_count = bronze_check.count()

print(f"Bronze records read back: {bronze_count:,}")

print("\nBronze schema after Parquet write:")

bronze_check.printSchema()

print("\nBronze sample:")

bronze_check.show(10, truncate=False)


# ============================================================
# FINAL VALIDATION
# ============================================================

if bronze_count != raw_count:
    raise RuntimeError(
        f"Record count mismatch! "
        f"Raw={raw_count:,}, Bronze={bronze_count:,}"
    )

print("\n" + "=" * 70)
print("BRONZE INGESTION COMPLETE")
print(f"Records preserved: {bronze_count:,}")
print("Record count validation: PASSED")
print("=" * 70)


spark.stop()