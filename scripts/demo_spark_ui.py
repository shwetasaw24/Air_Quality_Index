import os
import time
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)

spark = (
    SparkSession.builder
    .appName("AirStream-Spark-UI-Demo")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Avoid Windows winutils dependency
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)

print("\n" + "=" * 60)
print("        AIRSTREAM - SPARK UI DEMONSTRATION")
print("=" * 60)

print(f"\nSpark version: {spark.version}")
print(f"Input: {SILVER_PATH}")

print("\nLoading Silver data...")

df = spark.read.parquet(str(SILVER_PATH))

print("\n--- SCHEMA ---")
df.printSchema()

print("\nCaching Silver DataFrame...")
df.cache()

print("\nCounting records...")
count = df.count()

print(f"\nSilver records: {count:,}")

print("\n--- SAMPLE DATA ---")
df.select(
    "location_id",
    "location",
    "datetime",
    "parameter",
    "units",
    "value"
).show(20, truncate=False)

print("\n--- PARTITION INFORMATION ---")
print(f"Number of Spark partitions: {df.rdd.getNumPartitions()}")

print("\n" + "=" * 60)
print("SPARK UI IS NOW AVAILABLE")
print("Open: http://localhost:4040")
print("=" * 60)

print("\nKeep this window running.")
print("Open Spark UI in your browser for screenshots.")
print("Press Ctrl+C here when finished.\n")

while True:
    time.sleep(10)