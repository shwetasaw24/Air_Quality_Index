from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
BARE_FS_JAR = Path.home() / ".ivy2.5.2" / "jars" / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"

spark = (
    SparkSession.builder
    .appName("AirStream AQI Unit Check")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Use BareLocalFileSystem to avoid Windows winutils problems
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)

print("=" * 70)
print("STAGE 5.1 - AQI UNIT CHECK")
print("=" * 70)

df = spark.read.parquet(str(SILVER_PATH))

aqi_parameters = [
    "pm25",
    "pm10",
    "no2",
    "so2",
    "co",
    "o3",
]

print("\nAQI pollutant unit distribution:\n")

(
    df
    .filter(col("parameter").isin(aqi_parameters))
    .groupBy("parameter", "units")
    .agg(count("*").alias("records"))
    .orderBy("parameter", "units")
    .show(100, truncate=False)
)

print("\nTotal AQI-related records:")

(
    df
    .filter(col("parameter").isin(aqi_parameters))
    .count()
)

total = (
    df
    .filter(col("parameter").isin(aqi_parameters))
    .count()
)

print(total)

print("\n" + "=" * 70)
print("UNIT CHECK COMPLETE")
print("=" * 70)

spark.stop()