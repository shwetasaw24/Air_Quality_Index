import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    count,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "air-quality"

CHECKPOINT_PATH = PROJECT_ROOT / "data" / "streaming" / "recovered-checkpoint"
OUTPUT_PATH = PROJECT_ROOT / "data" / "streaming" / "recovered-output"

# --------------------------------------------------
# Create directories
# --------------------------------------------------

CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


print("=" * 70)
print("AirStream - Spark Structured Streaming")
print("=" * 70)
print(f"Kafka       : {KAFKA_SERVER}")
print(f"Topic       : {KAFKA_TOPIC}")
print(f"Checkpoint  : {CHECKPOINT_PATH}")
print(f"Output      : {OUTPUT_PATH}")
print()


# --------------------------------------------------
# Spark configuration
# --------------------------------------------------

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


# Kafka connector dependencies downloaded by Spark/Ivy
IVY_JARS_DIR = Path.home() / ".ivy2.5.2" / "jars"

KAFKA_JAR_PATTERNS = [
    "org.apache.spark_spark-sql-kafka-0-10_2.13-4.2.0.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.2.0.jar",
    "org.apache.kafka_kafka-clients-3.9.2.jar",
    "at.yawk.lz4_lz4-java-1.11.0.jar",
    "org.apache.commons_commons-pool2-2.13.1.jar",
    "org.slf4j_slf4j-api-2.0.17.jar",
    "org.xerial.snappy_snappy-java-1.1.10.8.jar",
]

KAFKA_JARS = [IVY_JARS_DIR / name for name in KAFKA_JAR_PATTERNS]

for jar in KAFKA_JARS:
    if not jar.exists():
        raise FileNotFoundError(f"Required JAR not found: {jar}")

if not BARE_FS_JAR.exists():
    raise FileNotFoundError(f"Bare filesystem JAR not found: {BARE_FS_JAR}")

EXTRA_CLASSPATH = os.pathsep.join(
    [str(BARE_FS_JAR)] + [str(jar) for jar in KAFKA_JARS]
)

spark = (
    SparkSession.builder
    .appName("AirStream-Spark-Streaming")
    .master("local[*]")
    .config("spark.driver.extraClassPath", EXTRA_CLASSPATH)
    .config("spark.executor.extraClassPath", EXTRA_CLASSPATH)
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.fs.file.impl",
        "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem")
    .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager"
    )
    .getOrCreate()
)

# Preserve the Windows filesystem workaround

# Windows filesystem workaround for Spark and Structured Streaming
conf = spark.sparkContext._jsc.hadoopConfiguration()

# Use the Windows-compatible filesystem from the installed JAR
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.NakedLocalFileSystem"
)

# Do not set fs.AbstractFileSystem.file.impl to NakedLocalFileSystem
print("Configured filesystem:", conf.get("fs.file.impl"))
print(
    "Abstract filesystem:",
    conf.get("fs.AbstractFileSystem.file.impl")
)

print("Configured filesystem:", conf.get("fs.file.impl"))
print(
    "Configured abstract filesystem:",
    conf.get("fs.AbstractFileSystem.file.impl")
)

print(
    "Configured filesystem:",
    conf.get("fs.file.impl")
)


spark.sparkContext.setLogLevel("WARN")

# Windows filesystem workaround
# hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
# hadoop_conf.set(
#     "fs.file.impl",
#     "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
# )


# --------------------------------------------------
# Kafka event schema
# --------------------------------------------------

schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("units", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
])


# --------------------------------------------------
# Read Kafka stream
# --------------------------------------------------

print("Starting Kafka stream...")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)


# --------------------------------------------------
# Parse JSON messages
# --------------------------------------------------

events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# --------------------------------------------------
# Convert event timestamp
# --------------------------------------------------

events = events.withColumn(
    "event_time",
    col("datetime").cast(TimestampType())
)


# --------------------------------------------------
# Filter valid AQI pollutants
# --------------------------------------------------

events = events.filter(
    col("parameter").isin(
        "pm25",
        "pm10",
        "no2",
        "so2",
        "co",
        "o3",
    )
)

events = events.filter(
    col("value").isNotNull()
)


# --------------------------------------------------
# Windowed analytics
# --------------------------------------------------

windowed = (
    events
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(
            col("event_time"),
            "5 minutes"
        ),
        col("location_id"),
        col("location"),
        col("parameter"),
    )
    .agg(
        avg("value").alias("avg_value"),
        count("*").alias("measurement_count"),
    )
)


# --------------------------------------------------
# Flatten window columns
# --------------------------------------------------

result = (
    windowed
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("location_id"),
        col("location"),
        col("parameter"),
        col("avg_value"),
        col("measurement_count"),
    )
)


# --------------------------------------------------
# Write streaming output
# --------------------------------------------------

# --------------------------------------------------
# Write streaming output to Parquet
# --------------------------------------------------

result = result.withColumn(
    "date",
    to_date(col("window_start"))
)

query = (
    result.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(OUTPUT_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .partitionBy("date")
    .trigger(processingTime="10 seconds")
    .start()
)

print("Spark Structured Streaming is running.")
print("Waiting for Kafka events...")
print("Press Ctrl+C to stop.")
print()


try:
    query.awaitTermination()

except KeyboardInterrupt:
    print()
    print("Stopping Spark Structured Streaming...")

finally:
    query.stop()
    spark.stop()

    print("Streaming stopped.")