
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    to_date,
    when,
    window,
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

CHECKPOINT_PATH = PROJECT_ROOT / "data" / "streaming" / "checkpoint"
OUTPUT_PATH = PROJECT_ROOT / "data" / "streaming" / "output"

CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Project-level anomaly thresholds.
# CO is assumed to be in µg/m³ when values are reported that way.
# These are demonstration thresholds, NOT official CPCB AQI breakpoints.
ANOMALY_THRESHOLDS = {
    "pm25": 60.0,
    "pm10": 100.0,
    "no2": 80.0,
    "so2": 80.0,
    "co": 4000.0,
    "o3": 100.0,
}

print("=" * 68)
print("AirStream - Spark Structured Streaming")
print("=" * 68)
print(f"Kafka      : {KAFKA_SERVER}")
print(f"Topic      : {KAFKA_TOPIC}")
print(f"Checkpoint : {CHECKPOINT_PATH}")
print(f"Output     : {OUTPUT_PATH}")
print("Window     : 5 minutes")
print("Trigger    : 10 seconds")
print()

# --------------------------------------------------
# Windows-compatible filesystem and Kafka JARs
# --------------------------------------------------

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)

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

for jar in [BARE_FS_JAR, *KAFKA_JARS]:
    if not jar.exists():
        raise FileNotFoundError(f"Required JAR not found: {jar}")

EXTRA_CLASSPATH = os.pathsep.join(
    str(path) for path in [BARE_FS_JAR, *KAFKA_JARS]
)

spark = (
    SparkSession.builder
    .appName("AirStream-Spark-Streaming")
    .master("local[2]")
    .config("spark.driver.extraClassPath", EXTRA_CLASSPATH)
    .config("spark.executor.extraClassPath", EXTRA_CLASSPATH)
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.hadoop.fs.file.impl",
        "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
    )
    .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
    )
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.NakedLocalFileSystem",
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Kafka message schema
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
# Read and parse Kafka events
# --------------------------------------------------

print("Connecting to Kafka and initializing stream...")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)

events = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("datetime").cast(TimestampType()))
    .filter(col("event_time").isNotNull())
    .filter(col("value").isNotNull())
    .filter(col("location_id").isNotNull())
    .filter(
        col("parameter").isin(
            "pm25", "pm10", "no2", "so2", "co", "o3"
        )
    )
)

# --------------------------------------------------
# Five-minute event-time window aggregation
# --------------------------------------------------

windowed = (
    events
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("location_id"),
        col("location"),
        col("parameter"),
    )
    .agg(
        avg("value").alias("avg_value"),
        spark_max("value").alias("max_value"),
        count("*").alias("measurement_count"),
    )
)

# --------------------------------------------------
# Threshold-based anomaly detection
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
        col("max_value"),
        col("measurement_count"),
    )
    .withColumn("date", to_date(col("window_start")))
    .withColumn(
        "anomaly_threshold",
        when(col("parameter") == "pm25", ANOMALY_THRESHOLDS["pm25"])
        .when(col("parameter") == "pm10", ANOMALY_THRESHOLDS["pm10"])
        .when(col("parameter") == "no2", ANOMALY_THRESHOLDS["no2"])
        .when(col("parameter") == "so2", ANOMALY_THRESHOLDS["so2"])
        .when(col("parameter") == "co", ANOMALY_THRESHOLDS["co"])
        .when(col("parameter") == "o3", ANOMALY_THRESHOLDS["o3"]),
    )
    .withColumn(
        "anomaly_flag",
        col("avg_value") > col("anomaly_threshold"),
    )
    .withColumn(
        "anomaly_reason",
        when(
            col("anomaly_flag"),
            "Window average exceeded project threshold",
        ).otherwise(""),
    )
)

# --------------------------------------------------
# Persist results to Parquet with checkpointing
# --------------------------------------------------

print("Starting persistent streaming query...")

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

print("AirStream streaming query is running.")
print("Writing window aggregates and anomaly flags to Parquet.")
print("Press Ctrl+C to stop.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping AirStream streaming query...")
finally:
    if query.isActive:
        query.stop()
    spark.stop()
    print("Streaming stopped.")
