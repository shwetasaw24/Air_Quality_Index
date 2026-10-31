import json
import time
from pathlib import Path

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "air-quality"

# Small test batch first
MAX_EVENTS = 500

# Delay between simulated sensor events
DELAY_SECONDS = 0.2

# Six pollutants used by our AQI pipeline
AQI_PARAMETERS = [
    "pm25",
    "pm10",
    "no2",
    "so2",
    "co",
    "o3",
]


# --------------------------------------------------
# Kafka Producer
# --------------------------------------------------

print("=" * 60)
print("AirStream - Kafka Stream Simulator")
print("=" * 60)

print(f"Silver data : {SILVER_PATH}")
print(f"Kafka server: {KAFKA_SERVER}")
print(f"Kafka topic : {KAFKA_TOPIC}")
print(f"Max events  : {MAX_EVENTS}")
print()


# --------------------------------------------------
# Create Spark session
# --------------------------------------------------

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)

spark = (
    SparkSession.builder
    .appName("AirStreamKafkaProducer")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Use the Bare Local FileSystem so Windows does not require winutils.exe
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)


# --------------------------------------------------
# Read Silver data
# --------------------------------------------------

print("Reading Silver Parquet...")

df = (
    spark.read
    .parquet(str(SILVER_PATH))
    .filter(col("parameter").isin(AQI_PARAMETERS))
    .select(
        "location_id",
        "location",
        "datetime",
        "parameter",
        "units",
        "value",
        "lat",
        "lon",
    )
    .orderBy("datetime")
    .limit(MAX_EVENTS)
)

records = df.collect()

print(f"Records selected: {len(records)}")
print()


# --------------------------------------------------
# Create Kafka producer
# --------------------------------------------------

print("Connecting to Kafka...")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

print("Kafka connection successful.")
print()


# --------------------------------------------------
# Send events
# --------------------------------------------------

print("Sending events...")
print("-" * 60)

for index, row in enumerate(records, start=1):

    event = {
        "location_id": int(row["location_id"]),
        "location": row["location"],
        "datetime": row["datetime"].isoformat(),
        "parameter": row["parameter"],
        "units": row["units"],
        "value": float(row["value"]),
        "lat": float(row["lat"]) if row["lat"] is not None else None,
        "lon": float(row["lon"]) if row["lon"] is not None else None,
    }

    future = producer.send(KAFKA_TOPIC, value=event)

    metadata = future.get(timeout=10)

    print(
        f"[{index:02d}] "
        f"{event['datetime']} | "
        f"station={event['location_id']} | "
        f"{event['parameter']}={event['value']} "
        f"{event['units']} | "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )

    time.sleep(DELAY_SECONDS)


# Make sure all buffered messages are delivered
producer.flush()
producer.close()

spark.stop()

print()
print("=" * 60)
print("Producer test completed successfully.")
print("=" * 60)