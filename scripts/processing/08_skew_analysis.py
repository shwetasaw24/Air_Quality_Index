
from pathlib import Path
import csv
import math
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.functions import spark_partition_id



# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path.cwd()

SILVER_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "openaq_measurements"
)

REPORT_DIR = PROJECT_ROOT / "reports" / "skew_analysis"

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)

# A modest target for this local-machine experiment.
BALANCED_PARTITIONS = 8


# ============================================================
# 2. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirStream-Data-Skew-Analysis")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)


# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================



def get_partition_counts(dataframe):
    """Measure Spark partition sizes using Spark SQL."""

    rows = (
        dataframe
        .withColumn("_partition_id", spark_partition_id())
        .groupBy("_partition_id")
        .count()
        .orderBy("_partition_id")
        .collect()
    )

    return [row["count"] for row in rows]


def distribution_stats(counts):
    """Calculate basic partition distribution statistics."""

    if not counts:
        return {
            "partitions": 0,
            "total_records": 0,
            "min_records": 0,
            "max_records": 0,
            "average_records": 0,
            "max_min_ratio": 0,
            "coefficient_of_variation": 0,
        }

    total = sum(counts)
    average = total / len(counts)
    minimum = min(counts)
    maximum = max(counts)

    variance = sum(
        (value - average) ** 2 for value in counts
    ) / len(counts)

    stddev = math.sqrt(variance)

    return {
        "partitions": len(counts),
        "total_records": total,
        "min_records": minimum,
        "max_records": maximum,
        "average_records": round(average, 2),
        "max_min_ratio": (
            round(maximum / minimum, 2)
            if minimum > 0 else "undefined"
        ),
        "coefficient_of_variation": (
            round(stddev / average, 4)
            if average > 0 else 0
        ),
    }


def measure_partitions(dataframe, label):
    """Measure and display Spark partition distribution."""

    print("\n" + "=" * 70)
    print(label)
    print("=" * 70)

    counts = get_partition_counts(dataframe)
    stats = distribution_stats(counts)

    for index, records in enumerate(counts):
        print(f"Partition {index}: {records:,} records")

    print("\nSummary:")
    for key, value in stats.items():
        print(f"{key}: {value}")

    return counts, stats


# ============================================================
# 4. LOAD SILVER DATA
# ============================================================

print("=" * 70)
print("AirStream - Data Skew and Partition Balancing Experiment")
print("=" * 70)

print(f"\nSilver dataset: {SILVER_DIR}")

if not SILVER_DIR.exists():
    raise FileNotFoundError(
        f"Silver dataset not found: {SILVER_DIR}"
    )

df = spark.read.parquet(str(SILVER_DIR))

print("\nDataset schema:")
df.printSchema()

print("\nTotal records:")
total_records = df.count()
print(f"{total_records:,}")


# ============================================================
# 5. STATION-LEVEL DATA DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("STATION-LEVEL DATA DISTRIBUTION")
print("=" * 70)

station_distribution = (
    df.groupBy("location_id")
    .agg(count("*").alias("records"))
    .orderBy(col("records").desc())
)

station_distribution.show(20, truncate=False)

station_counts = [
    row["records"]
    for row in station_distribution.collect()
]

station_stats = distribution_stats(station_counts)

print("\nStation distribution summary:")
for key, value in station_stats.items():
    print(f"{key}: {value}")


# ============================================================
# 6. ORIGINAL SPARK PARTITION DISTRIBUTION
# ============================================================

original_counts, original_stats = measure_partitions(
    df,
    "ORIGINAL SPARK PARTITION DISTRIBUTION"
)


# ============================================================
# 7. REPARTITION THE DATA
# ============================================================

print("\n" + "=" * 70)
print("APPLYING REPARTITIONING")
print("=" * 70)

print(
    f"Redistributing records into "
    f"{BALANCED_PARTITIONS} Spark partitions..."
)

start_time = time.perf_counter()

balanced_df = df.repartition(BALANCED_PARTITIONS)

# Materialize the shuffle so that its cost is included.
balanced_record_count = balanced_df.count()

repartition_time = time.perf_counter() - start_time

print(f"Records after repartition: {balanced_record_count:,}")
print(f"Repartition + count time: {repartition_time:.2f} seconds")


# ============================================================
# 8. BALANCED SPARK PARTITION DISTRIBUTION
# ============================================================

balanced_counts, balanced_stats = measure_partitions(
    balanced_df,
    "AFTER REPARTITIONING"
)


# ============================================================
# 9. VERIFY RECORD COUNTS
# ============================================================

print("\n" + "=" * 70)
print("RECORD COUNT VERIFICATION")
print("=" * 70)

print(f"Original records:  {total_records:,}")
print(f"Repartitioned:     {balanced_record_count:,}")

if total_records == balanced_record_count:
    print("PASS: No records were lost during repartitioning.")
else:
    print("WARNING: Record counts do not match.")


# ============================================================
# 10. SAVE EXPERIMENT REPORT
# ============================================================

REPORT_DIR.mkdir(parents=True, exist_ok=True)

csv_path = REPORT_DIR / "partition_comparison.csv"

with open(
    csv_path,
    "w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow([
        "stage",
        "partition_id",
        "record_count"
    ])

    for index, records in enumerate(original_counts):
        writer.writerow([
            "original",
            index,
            records
        ])

    for index, records in enumerate(balanced_counts):
        writer.writerow([
            "repartitioned",
            index,
            records
        ])


summary_path = REPORT_DIR / "skew_summary.txt"

with open(
    summary_path,
    "w",
    encoding="utf-8"
) as file:

    file.write("AIRSTREAM DATA SKEW EXPERIMENT\n")
    file.write("=" * 50 + "\n\n")

    file.write(f"Total records: {total_records:,}\n")
    file.write(f"Station count: {len(station_counts):,}\n")
    file.write(f"Repartition target: {BALANCED_PARTITIONS}\n")
    file.write(f"Repartition + count time: {repartition_time:.2f} seconds\n\n")

    file.write("ORIGINAL PARTITIONS\n")
    for key, value in original_stats.items():
        file.write(f"{key}: {value}\n")

    file.write("\nREPARTITIONED PARTITIONS\n")
    for key, value in balanced_stats.items():
        file.write(f"{key}: {value}\n")

    file.write("\nSTATION DISTRIBUTION\n")
    for key, value in station_stats.items():
        file.write(f"{key}: {value}\n")


# ============================================================
# 11. FINAL SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("EXPERIMENT COMPLETE")
print("=" * 70)

print(f"CSV report: {csv_path}")
print(f"Summary:    {summary_path}")

print("\nOriginal partition statistics:")
print(original_stats)

print("\nRepartitioned statistics:")
print(balanced_stats)

print("\nNo existing Silver, Gold, AQI or streaming files were modified.")


# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()
