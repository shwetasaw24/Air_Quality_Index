from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    when,
    count
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

SILVER_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "silver"
    / "openaq_measurements"
)

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
    .appName("AirStream-Silver-Cleaning")
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


print("=" * 70)
print("AirStream - Silver Cleaning")
print("=" * 70)

print(f"Bronze: {BRONZE_DIR}")
print(f"Silver: {SILVER_DIR}")
print(f"Filesystem: {conf.get('fs.file.impl')}")
print("=" * 70)


# ============================================================
# LOAD BRONZE
# ============================================================

print("\nLoading Bronze Parquet...")

df = spark.read.parquet(str(BRONZE_DIR))

bronze_count = df.count()

print(f"Bronze records: {bronze_count:,}")


# ============================================================
# STEP 1 — NORMALIZE PARAMETERS
# ============================================================

print("\nStep 1: Normalizing parameter names...")

df_clean = df.withColumn(
    "parameter",
    lower(trim(col("parameter")))
)


# ============================================================
# STEP 2 — REMOVE NULL / INVALID CORE RECORDS
# ============================================================

print("Step 2: Removing invalid core records...")

df_clean = df_clean.filter(
    col("location_id").isNotNull()
    & col("sensors_id").isNotNull()
    & col("datetime").isNotNull()
    & col("parameter").isNotNull()
    & col("value").isNotNull()
    & col("lat").isNotNull()
    & col("lon").isNotNull()
)


# ============================================================
# STEP 3 — PARAMETER-SPECIFIC VALIDATION
# ============================================================

print("Step 3: Applying parameter-specific validity rules...")

valid_measurement = (
    # Pollutant concentrations cannot be negative
    (
        col("parameter").isin(
            "pm25",
            "pm10",
            "no2",
            "no",
            "nox",
            "so2",
            "o3",
            "co"
        )
        & (col("value") >= 0)
        & (col("value") <= 100000)
    )

    |

    # Temperature: broad physical range in Celsius
    (
        (col("parameter") == "temperature")
        & (col("value") >= -80)
        & (col("value") <= 80)
    )

    |

    # Relative humidity: 0–100%
    (
        (col("parameter") == "relativehumidity")
        & (col("value") >= 0)
        & (col("value") <= 100)
    )

    |

    # Wind speed: non-negative and reasonable
    (
        (col("parameter") == "wind_speed")
        & (col("value") >= 0)
        & (col("value") <= 100)
    )

    |

    # Wind direction: 0–360 degrees
    (
        (col("parameter") == "wind_direction")
        & (col("value") >= 0)
        & (col("value") <= 360)
    )
)

df_valid = df_clean.filter(valid_measurement)


# ============================================================
# STEP 4 — DEDUPLICATION
# ============================================================

print("Step 4: Removing duplicate measurements...")

dedup_columns = [
    "location_id",
    "sensors_id",
    "datetime",
    "parameter"
]

before_dedup = df_valid.count()

df_silver = df_valid.dropDuplicates(dedup_columns)

after_dedup = df_silver.count()

print(f"Records before deduplication: {before_dedup:,}")
print(f"Records after deduplication : {after_dedup:,}")
print(f"Duplicate records removed   : {before_dedup - after_dedup:,}")


# ============================================================
# STEP 5 — ADD TIME FEATURES
# ============================================================

print("\nStep 5: Adding time features...")

from pyspark.sql.functions import (
    hour,
    dayofmonth,
    dayofweek,
    weekofyear,
    date_format
)

df_silver = (
    df_silver
    .withColumn("hour", hour(col("datetime")))
    .withColumn("day", dayofmonth(col("datetime")))
    .withColumn("day_of_week", dayofweek(col("datetime")))
    .withColumn("week", weekofyear(col("datetime")))
    .withColumn("date", date_format(col("datetime"), "yyyy-MM-dd"))
)


# ============================================================
# STEP 6 — QUALITY FLAG
# ============================================================

print("Step 6: Creating quality flag...")

df_silver = df_silver.withColumn(
    "quality_flag",
    when(
        col("value").isNotNull(),
        "valid"
    ).otherwise(
        "invalid"
    )
)


# ============================================================
# FINAL COUNTS
# ============================================================

silver_count = df_silver.count()

print("\n" + "=" * 70)
print("SILVER SUMMARY")
print("=" * 70)

print(f"Bronze records : {bronze_count:,}")
print(f"Silver records : {silver_count:,}")
print(f"Records removed: {bronze_count - silver_count:,}")


# ============================================================
# PARAMETER DISTRIBUTION
# ============================================================

print("\nSilver parameter distribution:")

(
    df_silver
    .groupBy("parameter")
    .count()
    .orderBy(col("count").desc())
    .show(50, truncate=False)
)


# ============================================================
# VALIDATION
# ============================================================

print("\nChecking remaining invalid measurements...")

invalid_remaining = df_silver.filter(
    ~valid_measurement
).count()

print(f"Invalid measurements remaining: {invalid_remaining:,}")


# ============================================================
# WRITE SILVER
# ============================================================

print("\nWriting Silver Parquet...")
print("This may take several minutes.")

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(str(SILVER_DIR))
)

print("\n" + "=" * 70)
print("SILVER WRITE SUCCESS")
print("=" * 70)


# ============================================================
# READ BACK
# ============================================================

print("\nReading Silver Parquet back...")

silver_check = spark.read.parquet(str(SILVER_DIR))

silver_check_count = silver_check.count()

print(f"Silver records read back: {silver_check_count:,}")


# ============================================================
# FINAL VALIDATION
# ============================================================

if silver_check_count != silver_count:
    raise RuntimeError(
        f"Silver count mismatch! "
        f"Expected {silver_count:,}, "
        f"got {silver_check_count:,}"
    )

print("\n" + "=" * 70)
print("SILVER CLEANING COMPLETE")
print("=" * 70)

print(f"Bronze records : {bronze_count:,}")
print(f"Silver records : {silver_check_count:,}")
print(f"Removed        : {bronze_count - silver_check_count:,}")
print("Validation     : PASSED")
print("=" * 70)


spark.stop()