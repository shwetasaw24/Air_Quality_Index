from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    when,
    avg,
    count,
    min,
    max,
    to_date,
    hour,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "processed" / "silver"
AQI_OUTPUT = PROJECT_ROOT / "data" / "processed" / "aqi"


# Bare Local FS JAR used by this project on Windows
BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


spark = (
    SparkSession.builder
    .appName("AirStream AQI Preparation")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Avoid Windows Hadoop winutils dependency
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)


print("=" * 70)
print("STAGE 5.2 - AQI PREPARATION")
print("=" * 70)


# ------------------------------------------------------------
# 1. Load Silver data
# ------------------------------------------------------------

df = spark.read.parquet(str(SILVER_PATH))

print("\nSilver records:")
print(df.count())


# ------------------------------------------------------------
# 2. Select pollutants used by CPCB AQI
# ------------------------------------------------------------

aqi_parameters = [
    "pm25",
    "pm10",
    "no2",
    "so2",
    "co",
    "o3",
]

aqi_df = (
    df
    .withColumn("parameter", lower(trim(col("parameter"))))
    .withColumn("units", lower(trim(col("units"))))
    .filter(col("parameter").isin(aqi_parameters))
)


print("\nAQI pollutant records:")
print(aqi_df.count())


# ------------------------------------------------------------
# 3. Normalize units
#
# Target units:
#
# PM2.5 -> µg/m³
# PM10  -> µg/m³
# NO2   -> µg/m³
# SO2   -> µg/m³
# CO    -> mg/m³
# O3    -> µg/m³
#
# For now we retain only records already reported in the
# required concentration unit.
#
# This avoids introducing assumptions about temperature,
# pressure, or molecular-weight conversion for ppb/ppm data.
# ------------------------------------------------------------

normalized = (
    aqi_df
    .withColumn(
        "aqi_value",
        when(
            (col("parameter") == "pm25") &
            (col("units") == "µg/m³"),
            col("value")
        )
        .when(
            (col("parameter") == "pm10") &
            (col("units") == "µg/m³"),
            col("value")
        )
        .when(
            (col("parameter") == "no2") &
            (col("units") == "µg/m³"),
            col("value")
        )
        .when(
            (col("parameter") == "so2") &
            (col("units") == "µg/m³"),
            col("value")
        )
        .when(
            (col("parameter") == "o3") &
            (col("units") == "µg/m³"),
            col("value")
        )
        # CO is converted from µg/m³ to mg/m³
        .when(
            (col("parameter") == "co") &
            (col("units") == "µg/m³"),
            col("value") / 1000.0
        )
    )
)


# ------------------------------------------------------------
# 4. Keep only successfully normalized records
# ------------------------------------------------------------

# ------------------------------------------------------------
# 4. Validate AQI concentrations
#
# Remove negative values and measurements outside the
# concentration range used by the CPCB AQI framework.
# ------------------------------------------------------------

normalized = (
    normalized
    .filter(col("aqi_value").isNotNull())
    .filter(col("aqi_value") >= 0)
    .filter(
        (
            ((col("parameter") == "pm25") & (col("aqi_value") <= 500))
            |
            ((col("parameter") == "pm10") & (col("aqi_value") <= 500))
            |
            ((col("parameter") == "no2") & (col("aqi_value") <= 800))
            |
            ((col("parameter") == "so2") & (col("aqi_value") <= 2620))
            |
            ((col("parameter") == "co") & (col("aqi_value") <= 50))
            |
            ((col("parameter") == "o3") & (col("aqi_value") <= 1000))
        )
    )
)

# ------------------------------------------------------------
# 5. Add date/time fields
# ------------------------------------------------------------

normalized = (
    normalized
    .withColumn("date", to_date(col("datetime")))
    .withColumn("hour", hour(col("datetime")))
)


# ------------------------------------------------------------
# 6. Show normalized unit distribution
# ------------------------------------------------------------

print("\nNormalized AQI data:")

(
    normalized
    .groupBy("parameter")
    .agg(
        count("*").alias("records"),
        min("aqi_value").alias("min_value"),
        max("aqi_value").alias("max_value"),
        avg("aqi_value").alias("avg_value"),
    )
    .orderBy("parameter")
    .show(20, truncate=False)
)


# ------------------------------------------------------------
# 7. Create daily concentrations
#
# CPCB AQI mainly uses daily pollutant concentrations.
#
# We calculate daily averages per station and pollutant.
# ------------------------------------------------------------

daily = (
    normalized
    .groupBy(
        "location_id",
        "location",
        "date",
        "parameter",
    )
    .agg(
        avg("aqi_value").alias("concentration"),
        min("aqi_value").alias("min_concentration"),
        max("aqi_value").alias("max_concentration"),
        count("*").alias("measurement_count"),
    )
)


# ------------------------------------------------------------
# 8. Save AQI-ready daily data
# ------------------------------------------------------------

print("\nAQI daily records:")
print(daily.count())


print("\nWriting AQI data...")

(
    daily
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(str(AQI_OUTPUT))
)

print("AQI preparation write completed.")


# ------------------------------------------------------------
# 9. Read back and validate
# ------------------------------------------------------------

check = spark.read.parquet(str(AQI_OUTPUT))

print("\nAQI records after read-back:")
print(check.count())

print("\nAQI schema:")

check.printSchema()


print("\nSample AQI-ready records:")

(
    check
    .orderBy("date", "location_id", "parameter")
    .show(20, truncate=False)
)


print("\n" + "=" * 70)
print("STAGE 5.2 COMPLETE")
print("=" * 70)


spark.stop()