from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    max as spark_max,
    first,
    lit,
    greatest,
    round as spark_round,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

AQI_INPUT = PROJECT_ROOT / "data" / "processed" / "aqi"
AQI_OUTPUT = PROJECT_ROOT / "data" / "processed" / "aqi_final"

BARE_FS_JAR = (
    Path.home()
    / ".ivy2.5.2"
    / "jars"
    / "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
)


spark = (
    SparkSession.builder
    .appName("AirStream CPCB AQI Calculation")
    .master("local[*]")
    .config("spark.driver.extraClassPath", str(BARE_FS_JAR))
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set(
    "fs.file.impl",
    "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
)

print("=" * 70)
print("STAGE 5.3 - CPCB AQI CALCULATION")
print("=" * 70)


# ------------------------------------------------------------
# 1. Load AQI-ready daily data
# ------------------------------------------------------------

df = spark.read.parquet(str(AQI_INPUT))

print("\nAQI preparation records:")
print(df.count())


# ------------------------------------------------------------
# 2. CPCB breakpoint function
#
# Each tuple:
# concentration_low,
# concentration_high,
# AQI_low,
# AQI_high
#
# CPCB AQI = linear interpolation between breakpoints.
# ------------------------------------------------------------

def sub_index_expression(parameter, value_column):

    if parameter == "pm25":
        breakpoints = [
            (0, 30, 0, 50),
            (30, 60, 51, 100),
            (60, 90, 101, 200),
            (90, 120, 201, 300),
            (120, 250, 301, 400),
            (250, 500, 401, 500),
        ]

    elif parameter == "pm10":
        breakpoints = [
            (0, 50, 0, 50),
            (50, 100, 51, 100),
            (100, 250, 101, 200),
            (250, 350, 201, 300),
            (350, 430, 301, 400),
            (430, 500, 401, 500),
        ]

    elif parameter == "no2":
        breakpoints = [
            (0, 40, 0, 50),
            (40, 80, 51, 100),
            (80, 180, 101, 200),
            (180, 280, 201, 300),
            (280, 400, 301, 400),
            (400, 800, 401, 500),
        ]

    elif parameter == "so2":
        breakpoints = [
            (0, 40, 0, 50),
            (40, 80, 51, 100),
            (80, 380, 101, 200),
            (380, 800, 201, 300),
            (800, 1600, 301, 400),
            (1600, 2620, 401, 500),
        ]

    elif parameter == "co":
        breakpoints = [
            (0, 1, 0, 50),
            (1, 2, 51, 100),
            (2, 10, 101, 200),
            (10, 17, 201, 300),
            (17, 34, 301, 400),
            (34, 50, 401, 500),
        ]

    elif parameter == "o3":
        breakpoints = [
            (0, 50, 0, 50),
            (50, 100, 51, 100),
            (100, 168, 101, 200),
            (168, 208, 201, 300),
            (208, 748, 301, 400),
            (748, 1000, 401, 500),
        ]

    else:
        return lit(None)

    # Build the complete CASE expression in one chain.
    expression = None

    for c_low, c_high, i_low, i_high in breakpoints:

        current = (
            (lit(i_high - i_low) / lit(c_high - c_low))
            * (value_column - lit(c_low))
            + lit(i_low)
        )

        condition = (
            (value_column >= c_low)
            & (value_column <= c_high)
        )

        if expression is None:
            expression = when(condition, current)
        else:
            expression = expression.when(condition, current)

    return expression

# ------------------------------------------------------------
# 3. Calculate pollutant sub-index
# ------------------------------------------------------------

df = (
    df
    .withColumn(
        "sub_index",
        when(
            col("parameter") == "pm25",
            sub_index_expression("pm25", col("concentration"))
        )
        .when(
            col("parameter") == "pm10",
            sub_index_expression("pm10", col("concentration"))
        )
        .when(
            col("parameter") == "no2",
            sub_index_expression("no2", col("concentration"))
        )
        .when(
            col("parameter") == "so2",
            sub_index_expression("so2", col("concentration"))
        )
        .when(
            col("parameter") == "co",
            sub_index_expression("co", col("concentration"))
        )
        .when(
            col("parameter") == "o3",
            sub_index_expression("o3", col("concentration"))
        )
    )
)


# ------------------------------------------------------------
# 4. Round pollutant sub-indices
# ------------------------------------------------------------

df = df.withColumn(
    "sub_index",
    spark_round(col("sub_index"), 2)
)


print("\nSample pollutant sub-indices:")

(
    df
    .select(
        "location_id",
        "location",
        "date",
        "parameter",
        "concentration",
        "sub_index",
    )
    .orderBy("date", "location_id", "parameter")
    .show(30, truncate=False)
)


# ------------------------------------------------------------
# 5. Pivot pollutants into one row per station/day
# ------------------------------------------------------------

pivoted = (
    df
    .groupBy(
        "location_id",
        "location",
        "date",
    )
    .pivot("parameter", [
        "pm25",
        "pm10",
        "no2",
        "so2",
        "co",
        "o3",
    ])
    .agg(first("sub_index"))
)


pivoted = (
    pivoted
    .withColumnRenamed("pm25", "pm25_aqi")
    .withColumnRenamed("pm10", "pm10_aqi")
    .withColumnRenamed("no2", "no2_aqi")
    .withColumnRenamed("so2", "so2_aqi")
    .withColumnRenamed("co", "co_aqi")
    .withColumnRenamed("o3", "o3_aqi")
)


# ------------------------------------------------------------
# 6. Overall AQI
#
# CPCB uses the maximum pollutant sub-index.
# ------------------------------------------------------------

pivoted = pivoted.withColumn(
    "aqi",
    greatest(
        col("pm25_aqi"),
        col("pm10_aqi"),
        col("no2_aqi"),
        col("so2_aqi"),
        col("co_aqi"),
        col("o3_aqi"),
    )
)


# ------------------------------------------------------------
# 7. Determine dominant pollutant
# ------------------------------------------------------------

pivoted = pivoted.withColumn(
    "dominant_pollutant",
    when(col("aqi") == col("pm25_aqi"), "PM2.5")
    .when(col("aqi") == col("pm10_aqi"), "PM10")
    .when(col("aqi") == col("no2_aqi"), "NO2")
    .when(col("aqi") == col("so2_aqi"), "SO2")
    .when(col("aqi") == col("co_aqi"), "CO")
    .when(col("aqi") == col("o3_aqi"), "O3")
)


# ------------------------------------------------------------
# 8. CPCB AQI category
# ------------------------------------------------------------

pivoted = pivoted.withColumn(
    "category",
    when(col("aqi") <= 50, "Good")
    .when(col("aqi") <= 100, "Satisfactory")
    .when(col("aqi") <= 200, "Moderate")
    .when(col("aqi") <= 300, "Poor")
    .when(col("aqi") <= 400, "Very Poor")
    .otherwise("Severe")
)


# ------------------------------------------------------------
# 9. Round AQI
# ------------------------------------------------------------

pivoted = pivoted.withColumn(
    "aqi",
    spark_round(col("aqi"), 0)
)


# ------------------------------------------------------------
# 10. Show results
# ------------------------------------------------------------

print("\nFinal AQI sample:")

(
    pivoted
    .select(
        "location_id",
        "location",
        "date",
        "pm25_aqi",
        "pm10_aqi",
        "no2_aqi",
        "so2_aqi",
        "co_aqi",
        "o3_aqi",
        "aqi",
        "dominant_pollutant",
        "category",
    )
    .orderBy("date", "location_id")
    .show(30, truncate=False)
)


# ------------------------------------------------------------
# 11. AQI category distribution
# ------------------------------------------------------------

print("\nAQI category distribution:")

(
    pivoted
    .groupBy("category")
    .count()
    .orderBy("category")
    .show()
)


# ------------------------------------------------------------
# 12. Save final AQI dataset
# ------------------------------------------------------------

print("\nFinal AQI records:")
print(pivoted.count())


print("\nWriting final AQI dataset...")

(
    pivoted
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(str(AQI_OUTPUT))
)

print("Final AQI write completed.")


# ------------------------------------------------------------
# 13. Read-back validation
# ------------------------------------------------------------

check = spark.read.parquet(str(AQI_OUTPUT))

print("\nFinal AQI records after read-back:")
print(check.count())

print("\nFinal AQI schema:")
check.printSchema()


print("\nFinal AQI validation sample:")

(
    check
    .select(
        "location_id",
        "location",
        "date",
        "aqi",
        "dominant_pollutant",
        "category",
    )
    .orderBy("date", "location_id")
    .show(20, truncate=False)
)


print("\n" + "=" * 70)
print("STAGE 5.3 COMPLETE")
print("=" * 70)

spark.stop()