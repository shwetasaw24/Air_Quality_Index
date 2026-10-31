from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

spark = (
    SparkSession.builder
    .appName("AirStream-OpenAQ-Test")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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

input_file = (
    r"data\raw\openaq\historical\location_17"
    r"\2015\location-17-20150629.csv.gz"
)

print("=" * 60)
print("AirStream - Spark OpenAQ Test")
print("=" * 60)
print(f"Reading: {input_file}")

df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(input_file)
)

print("\nSchema:")
df.printSchema()

print("\nRecord count:")
print(df.count())

print("\nSample records:")
df.show(10, truncate=False)

print("\nParameters:")
df.groupBy("parameter").count().orderBy("parameter").show()

spark.stop()

print("\nTest completed successfully.")