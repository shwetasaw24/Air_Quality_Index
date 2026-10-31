from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

root = Path.cwd()
output_path = root / "data" / "streaming" / "output"

spark = (
    SparkSession.builder
    .appName("AirStream-Validate-Output")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.jars", str(
        Path.home() / ".ivy2.5.2" / "jars" /
        "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar"
    ))
    .config(
        "spark.driver.extraClassPath",
        str(Path.home() / ".ivy2.5.2" / "jars" /
            "com.sparkutils_hadoop-bare-naked-local-fs-0.2.0.jar")
    )
    .config(
        "spark.hadoop.fs.file.impl",
        "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(str(output_path))

    print("\n=== STREAMING OUTPUT SCHEMA ===")
    df.printSchema()

    total = df.count()
    print(f"\nTotal saved window records: {total}")

    print("\n=== SAMPLE RECORDS ===")
    df.show(10, truncate=False)

    print("\n=== ANOMALY VALIDATION ===")
    if "anomaly_flag" in df.columns:
        df.groupBy("anomaly_flag").count().show()
        df.filter(col("anomaly_flag") == True).show(10, truncate=False)
    else:
        print("anomaly_flag column is missing.")

    print("\n=== REQUIRED COLUMN CHECK ===")
    required = {
        "window_start", "window_end", "location_id",
        "parameter", "avg_value", "measurement_count"
    }
    print("Missing columns:", sorted(required - set(df.columns)))

finally:
    spark.stop()
