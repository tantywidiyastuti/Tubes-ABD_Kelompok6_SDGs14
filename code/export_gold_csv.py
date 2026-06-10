from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Export Gold to CSV") \
    .getOrCreate()

gold_base = "/datalake/gold/coral_bleaching"
output_base = "/datalake/gold/coral_bleaching_csv"

datasets = ["gold_country", "gold_ocean", "gold_category", "gold_year"]

for name in datasets:
    df = spark.read.parquet(f"{gold_base}/{name}")
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{output_base}/{name}")
    print(f"Export selesai: {name}")

spark.stop()