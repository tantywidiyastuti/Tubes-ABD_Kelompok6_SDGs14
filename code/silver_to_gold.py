from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, max, min, round

spark = SparkSession.builder \
    .appName("Silver to Gold Coral Bleaching") \
    .getOrCreate()

silver_path = "/datalake/silver/coral_bleaching"
gold_path = "/datalake/gold/coral_bleaching"

df = spark.read.parquet(silver_path)

gold_country = df.groupBy("Country_Name") \
    .agg(
        count("*").alias("total_observation"),
        round(avg("Percent_Bleaching"), 2).alias("avg_percent_bleaching"),
        round(avg("Temperature_Celsius"), 2).alias("avg_temperature"),
        max("Percent_Bleaching").alias("max_bleaching"),
        min("Percent_Bleaching").alias("min_bleaching")
    ) \
    .orderBy("total_observation", ascending=False)

gold_ocean = df.groupBy("Ocean_Name") \
    .agg(
        count("*").alias("total_observation"),
        round(avg("Percent_Bleaching"), 2).alias("avg_percent_bleaching"),
        round(avg("SSTA_DHW"), 2).alias("avg_ssta_dhw"),
        round(avg("TSA_DHW"), 2).alias("avg_tsa_dhw")
    ) \
    .orderBy("total_observation", ascending=False)

gold_category = df.groupBy("Bleaching_Category") \
    .agg(
        count("*").alias("total_observation"),
        round(avg("Percent_Bleaching"), 2).alias("avg_percent_bleaching")
    ) \
    .orderBy("total_observation", ascending=False)

gold_year = df.groupBy("Date_Year") \
    .agg(
        count("*").alias("total_observation"),
        round(avg("Percent_Bleaching"), 2).alias("avg_percent_bleaching"),
        round(avg("Temperature_Celsius"), 2).alias("avg_temperature")
    ) \
    .orderBy("Date_Year")

gold_country.write.mode("overwrite").parquet(gold_path + "/gold_country")
gold_ocean.write.mode("overwrite").parquet(gold_path + "/gold_ocean")
gold_category.write.mode("overwrite").parquet(gold_path + "/gold_category")
gold_year.write.mode("overwrite").parquet(gold_path + "/gold_year")

print("=== GOLD COUNTRY ===")
gold_country.show(20, truncate=False)

spark.stop()