import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round

spark = SparkSession.builder \
    .appName("Evaluasi Spark Coral Bleaching") \
    .getOrCreate()

input_path = "/datalake/bronze/coral_bleaching/global_bleaching_environmental.csv"

start_time = time.time()

df = spark.read.csv(input_path, header=True, inferSchema=True)

df_clean = df.dropDuplicates().filter(
    df["Latitude_Degrees"].isNotNull() &
    df["Longitude_Degrees"].isNotNull() &
    df["Country_Name"].isNotNull()
)

result = df_clean.groupBy("Country_Name") \
    .agg(
        count("*").alias("total_observation"),
        round(avg("Percent_Bleaching"), 2).alias("avg_percent_bleaching")
    )

total_rows = df_clean.count()
result_rows = result.count()

end_time = time.time()

execution_time = end_time - start_time
throughput = total_rows / execution_time

print("=== HASIL EVALUASI SPARK ===")
print(f"Total Rows        : {total_rows}")
print(f"Result Rows       : {result_rows}")
print(f"Execution Time    : {execution_time:.2f} detik")
print(f"Throughput        : {throughput:.2f} baris/detik")

spark.stop()