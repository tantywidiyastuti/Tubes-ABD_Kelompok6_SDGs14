from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, concat_ws, when, round

spark = SparkSession.builder \
    .appName("Bronze to Silver Coral Bleaching") \
    .getOrCreate()

input_path = "/datalake/bronze/coral_bleaching/global_bleaching_environmental.csv"
output_path = "/datalake/silver/coral_bleaching"

df = spark.read.csv(input_path, header=True, inferSchema=True)

selected_cols = [
    "Site_ID",
    "Sample_ID",
    "Data_Source",
    "Latitude_Degrees",
    "Longitude_Degrees",
    "Ocean_Name",
    "Country_Name",
    "Date_Year",
    "Date_Month",
    "Date_Day",
    "Depth_m",
    "Percent_Bleaching",
    "Bleaching_Level",
    "Temperature_Kelvin",
    "Temperature_Mean",
    "Temperature_Minimum",
    "Temperature_Maximum",
    "SSTA",
    "SSTA_DHW",
    "TSA",
    "TSA_DHW"
]

df = df.select(*selected_cols).dropDuplicates()

# Ubah nilai "nd" menjadi null, lalu cast ke numeric
numeric_cols = [
    "Depth_m",
    "Percent_Bleaching",
    "Temperature_Kelvin",
    "Temperature_Mean",
    "Temperature_Minimum",
    "Temperature_Maximum",
    "SSTA",
    "SSTA_DHW",
    "TSA",
    "TSA_DHW"
]

for c in numeric_cols:
    df = df.withColumn(
        c,
        when(col(c) == "nd", None).otherwise(col(c)).cast("double")
    )

df = df.withColumn(
    "Observation_Date",
    to_date(concat_ws("-", col("Date_Year"), col("Date_Month"), col("Date_Day")))
)

# Konversi Kelvin ke Celsius
df = df.withColumn(
    "Temperature_Celsius",
    round(col("Temperature_Mean") - 273.15, 2)
)

df = df.withColumn(
    "Bleaching_Category",
    when(col("Percent_Bleaching").isNull(), "Unknown")
    .when(col("Percent_Bleaching") == 0, "No Bleaching")
    .when(col("Percent_Bleaching") <= 25, "Low")
    .when(col("Percent_Bleaching") <= 50, "Medium")
    .otherwise("High")
)

df_clean = df.filter(
    col("Latitude_Degrees").isNotNull() &
    col("Longitude_Degrees").isNotNull() &
    col("Country_Name").isNotNull()
)

df_clean.write.mode("overwrite").parquet(output_path)

print("=== BRONZE TO SILVER SELESAI ===")
df_clean.printSchema()
df_clean.select(
    "Country_Name",
    "Temperature_Mean",
    "Temperature_Celsius",
    "Percent_Bleaching",
    "Bleaching_Category"
).show(20, truncate=False)

spark.stop()