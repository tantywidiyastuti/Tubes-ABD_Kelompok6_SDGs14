import pandas as pd
import time

input_path = "/tmp/global_bleaching_environmental.csv"

start_time = time.time()

df = pd.read_csv(input_path, low_memory=False)

df["Percent_Bleaching"] = pd.to_numeric(df["Percent_Bleaching"], errors="coerce")

df_clean = df.drop_duplicates()
df_clean = df_clean[
    df_clean["Latitude_Degrees"].notna() &
    df_clean["Longitude_Degrees"].notna() &
    df_clean["Country_Name"].notna() &
    df_clean["Percent_Bleaching"].notna()
]

result = df_clean.groupby("Country_Name").agg(
    total_observation=("Country_Name", "count"),
    avg_percent_bleaching=("Percent_Bleaching", "mean")
).reset_index()

result["avg_percent_bleaching"] = result["avg_percent_bleaching"].round(2)

total_rows = len(df_clean)
result_rows = len(result)

end_time = time.time()

execution_time = end_time - start_time
throughput = total_rows / execution_time

print("=== HASIL EVALUASI PANDAS ===")
print(f"Total Rows        : {total_rows}")
print(f"Result Rows       : {result_rows}")
print(f"Execution Time    : {execution_time:.2f} detik")
print(f"Throughput        : {throughput:.2f} baris/detik")

print("=== CONTOH HASIL AGREGASI ===")
print(result.head(10))