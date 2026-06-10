import pandas as pd

input_path = "/tmp/global_bleaching_environmental.csv"
output_path = "/tmp/global_bleaching_environmental_5x.csv"

df = pd.read_csv(input_path, low_memory=False)

df_big = pd.concat([df, df, df, df, df], ignore_index=True)

df_big.to_csv(output_path, index=False)

print("Dataset berhasil diperbesar")
print("Jumlah baris awal:", len(df))
print("Jumlah baris baru:", len(df_big))