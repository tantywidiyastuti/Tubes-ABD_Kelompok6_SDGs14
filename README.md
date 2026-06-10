# 🌊 Tubes ABD Kelompok 6 - SDGs 14: Life Below Water
## Analisis Big Data: Global Coral Bleaching & Environmental Factors

### 📌 Deskripsi Proyek
Proyek ini merupakan bagian dari Tugas Besar (Tubes) mata kuliah **Analisis Big Data (ABD)** oleh **Kelompok 6**. Proyek ini berfokus pada analisis data global pemutihan terumbu karang (*coral bleaching*) dan faktor lingkungan yang mempengaruhinya, sejalan dengan **Tujuan Pembangunan Berkelanjutan (SDGs) 14: Life Below Water** (Ekosistem Laut).

Tujuan utama dari proyek ini adalah menjawab pertanyaan ilmiah:
> **"Apakah implementasi pipeline Big Data berbasis Apache Spark dengan Medallion Architecture mampu meningkatkan efisiensi pemrosesan data coral bleaching dibandingkan pemrosesan konvensional menggunakan Pandas?"**

#### 🎯 Tujuan Proyek
1. **Tujuan Utama**: Membandingkan efisiensi pemrosesan data *coral bleaching* skala besar antara Apache Spark (pemrosesan paralel terdistribusi) dan Pandas (pemrosesan sekuensial *single-machine*).
2. **Tujuan Sekunder**:
   * Mengimplementasikan Hadoop Distributed File System (HDFS) sebagai media penyimpanan data terdistribusi.
   * Mengukur tingkat skalabilitas (*scalability*) Apache Spark terhadap peningkatan ukuran data (melalui pengujian kelipatan dataset).
   * Mengevaluasi performa sistem dari aspek kecepatan pemrosesan (*throughput*) dan waktu respon (*latency*).

---

### 📂 Struktur Direktori
Berikut adalah struktur folder dan file pada repositori ini:
*   📄 [Proposal TUBES ABD (1).docx](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/Proposal%20TUBES%20ABD%20(1).docx) : Dokumen proposal lengkap tugas besar Kelompok 6.
*   📁 **`code/`** : Berisi skrip Python untuk pipeline data (ETL) dan evaluasi performa.
    *   [bronze_to_silver.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/bronze_to_silver.py) : Skrip PySpark untuk pembersihan dan standardisasi data mentah (*Bronze*) menjadi terstruktur (*Silver*).
    *   [silver_to_gold.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/silver_to_gold.py) : Skrip PySpark untuk agregasi data *Silver* menjadi metrik analitik (*Gold*).
    *   [export_gold_csv.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/export_gold_csv.py) : Skrip PySpark untuk mengekspor data *Gold* format Parquet menjadi CSV.
    *   [evaluasi_pandas.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/evaluasi_pandas.py) : Skrip pengujian performa pembersihan dan agregasi data baseline menggunakan Pandas.
    *   [evaluasi_spark.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/evaluasi_spark.py) : Skrip pengujian performa pembersihan dan agregasi data menggunakan Apache Spark.
    *   [perbesar_dataset.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/perbesar_dataset.py) : Skrip pembantu untuk melipatgandakan data (5x) guna mensimulasikan volume data yang lebih besar pada stress test.
*   📁 **`dashboard/`** : Dokumen visualisasi analitis.
    *   `Dashboard Tubes ABD.pbix` : File visualisasi interaktif Power BI Desktop.
    *   [Dashboard Tubes ABD.pdf](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/dashboard/Dashboard%20Tubes%20ABD.pdf) : Ekspor laporan visualisasi dashboard dalam format PDF.
*   📁 **`data/`** : Sumber data mentah.
    *   [global_bleaching_environmental.csv](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/data/global_bleaching_environmental.csv) : Dataset mentah *Global Coral Bleaching Database* (~41.000 baris, >60 atribut).
*   📁 **`jurnal/`** : Kumpulan referensi jurnal ilmiah terkait ekosistem laut dan analisis big data oseanografi.
*   📁 **`output/`** : Hasil akhir pemrosesan data (ETL).
    *   `coral_bleaching/` : Hasil akhir data Gold terbagi berdasarkan dimensi analisis (`gold_country`, `gold_ocean`, `gold_category`, `gold_year`).

---

### 🗃️ Dataset & Prapemrosesan
Dataset yang digunakan diperoleh dari Kaggle (**Global Coral Bleaching Database**). Dataset ini memuat sekitar **41.000 observasi** dengan **lebih dari 60 atribut** lingkungan seperti lokasi stasiun, koordinat, nama negara, kedalaman laut, suhu permukaan, tingkat stres termal (SSTA, TSA, DHW), dan persentase pemutihan terumbu karang.

Sebelum dianalisis, data mentah melalui prapemrosesan berikut:
1. **Penanganan Data Duplikat**: Menghapus baris yang identik untuk menghindari bias.
2. **Penanganan Missing Values**: Mengonversi nilai `"nd"` (no data) menjadi `null` dan menyaring baris koordinat (latitude/longitude) serta nama negara yang kosong.
3. **Standardisasi Tipe Data**: Memastikan tipe numerik dideklarasikan dengan tipe *double* untuk komputasi presisi.
4. **Konversi ke Parquet**: Mengubah data hasil pembersihan dari CSV ke format penyimpanan Parquet demi performa I/O dan kompresi yang lebih optimal.

---

### ⚙️ Arsitektur Sistem & Medallion Pipeline
Sistem ini menggunakan infrastruktur berbasis **Docker** dengan konfigurasi **Docker Hadoop Cluster** (sebagai *Distributed Storage*) dan **Docker Spark Cluster** (sebagai *Distributed Processing*).

#### Alur Pipeline (Medallion Architecture):
1. **Bronze Layer**: Menyimpan data mentah dari HDFS (`/datalake/bronze/coral_bleaching/`).
2. **Silver Layer** ([bronze_to_silver.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/bronze_to_silver.py)):
   * Membaca CSV dari Bronze Layer.
   * Melakukan pembersihan data (*data cleaning*).
   * Melakukan rekayasa fitur (*feature engineering*):
     * Konversi suhu Kelvin ke Celsius (`Temperature_Celsius`).
     * Penggabungan tahun/bulan/hari menjadi `Observation_Date`.
     * Kategorisasi pemutihan terumbu karang (`Bleaching_Category`): *Unknown*, *No Bleaching*, *Low*, *Medium*, *High*.
   * Menyimpan output hasil transformasi ke format Parquet di Silver Layer (`/datalake/silver/`).
3. **Gold Layer** ([silver_to_gold.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/silver_to_gold.py)):
   * Melakukan agregasi data sesuai dengan dimensi kebutuhan pelaporan (Negara, Samudra, Kategori, Tahun).
   * Menyimpan dataset analitik akhir di `/datalake/gold/` dalam format Parquet dan CSV ([export_gold_csv.py](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/code/export_gold_csv.py)).

---

### 📊 Hasil Pengujian & Evaluasi Performa
Pengujian dilakukan dengan membandingkan **PySpark (Docker Spark Cluster)** sebagai teknologi utama proyek dengan **Pandas (Single-Machine Sequential Baseline)**. 

* **Metrik Pengukuran**:
  * **Waktu Eksekusi (Execution Time)**: Diukur dalam satuan detik untuk menyelesaikan seluruh alur pembersihan hingga agregasi data.
  * **Throughput**: Jumlah baris data yang berhasil diproses per detik (baris/detik).
* **Stress Test**: Pengujian juga dilakukan pada dataset yang diperbesar 5 kali lipat menggunakan skrip `perbesar_dataset.py` untuk mengukur tingkat skalabilitas (*scalability*) pemrosesan terdistribusi Spark saat menangani beban data yang berlipat.

---

### 💻 Cara Menjalankan Proyek

#### Prasyarat (Prerequisites)
Pastikan perangkat Anda sudah terinstal:
* Python 3.8+
* Apache Spark (dan PySpark)
* Pandas
* Java Development Kit (JDK) untuk menjalankan Spark (direkomendasikan versi 8 atau 11)
* Docker & Docker Compose (opsional, untuk menjalankan Hadoop/Spark cluster)

#### Langkah-langkah Eksekusi
1. **Kloning Repositori**:
   ```bash
   git clone https://github.com/tantywidiyastuti/Tubes-ABD_Kelompok6_SDGs14.git
   cd Tubes-ABD_Kelompok6_SDGs14
   ```
2. **Menjalankan Pipeline ETL (PySpark)**:
   * Jalankan pembersihan Bronze ke Silver:
     ```bash
     python code/bronze_to_silver.py
     ```
   * Jalankan agregasi Silver ke Gold:
     ```bash
     python code/silver_to_gold.py
     ```
   * Ekspor hasil Gold ke format CSV untuk divisualisasikan:
     ```bash
     python code/export_gold_csv.py
     ```
3. **Menjalankan Pengujian Perbandingan Performa**:
   * Perbesar ukuran dataset (Opsional untuk *stress testing*):
     ```bash
     python code/perbesar_dataset.py
     ```
   * Jalankan uji performa dengan Pandas:
     ```bash
     python code/evaluasi_pandas.py
     ```
   * Jalankan uji performa dengan PySpark:
     ```bash
     python code/evaluasi_spark.py
     ```

---

### 📈 Visualisasi & Dashboard (Power BI)
Dataset Gold dari alur pemrosesan Spark divisualisasikan secara interaktif dalam dashboard Power BI (`dashboard/Dashboard Tubes ABD.pbix`). Visualisasi berfokus pada:
1. **Peta Distribusi Geografis**: Visualisasi tingkat keparahan pemutihan terumbu karang global.
2. **Korelasi Suhu & Pemutihan**: Kenaikan suhu rata-rata permukaan laut per tahun terhadap peningkatan persentase kerusakan terumbu karang.
3. **Korelasi Stres Termal**: Distribusi rata-rata *Sea Surface Temperature Anomaly* (SSTA DHW) dan *Thermal Stress Anomaly* (TSA DHW) di berbagai samudra dunia.

Untuk melihat laporan lengkap visualisasi ini, Anda dapat membuka dokumen ekspornya di [Dashboard Tubes ABD.pdf](file:///C:/Users/ASUS/.gemini/antigravity/scratch/Tubes-ABD_Kelompok6_SDGs14/dashboard/Dashboard%20Tubes%20ABD.pdf).

---

### 👥 Kontributor
**Kelompok 6 - Analisis Big Data**
* Tanty Widiyastuti
* *(Anggota Kelompok Lain)*

---
*Proyek ini didedikasikan untuk mendukung kesadaran global terhadap kelestarian ekosistem laut sesuai arahan SDGs 14.*
