# Dashboard Analisis Konsumsi Energi Rumah Tangga
## Berbasis Medallion Architecture & Apache Spark MLlib

> **Mata Kuliah:** Analisis Big Data (ABD)  
> **Penyusun:** 
> 1. Efi Defiyati (123450005)
> 2. Nadia Faraj Alyafaatin Simbolon (123450092)
> 3. Arini Puteri Elandra (123450069)
> 4. Muhammad Naufal Al Ghani (123450116)
> **Implementasi:** K-Means Clustering & Linear Regression Terdistribusi Kelompok 27 SDG 7 - Energi Bersih dan Terjangkau

---

## Struktur Direktori Proyek

Sebelum memulai, pastikan struktur direktori pada sistem lokal Anda sudah sesuai dengan bagan berikut:

```
D:\Tugas_Besar_ABD\
├── data/
│   ├── bronze/
│   │   └── household_power_consumption.csv
│   ├── silver/
│   └── gold/
├── Dockerfile
├── docker-compose.yml
└── pipeline_ml.py
```

---

## Fase 1 — Persiapan Lingkungan & Ingesti Data (WSL Ubuntu)

### Langkah 1: Buat Struktur Direktori Medallion Architecture

Buka terminal WSL Ubuntu, lalu jalankan perintah berikut untuk membuat folder kerja beserta sub-folder penyimpanan bertingkat:

```bash
mkdir -p /mnt/d/Tugas_Besar_ABD/data/bronze
mkdir -p /mnt/d/Tugas_Besar_ABD/data/silver
mkdir -p /mnt/d/Tugas_Besar_ABD/data/gold
cd /mnt/d/Tugas_Besar_ABD
```

### Langkah 2: Unduh dan Siapkan Dataset (Bronze Layer)

Masuk ke folder `bronze`, lalu unduh dataset dari UCI Machine Learning Repository:

```bash
cd data/bronze
wget https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip

# Install unzip dan ekstrak berkas
sudo apt-get update && sudo apt-get install -y unzip
unzip individual+household+electric+power+consumption.zip
mv household_power_consumption.txt household_power_consumption.csv

# Kembali ke direktori utama
cd /mnt/d/Tugas_Besar_ABD
```

---

## Fase 2 — Orkestrasi Infrastruktur Multi-Container (Docker)

### Langkah 3: Buat `Dockerfile` Kustom

Buat `Dockerfile` agar pustaka `NumPy` terpasang secara bawaan pada image Spark:

```bash
nano Dockerfile
```

Isi dengan konfigurasi berikut:

```dockerfile
FROM apache/spark:3.5.0
USER root
RUN pip install --no-cache-dir numpy
```

> **Simpan:** `Ctrl + O` → `Enter` → `Ctrl + X`

---

### Langkah 4: Buat `docker-compose.yml`

```bash
nano docker-compose.yml
```

Isi dengan konfigurasi lengkap berikut:

```yaml
services:
  # Master Node Komputasi Spark
  spark-master:
    build: .
    container_name: abd-spark-master
    user: root
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - .:/opt/bitnami/spark/app
    networks:
      - abd-network
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master

  # Worker Node Komputasi Spark
  spark-worker:
    build: .
    container_name: abd-spark-worker
    user: root
    depends_on:
      - spark-master
    volumes:
      - .:/opt/bitnami/spark/app
    networks:
      - abd-network
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077

  # Relational Data Warehouse (PostgreSQL)
  postgres:
    image: postgres:15
    container_name: abd-postgres
    environment:
      - POSTGRES_USER=superset
      - POSTGRES_PASSWORD=superset_password
      - POSTGRES_DB=energy_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata_energy:/var/lib/postgresql/data
    networks:
      - abd-network

  # Business Intelligence Layer (Apache Superset)
  superset:
    image: apache/superset:3.1.0
    container_name: abd-superset
    depends_on:
      - postgres
    environment:
      - SUPERSET_SECRET_KEY=itera-sainsdata-abd-secret-key-2026
      - DATABASE_URL=postgresql+psycopg2://superset:superset_password@postgres:5432/energy_db
    ports:
      - "8089:8088"
    networks:
      - abd-network
    command: >
      bash -c "
      superset db upgrade &&
      superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin_password &&
      superset init &&
      superset run -h 0.0.0.0 -p 8088
      "

volumes:
  pgdata_energy:

networks:
  abd-network:
    driver: bridge
```

> **Simpan:** `Ctrl + O` → `Enter` → `Ctrl + X`

---

### Langkah 5: Jalankan Ekosistem Container

Hapus volume lama untuk menghindari konflik, lalu bangun dan jalankan semua container:

```bash
# Hapus container dan volume lama
docker compose down -v --remove-orphans

# Build ulang dan jalankan
docker compose up -d --build
```

> Tunggu sekitar **30–60 detik** agar seluruh layanan selesai diinisialisasi.

---

## Fase 3 — Implementasi Pipeline Data & Pemodelan AI (Spark MLlib)

### Langkah 6: Buat Skrip Utama `pipeline_ml.py`

```bash
nano pipeline_ml.py
```

Isi dengan kode PySpark lengkap berikut:

```python
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression

def main():
    print("\n" + "="*60 + "\n[SPARK ENGINE] INITIALIZING PIPELINE RUN...\n" + "="*60)

    spark = SparkSession.builder \
        .appName("ITERA_Medallion_ML_Pipeline") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # =========================================================
    # 1. BRONZE LAYER — Membaca Data Mentah
    # =========================================================
    print("\n[BRONZE LAYER] LOADING RAW DATA FROM BRONZE DIRECTORY...")
    bronze_path = "/opt/bitnami/spark/app/data/bronze/household_power_consumption.csv"

    if not os.path.exists(bronze_path):
        print(f"[ERROR] Berkas tidak ditemukan di jalur: {bronze_path}")
        sys.exit(1)

    df_bronze = spark.read.option("header", "true").option("delimiter", ";").csv(bronze_path)

    # =========================================================
    # 2. SILVER LAYER — Pembersihan & Transformasi Data
    # =========================================================
    print("\n[SILVER LAYER] CLEANING DATA & TRANSFORMING DATA TYPES...")
    numeric_columns = [
        "Global_active_power", "Global_reactive_power", "Voltage",
        "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
    ]

    df_silver_temp = df_bronze
    for col in numeric_columns:
        df_silver_temp = df_silver_temp.withColumn(
            col,
            F.when(F.col(col) == "?", None).otherwise(F.col(col)).cast(DoubleType())
        )

    df_silver_cleaned = df_silver_temp.dropna()
    df_silver_final = df_silver_cleaned.withColumn(
        "Full_Timestamp",
        F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "d/M/yyyy HH:mm:ss")
    )

    silver_output_path = "/opt/bitnami/spark/app/data/silver/energy_cleaned.parquet"
    df_silver_final.write.mode("overwrite").parquet(silver_output_path)
    print(f"[SUCCESS] Data Silver Layer berhasil disimpan.")

    # =========================================================
    # 3. GOLD LAYER — Machine Learning Terdistribusi
    # =========================================================
    print("\n[GOLD LAYER] PROCESSING MACHINE LEARNING MODELS...")
    df_silver_load = spark.read.parquet(silver_output_path)

    # ML 1: K-Means Clustering
    print("[GOLD - ML 1] Training K-Means Clustering Model...")
    assembler_km = VectorAssembler(
        inputCols=["Sub_metering_1", "Sub_metering_2", "Sub_metering_3"],
        outputCol="cluster_features"
    )
    df_km_vector = assembler_km.transform(df_silver_load)
    kmeans = KMeans(featuresCol="cluster_features", predictionCol="Cluster_ID", k=3, seed=42)
    model_km = kmeans.fit(df_km_vector)
    df_clustered_raw = model_km.transform(df_km_vector)

    df_gold_cluster = df_clustered_raw \
        .withColumn("Tanggal", F.to_date(F.col("Full_Timestamp"))) \
        .groupBy("Tanggal", "Cluster_ID") \
        .agg(
            F.round(F.avg("Global_active_power"), 3).alias("Rata_Daya_kW"),
            F.count("Cluster_ID").alias("Durasi_Menit")
        ) \
        .withColumn("Kategori_Hari",
            F.when(F.col("Cluster_ID") == 0, "Profil Hemat")
            .when(F.col("Cluster_ID") == 1, "Profil Normal")
            .otherwise("Profil Boros Ekstrem")
        ) \
        .orderBy("Tanggal")

    gold_cluster_path = "/opt/bitnami/spark/app/data/gold/fact_ml_klaster_hari.parquet"
    df_gold_cluster.write.mode("overwrite").parquet(gold_cluster_path)

    # ML 2: Linear Regression
    print("[GOLD - ML 2] Training Linear Regression Model...")
    assembler_lr = VectorAssembler(
        inputCols=["Global_intensity", "Voltage"],
        outputCol="regression_features"
    )
    df_lr_vector = assembler_lr.transform(df_silver_load)
    train_data, test_data = df_lr_vector.randomSplit([0.8, 0.2], seed=42)

    lr = LinearRegression(
        featuresCol="regression_features",
        labelCol="Global_active_power",
        predictionCol="Prediksi_Daya_kW"
    )
    model_lr = lr.fit(train_data)
    df_predictions = model_lr.transform(test_data)

    df_gold_prediction = df_predictions \
        .withColumn("Tanggal", F.to_date(F.col("Full_Timestamp"))) \
        .withColumn("Jam", F.hour(F.col("Full_Timestamp"))) \
        .groupBy("Tanggal", "Jam") \
        .agg(
            F.round(F.avg("Global_active_power"), 3).alias("Daya_Aktif_Aktual_kW"),
            F.round(F.avg("Prediksi_Daya_kW"), 3).alias("Daya_Aktif_Prediksi_kW")
        ) \
        .orderBy("Tanggal", "Jam")

    gold_pred_path = "/opt/bitnami/spark/app/data/gold/fact_ml_prediksi_daya.parquet"
    df_gold_prediction.write.mode("overwrite").parquet(gold_pred_path)

    # =========================================================
    # 4. DATA EXPORT LAYER — Sinkronisasi ke PostgreSQL
    # =========================================================
    print("\n[DATA WAREHOUSE] EXPORTING GOLD TABLES TO POSTGRESQL...")
    db_url = "jdbc:postgresql://postgres:5432/energy_db"
    db_properties = {
        "user": "superset",
        "password": "superset_password",
        "driver": "org.postgresql.Driver"
    }

    print("[WRITE] Mengirim tabel 'gold_fact_ml_klaster_hari'...")
    df_gold_cluster.write.jdbc(url=db_url, table="gold_fact_ml_klaster_hari", mode="overwrite", properties=db_properties)

    print("[WRITE] Mengirim tabel 'gold_fact_ml_prediksi_daya'...")
    df_gold_prediction.write.jdbc(url=db_url, table="gold_fact_ml_prediksi_daya", mode="overwrite", properties=db_properties)

    print("\n" + "="*60 + "\n[STATUS] MEDALLION ARCHITECTURE PIPELINE BERHASIL DIEKSEKUSI SECARA SEMPURNA!\n" + "="*60)
    spark.stop()

if __name__ == "__main__":
    main()
```

> **Simpan:** `Ctrl + O` → `Enter` → `Ctrl + X`

---

### Langkah 7: Jalankan Pipeline dengan Spark-Submit

```bash
docker exec -it abd-spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/bitnami/spark/app/pipeline_ml.py
```

> Proses komputasi membutuhkan waktu **1–3 menit**. Tunggu hingga log terminal menampilkan pesan berikut sebelum melanjutkan:
>
> ```
> [STATUS] MEDALLION ARCHITECTURE PIPELINE BERHASIL DIEKSEKUSI SECARA SEMPURNA!
> ```

---

## Fase 4 — Integrasi Data Warehouse & Visualisasi (Apache Superset)

### Langkah 8: Koneksikan Database PostgreSQL

1. Buka browser, akses **`http://localhost:8089`**
2. Login dengan kredensial:
   - **Username:** `admin`
   - **Password:** `admin_password`
3. Navigasi ke **Settings** → **Database Connections** → klik tombol **`+ DATABASE`**
4. Pilih **`PostgreSQL`**, lalu isi parameter berikut:

   | Parameter | Nilai |
   |-----------|-------|
   | HOST | `postgres` |
   | PORT | `5432` |
   | DATABASE NAME | `energy_db` |
   | USERNAME | `superset` |
   | PASSWORD | `superset_password` |

5. Klik **`CONNECT`**

---

### Langkah 9: Daftarkan Dataset Gold Layer

1. Buka menu **Datasets** → klik **`+ DATASET`**
2. Isi konfigurasi:
   - **Database:** `PostgreSQL`
   - **Schema:** `public`
   - **Table:** `gold_fact_ml_klaster_hari`
3. Klik **`ADD`**
4. Ulangi langkah 1–3 untuk tabel kedua: `gold_fact_ml_prediksi_daya`

---

### Langkah 10: Buat 5 Komponen Chart Analitik

Buka menu **Charts** → klik **`+ CHART`** untuk setiap chart berikut:

#### Chart 1 — Profil Distribusi Gaya Hidup Penggunaan Energi (K-Means)
| Parameter | Nilai |
|-----------|-------|
| Dataset | `gold_fact_ml_klaster_hari` |
| Tipe Chart | `Pie Chart` |
| Dimension | `Kategori_Hari` |
| Metric | `Durasi_Menit` (SUM) |

Klik **Run** → **Save** dengan judul: `"Profil Distribusi Gaya Hidup Penggunaan Energi"`

---

#### Chart 2 — Evaluasi Perbandingan Aktual vs Prediksi AI (Linear Regression)
| Parameter | Nilai |
|-----------|-------|
| Dataset | `gold_fact_ml_prediksi_daya` |
| Tipe Chart | `Line Chart` |
| Time Column | `Jam` |
| Metrics | `Daya_Aktif_Aktual_kW` (AVG) + `Daya_Aktif_Prediksi_kW` (AVG) |

Klik **Run** → **Save** dengan judul: `"Grafik Evaluasi Perbandingan Nilai Aktual vs Prediksi Model"`

---

#### Chart 3 — Total Durasi Monitoring Data (KPI Card)
| Parameter | Nilai |
|-----------|-------|
| Dataset | `gold_fact_ml_klaster_hari` |
| Tipe Chart | `Big Number` |
| Metric | `Durasi_Menit` (SUM) |

Klik **Run** → **Save** dengan judul: `"Total Menit Pengamatan Data"`

---

#### Chart 4 — Analisis Deviasi Energi per Jam (Error Analysis)
| Parameter | Nilai |
|-----------|-------|
| Dataset | `gold_fact_ml_prediksi_daya` |
| Tipe Chart | `Bar Chart` |
| X-Axis | `Jam` |
| Metrics | `Daya_Aktif_Aktual_kW` (AVG) + `Daya_Aktif_Prediksi_kW` (AVG) |

Klik **Run** → **Save** dengan judul: `"Analisis Batang Perbandingan Deviasi Energi"`

---

#### Chart 5 — Statistik Kuantitatif Klaster (Pivot Table)
| Parameter | Nilai |
|-----------|-------|
| Dataset | `gold_fact_ml_klaster_hari` |
| Tipe Chart | `Pivot Table` |
| Rows | `Kategori_Hari` |
| Metrics | `Rata_Daya_kW` (AVG) + `Durasi_Menit` (SUM) |

Klik **Run** → **Save** dengan judul: `"Tabel Statistik Parameter Klaster"`

---

### Langkah 11: Susun dan Publikasikan Dashboard

1. Buka menu **Dashboards** → klik **`+ DASHBOARD`**
2. Ganti nama default dengan judul:
   > **Dashboard Analisis Spesifik Konsumsi Energi Rumah Tangga Berbasis Medallion Architecture dan Spark MLlib**
3. Isi sub-judul dengan identitas (contoh):
   > *Disusun oleh: Efi Defiyati (Sains Data ITERA) — Implementasi K-Means & Linear Regression Terdistribusi*
4. Drag-and-drop kelima chart ke kanvas dashboard
5. Tata letak mengikuti **Pola Hierarki Piramida Data**:
   - **Baris atas:** KPI Big Number (ringkasan eksekutif)
   - **Baris tengah:** Line Chart + Bar Chart (berdampingan)
   - **Baris bawah:** Pie Chart + Pivot Table (berdampingan)
6. Klik **`SAVE & PUBLISH`**

---

## Fase 5 — Demonstrasi Output kepada Dosen

### Pembuktian Output K-Means Clustering

Arahkan ke **Pie Chart** dan **Pivot Table**, lalu sampaikan interpretasi berikut:

> Model K-Means membagi data historis ke dalam **3 profil konsumsi harian**:
> - **Profil Hemat (Cluster 0):** Rata-rata daya aktif rendah — mayoritas perangkat dalam kondisi mati
> - **Profil Normal (Cluster 1):** Konsumsi listrik harian standar
> - **Profil Boros Ekstrem (Cluster 2):** Rata-rata daya tertinggi, dipicu penggunaan simultan perangkat Sub_metering_3 (AC, pemanas air)

### Pembuktian Output Linear Regression

Arahkan ke **Multi-Series Line Chart**, lalu sampaikan interpretasi berikut:

> Garis prediksi (merah putus-putus) berhimpitan erat dengan garis aktual (biru solid), membuktikan akurasi tinggi model. Pada pukul 18.00–21.00 (jam sibuk), kedua garis naik secara paralel — mengonfirmasi korelasi linear kuat antara `Global_intensity` dan `Voltage` dalam memprediksi beban daya aktif.

---

### Menampilkan Data untuk Lampiran Laporan

Buka Spark Shell interaktif untuk menghasilkan output `.show()` sebagai bahan screenshot laporan:

```bash
docker exec -it abd-spark-master /opt/spark/bin/spark-shell
```

Setelah prompt `scala>` muncul, jalankan:

```scala
spark.read.parquet("/opt/bitnami/spark/app/data/gold/fact_ml_klaster_hari.parquet").show(5)
spark.read.parquet("/opt/bitnami/spark/app/data/gold/fact_ml_prediksi_daya.parquet").show(5)
```

Untuk keluar dari Spark Shell:

```scala
:quit
```

> **Catatan:** Pastikan menyertakan simbol titik dua `[:]` sebelum kata `quit`.

---

## Ringkasan Kredensial & Port Layanan

| Layanan | URL / Port | Username | Password |
|---------|-----------|----------|----------|
| Apache Superset | `http://localhost:8089` | `admin` | `admin_password` |
| Spark Master UI | `http://localhost:8080` | — | — |
| PostgreSQL | `localhost:5432` | `superset` | `superset_password` |

---

*Tugas Besar — Arsitektur Big Data | Institut Teknologi Sumatera (ITERA)*
