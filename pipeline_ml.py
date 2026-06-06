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
    
    # Inisialisasi Session Spark
    spark = SparkSession.builder \
        .appName("ITERA_Medallion_ML_Pipeline") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # =========================================================================
    # 1. BRONZE LAYER (Membaca Data Mentah)
    # =========================================================================
    print("\n[BRONZE LAYER] LOADING RAW DATA FROM BRONZE DIRECTORY...")
    bronze_path = "/opt/bitnami/spark/app/data/bronze/household_power_consumption.csv"
    
    if not os.path.exists(bronze_path):
        print(f"[ERROR] Berkas tidak ditemukan di jalur: {bronze_path}")
        print("Pastikan Anda sudah mengunduh dataset ke folder data/bronze/")
        sys.exit(1)
        
    # PERBAIKAN UTAMA: Menggunakan delimiter ";" sesuai format asli dataset UCI
    df_bronze = spark.read.option("header", "true").option("delimiter", ";").csv(bronze_path)
    
    # =========================================================================
    # 2. SILVER LAYER (Pembersihan & Transformasi ke Parquet)
    # =========================================================================
    print("\n[SILVER LAYER] CLEANING DATA & TRANSFORMING DATA TYPES...")
    numeric_columns = [
        "Global_active_power", "Global_reactive_power", 
        "Voltage", "Global_intensity", 
        "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
    ]
    
    # Mengganti karakter "?" menjadi None/Null dan mengubah tipe data menjadi Double
    df_silver_temp = df_bronze
    for col in numeric_columns:
        df_silver_temp = df_silver_temp.withColumn(
            col, 
            F.when(F.col(col) == "?", None).otherwise(F.col(col)).cast(DoubleType())
        )
        
    # Menghapus baris yang memiliki nilai Kosong/Null
    df_silver_cleaned = df_silver_temp.dropna()
    
    # Menggabungkan kolom Date dan Time menjadi satu objek Timestamp tunggal
    df_silver_final = df_silver_cleaned.withColumn(
        "Full_Timestamp", 
        F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "d/M/yyyy HH:mm:ss")
    )
    
    # Menyimpan hasil pembersihan ke folder Silver dengan format Parquet
    silver_output_path = "/opt/bitnami/spark/app/data/silver/energy_cleaned.parquet"
    df_silver_final.write.mode("overwrite").parquet(silver_output_path)
    print(f"[SUCCESS] Data Silver Layer berhasil disimpan di: {silver_output_path}")

    # =========================================================================
    # 3. GOLD LAYER (Penerapan Machine Learning & Ekspor ke PostgreSQL)
    # =========================================================================
    print("\n[GOLD LAYER] PROCESSING MACHINE LEARNING MODELS...")
    df_silver_load = spark.read.parquet(silver_output_path)
    
    # -------------------------------------------------------------------------
    # ANALISIS 1: K-MEANS CLUSTERING (Profil Klasifikasi Pemakaian Energi)
    # -------------------------------------------------------------------------
    print("[GOLD - ML 1] Training K-Means Clustering Model...")
    assembler_km = VectorAssembler(
        inputCols=["Sub_metering_1", "Sub_metering_2", "Sub_metering_3"], 
        outputCol="cluster_features"
    )
    df_km_vector = assembler_km.transform(df_silver_load)
    
    kmeans = KMeans(featuresCol="cluster_features", predictionCol="Cluster_ID", k=3, seed=42)
    model_km = kmeans.fit(df_km_vector)
    df_clustered_raw = model_km.transform(df_km_vector)
    
    # Agregasi data klasterisasi per hari untuk kebutuhan visualisasi Chart bisnis
    df_gold_cluster = df_clustered_raw.withColumn("Tanggal", F.to_date(F.col("Full_Timestamp"))) \
        .groupBy("Tanggal", "Cluster_ID") \
        .agg(
            F.round(F.avg("Global_active_power"), 3).alias("Rata_Daya_kW"), 
            F.count("Cluster_ID").alias("Durasi_Menit")
        ) \
        .withColumn(
            "Kategori_Hari", 
            F.when(F.col("Cluster_ID") == 0, "Profil Hemat")
             .when(F.col("Cluster_ID") == 1, "Profil Normal")
             .otherwise("Profil Boros Ekstrem")
        ) \
        .orderBy("Tanggal")
        
    # Menyimpan hasil klaster ke file lokal Gold
    gold_cluster_path = "/opt/bitnami/spark/app/data/gold/fact_ml_klaster_hari.parquet"
    df_gold_cluster.write.mode("overwrite").parquet(gold_cluster_path)
        
    # -------------------------------------------------------------------------
    # ANALISIS 2: LINEAR REGRESSION (Prediksi Estimasi Beban Listrik Rumah)
    # -------------------------------------------------------------------------
    print("[GOLD - ML 2] Training Linear Regression Model...")
    assembler_lr = VectorAssembler(
        inputCols=["Global_intensity", "Voltage"], 
        outputCol="regression_features"
    )
    df_lr_vector = assembler_lr.transform(df_silver_load)
    
    # Split data: 80% Training, 20% Testing
    train_data, test_data = df_lr_vector.randomSplit([0.8, 0.2], seed=42)
    lr = LinearRegression(
        featuresCol="regression_features", 
        labelCol="Global_active_power", 
        predictionCol="Prediksi_Daya_kW"
    )
    model_lr = lr.fit(train_data)
    df_predictions = model_lr.transform(test_data)
    
    # Agregasi hasil prediksi per jam untuk kebutuhan visualisasi Line-Chart Line Perbandingan
    df_gold_prediction = df_predictions.withColumn("Tanggal", F.to_date(F.col("Full_Timestamp"))) \
        .withColumn("Jam", F.hour(F.col("Full_Timestamp"))) \
        .groupBy("Tanggal", "Jam") \
        .agg(
            F.round(F.avg("Global_active_power"), 3).alias("Daya_Aktif_Aktual_kW"), 
            F.round(F.avg("Prediksi_Daya_kW"), 3).alias("Daya_Aktif_Prediksi_kW")
        ) \
        .orderBy("Tanggal", "Jam")

    # Menyimpan hasil prediksi ke file lokal Gold
    gold_pred_path = "/opt/bitnami/spark/app/data/gold/fact_ml_prediksi_daya.parquet"
    df_gold_prediction.write.mode("overwrite").parquet(gold_pred_path)

    # =========================================================================
    # 4. DATA EXPORT LAYER (Mengirim Data ke PostgreSQL Data Warehouse)
    # =========================================================================
    print("\n[DATA WAREHOUSE] EXPORTING GOLD TABLES TO POSTGRESQL...")
    db_url = "jdbc:postgresql://postgres:5432/energy_db"
    db_properties = {
        "user": "superset", 
        "password": "superset_password", 
        "driver": "org.postgresql.Driver"
    }
    
    # Menulis langsung ke database PostgreSQL target visualisasi Apache Superset
    print("[WRITE] Mengirim tabel 'gold_fact_ml_klaster_hari'...")
    df_gold_cluster.write.jdbc(url=db_url, table="gold_fact_ml_klaster_hari", mode="overwrite", properties=db_properties)
    
    print("[WRITE] Mengirim tabel 'gold_fact_ml_prediksi_daya'...")
    df_gold_prediction.write.jdbc(url=db_url, table="gold_fact_ml_prediksi_daya", mode="overwrite", properties=db_properties)
    
    print("\n" + "="*60 + "\n[STATUS] MEDALLION ARCHITECTURE PIPELINE BERHASIL DIEKSEKUSI SECARA SEMPURNA!\n" + "="*60)
    spark.stop()

if __name__ == "__main__":
    main()
