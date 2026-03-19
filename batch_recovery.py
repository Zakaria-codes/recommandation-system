import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

def run_batch_recovery():
    # 1. Initialisation Spark (Batch mode, pas besoin de Kafka ici)
    spark = SparkSession.builder \
        .appName("BatchRecoveryJob") \
        .config("spark.jars", "mysql-connector-j-8.0.33.jar") \
        .master("local[*]") \
        .getOrCreate()

    print("📖 Lecture du Master Dataset (Parquet)...")
    # 2. Lecture de l'historique complet
    historical_df = spark.read.parquet("./data_lake/raw_events/")

    # 3. Configuration JDBC
    host = os.getenv("HOST")
    db = os.getenv("DATABASE")
    jdbc_url = f"jdbc:mysql://{host}:3306/{db}"
    
    print(f"🚀 Re-traitement de {historical_df.count()} lignes vers MySQL...")

    # 4. Écriture vers une table de secours ou la table principale
    historical_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "raw_data_recovered") \
        .option("user", os.getenv("USER")) \
        .option("password", os.getenv("PASSWORD")) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    print("✅ Récupération terminée.")
    spark.stop()

if __name__ == "__main__":
    run_batch_recovery()