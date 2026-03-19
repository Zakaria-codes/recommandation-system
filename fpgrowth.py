import os
import time
import gc
import mysql.connector  
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, concat_ws, size
from pyspark.ml.fpm import FPGrowth

load_dotenv()
DB_USER = os.getenv("USER")
DB_PASS = os.getenv("PASSWORD")
DB_HOST = os.getenv("HOST")
DB_NAME = os.getenv("DATABASE")

current_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = os.path.join(current_dir, "mysql-connector-j-8.0.33.jar")
jdbc_url = f"jdbc:mysql://{DB_HOST}:3306/{DB_NAME}?useSSL=false&allowPublicKeyRetrieval=true"

MODEL_DIR = os.path.join(current_dir, "fpgrowth_saved")
os.makedirs(MODEL_DIR, exist_ok=True)
FPGROWTH_MODEL_PATH = os.path.join(MODEL_DIR, "fpgrowth_model")

spark = SparkSession.builder \
    .appName("FPGrowth_Production_Engine") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("🚀 Système FP-Growth prêt (Mode Production) ! En attente de paniers...")

def get_mysql_count():
    """Vérifie le nombre de lignes dans raw_data sans tout charger."""
    try:
        count_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", "SELECT COUNT(*) as cnt FROM raw_data") \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        return count_df.collect()[0]['cnt']
    except Exception as e:
        print(f"⚠️ Erreur lors du comptage MySQL: {e}")
        return 0

last_trained_count = 0 

while True:
    try:
        current_count = get_mysql_count()
        new_records = current_count - last_trained_count

        if current_count < 10:
            print(f"⚠️ Données insuffisantes ({current_count}/10). Attente 20s...")
            time.sleep(20)
            continue
        
        if new_records < 50 and last_trained_count != 0:
            print(f"📊 Pas assez de nouvelles données pour relancer FP-Growth ({new_records}/50). Repos 30s...")
            time.sleep(30) 
            continue

        print(f"\n🔄 Entraînement FP-Growth lancé : {new_records} nouveaux événements détectés.")
        start_time = time.time()

        # SQL Pushdown: Get only the columns we need from the last 30 days directly from MySQL
        query = """
        (SELECT visitor_id, product_name 
         FROM raw_data 
         WHERE event_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        ) as recent_data
        """

        df = spark.read.format("jdbc").options(
            url=jdbc_url, dbtable=query, user=DB_USER, password=DB_PASS,
            driver="com.mysql.cj.jdbc.Driver"
        ).load()

        baskets = df.groupBy("visitor_id") \
            .agg(collect_set("product_name").alias("items")) \
            .filter(size(col("items")) > 1)
        
        baskets.cache()
        count_valid = baskets.count()

        if count_valid == 0:
            print("ℹ️ Aucun panier multi-articles récent trouvé pour faire des recommandations.")
        else:
            print(f"🔥 Analyse de {count_valid} paniers complexes...")
            
            fp = FPGrowth(itemsCol="items", minSupport=0.0001, minConfidence=0.0001)
            model = fp.fit(baskets)

            rules = model.associationRules
            if rules.count() > 0:
                output = rules.select(
                    concat_ws(", ", col("antecedent")).alias("produit_achete"),
                    concat_ws(", ", col("consequent")).alias("produit_recommande"),
                    col("confidence"),
                    col("lift")
                )

                # ---------------------------------------------------------
                # THE FIX: ZERO-DOWNTIME ATOMIC SWAP
                # 1. Write to a staging table first
                staging_table = "fpgrowth_staging"
                output.write.format("jdbc").options(
                    url=jdbc_url, dbtable=staging_table, user=DB_USER, password=DB_PASS,
                    driver="com.mysql.cj.jdbc.Driver"
                ).mode("overwrite").save()
                
                # 2. Swap the tables natively in MySQL instantly
                conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
                cursor = conn.cursor()
                
                # Ensure the main table exists
                cursor.execute(f"CREATE TABLE IF NOT EXISTS fpgrowth LIKE {staging_table}")
                
                # Perform the atomic rename
                cursor.execute("DROP TABLE IF EXISTS fpgrowth_old")
                cursor.execute("""
                    RENAME TABLE 
                        fpgrowth TO fpgrowth_old, 
                        fpgrowth_staging TO fpgrowth
                """)
                cursor.execute("DROP TABLE IF EXISTS fpgrowth_old")
                
                conn.commit()
                cursor.close()
                conn.close()
                # ---------------------------------------------------------
                
                duration = time.time() - start_time
                print(f"✅ {rules.count()} règles enregistrées (Zero-Downtime) dans la table 'fpgrowth' en {duration:.2f}s !")
            else:
                print("ℹ️ Paniers trouvés, mais pas de répétitions statistiques suffisantes.")

        last_trained_count = current_count
        baskets.unpersist()
        spark.catalog.clearCache()
        
        if 'model' in locals(): del model
        if 'rules' in locals(): del rules
        del df
        
        gc.collect() 

        time.sleep(60) 

    except Exception as e:
        print(f"❌ Erreur critique: {e}")
        spark.catalog.clearCache()
        gc.collect()
        time.sleep(30)