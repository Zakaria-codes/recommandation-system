import os
import time
import gc
import mysql.connector
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, when, explode
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline

load_dotenv()
DB_USER = os.getenv("USER")
DB_PASS = os.getenv("PASSWORD")
DB_HOST = os.getenv("HOST")
DB_NAME = os.getenv("DATABASE")

current_dir = os.path.dirname(os.path.abspath(__file__))
jar_name = "mysql-connector-j-8.0.33.jar"
jar_path = os.path.join(current_dir, jar_name)

jdbc_url = f"jdbc:mysql://{DB_HOST}:3306/{DB_NAME}?useSSL=false&allowPublicKeyRetrieval=true"

MODEL_DIR = os.path.join(current_dir, "als")
os.makedirs(MODEL_DIR, exist_ok=True)
ALS_MODEL_PATH = os.path.join(MODEL_DIR, "als_model")
PIPELINE_PATH = os.path.join(MODEL_DIR, "als_pipeline_model")

spark = SparkSession.builder \
    .appName("StreamPulse_AI_Engine_Prod") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", jar_path) \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.rpc.message.maxSize", "1024") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .getOrCreate()

print("🚀 StreamPulse AI Engine prêt")

def get_mysql_count():
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
        print(f"Erreur lors du comptage MySQL: {e}")
        return 0

last_trained_count = 0

while True:
    try:
        current_count = get_mysql_count()
        new_records = current_count - last_trained_count

        if current_count < 10:
            print(f"Données insuffisantes ({current_count}/10). Attente 20s...")
            time.sleep(20)
            continue

        if new_records < 100 and last_trained_count != 0:
            print(f"Pas assez de nouvelles données ({new_records}/100). Repos 30s...")
            time.sleep(30)
            continue

        print(f"Entraînement lancé : {new_records} nouvelles interactions détectées.")
        start_time = time.time()

        # FIX 1 : filtres défensifs directement dans la requête SQL
        # MySQL élimine les nulls avant de transférer les données à Spark
        query = """
        (SELECT CAST(visitor_id AS CHAR) AS visitor_id,
                product_name,
                event_type
         FROM raw_data
         WHERE event_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
           AND event_type IN ('view', 'addtocart', 'transaction')
           AND visitor_id   IS NOT NULL
           AND product_name IS NOT NULL
           AND product_name <> ''
        ) as recent_data
        """

        data = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load() \
            .withColumn("visitor_id",   col("visitor_id").cast("string")) \
            .withColumn("product_name", col("product_name").cast("string")) \
            .withColumn("rating",
                when(col("event_type") == "transaction", 5.0)
                .when(col("event_type") == "addtocart",  3.0)
                .otherwise(1.0)
            ) \
            .filter(                          # FIX 2 : garde-fou côté Spark
                col("visitor_id").isNotNull()   &
                col("product_name").isNotNull() &
                col("rating").isNotNull()       &
                (col("visitor_id")   != "")     &
                (col("product_name") != "")
            )

        data.cache()

        # FIX 3 : vérification du count AVANT de lancer ALS
        row_count = data.count()
        print(f"Lignes valides pour ALS : {row_count}")

        if row_count < 10:
            print("Pas assez de lignes valides après filtrage, skip ce cycle.")
            data.unpersist()
            time.sleep(30)
            continue

        indexer_user    = StringIndexer(inputCol="visitor_id",   outputCol="user_index",    handleInvalid="skip")
        indexer_product = StringIndexer(inputCol="product_name", outputCol="product_index", handleInvalid="skip")
        pipeline        = Pipeline(stages=[indexer_user, indexer_product])

        model_indexer    = pipeline.fit(data)
        transformed_data = model_indexer.transform(data)

        # FIX 4 : vérification après transformation (handleInvalid="skip" peut encore supprimer des lignes)
        transformed_count = transformed_data.count()
        print(f"Lignes après StringIndexer : {transformed_count}")

        if transformed_count < 10:
            print("Trop de lignes supprimées par StringIndexer, skip ce cycle.")
            data.unpersist()
            time.sleep(30)
            continue

        als = ALS(
            maxIter=10,
            rank=15,
            regParam=0.1,
            userCol="user_index",
            itemCol="product_index",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        model = als.fit(transformed_data)

        model.write().overwrite().save(ALS_MODEL_PATH)
        model_indexer.write().overwrite().save(PIPELINE_PATH)

        user_recs    = model.recommendForAllUsers(3)
        recs_exploded = user_recs \
            .select("user_index", explode("recommendations").alias("rec")) \
            .select("user_index", col("rec.product_index"), col("rec.rating").alias("confidence"))

        user_converter    = IndexToString(inputCol="user_index",    outputCol="visitor_id",   labels=model_indexer.stages[0].labels)
        product_converter = IndexToString(inputCol="product_index", outputCol="product_name", labels=model_indexer.stages[1].labels)

        final_recs = user_converter.transform(recs_exploded)
        final_recs = product_converter.transform(final_recs)

        # Zero-downtime atomic swap
        staging_table = "product_recommendations_staging"
        final_recs.select("visitor_id", "product_name", "confidence").write \
            .format("jdbc") \
            .option("url",      jdbc_url) \
            .option("dbtable",  staging_table) \
            .option("user",     DB_USER) \
            .option("password", DB_PASS) \
            .option("driver",   "com.mysql.cj.jdbc.Driver") \
            .mode("overwrite") \
            .save()

        conn   = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        cursor = conn.cursor()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS product_recommendations LIKE {staging_table}")
        cursor.execute("DROP TABLE IF EXISTS product_recs_old")
        cursor.execute("""
            RENAME TABLE
                product_recommendations         TO product_recs_old,
                product_recommendations_staging TO product_recommendations
        """)
        cursor.execute("DROP TABLE IF EXISTS product_recs_old")

        conn.commit()
        cursor.close()
        conn.close()

        last_trained_count = current_count
        duration = time.time() - start_time
        print(f"✅ Recommandations mises à jour (Zero-Downtime) en {duration:.2f}s pour {current_count} lignes.")

        data.unpersist()
        spark.catalog.clearCache()

        del model
        del transformed_data
        del final_recs

        gc.collect()
        time.sleep(60)

    except Exception as e:
        print(f"❌ Erreur critique lors de l'entraînement : {e}")
        spark.catalog.clearCache()
        gc.collect()
        time.sleep(30)