import os 
import dotenv
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, coalesce, lit, xxhash64, from_json,to_timestamp  
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType

dotenv.load_dotenv(override=True)

class RetailStarSchemaConsumer:
    def __init__(self, spark_session, jdbc_url, jdbc_props):
        self.spark = spark_session
        self.jdbc_url = jdbc_url
        self.jdbc_props = jdbc_props

    def _ensure_primary_key(self, cursor, table_name, primary_key):
        """Check if a PRIMARY KEY exists on the table and add it if not."""
        cursor.execute(f"""
            SELECT COUNT(1) FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = '{table_name}'
              AND CONSTRAINT_TYPE = 'PRIMARY KEY'
        """)
        has_pk = cursor.fetchone()[0]
        if not has_pk:
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})")
                print(f"  🔑 Primary key added on '{table_name}' ({primary_key})")
            except Exception as e:
                print(f"  ⚠️  Could not add primary key on '{table_name}': {e}")

    def write_to_mysql(self, df, table_name, primary_key=None):
        staging_table = f"{table_name}_staging"

        try:
            # 1. Write the micro-batch to a temporary staging table in MySQL
            df.write.jdbc(
                url=self.jdbc_url,
                table=staging_table,
                mode="overwrite",
                properties=self.jdbc_props
            )

            # 2. Connect directly to MySQL to perform the INSERT IGNORE
            conn = mysql.connector.connect(
                host=os.getenv("HOST"),
                user=os.getenv("USER"),
                password=os.getenv("PASSWORD"),
                database=os.getenv("DATABASE")
            )
            cursor = conn.cursor()

            # 3. Ensure the main table exists (create from staging schema if needed)
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} LIKE {staging_table}")

            # 4. FIX: Ensure PRIMARY KEY exists on the main table so INSERT IGNORE works
            if primary_key:
                self._ensure_primary_key(cursor, table_name, primary_key)

            # Get column names dynamically
            columns = df.columns
            cols_str = ", ".join(columns)

            # 5. Perform deduplication natively in MySQL
            if primary_key:
                # Dimension tables: skip rows whose PK already exists
                insert_query = f"""
                    INSERT IGNORE INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                """
            else:
                # Fact table: also deduplicate on event_id via INSERT IGNORE
                insert_query = f"""
                    INSERT IGNORE INTO {table_name} ({cols_str})
                    SELECT {cols_str} FROM {staging_table}
                """

            cursor.execute(insert_query)
            conn.commit()

            inserted_count = cursor.rowcount
            cursor.close()
            conn.close()

            print(f"✅ {table_name} : {inserted_count} nouvelles lignes insérées (doublons ignorés).")

        except Exception as e:
            print(f"\n🛑 Erreur critique lors de l'écriture dans '{table_name}'.")
            print(f"🔍 Raison exacte: {str(e)}\n")
            raise e

    def process_batch(self, batch_df, batch_id):
        if batch_df.count() > 0:
            print(f"\n🚀 Processing Batch {batch_id} into Star Schema...")

            batch_df.cache()

            try:
                # Generate surrogate keys for device and location dimensions
                batch_df = batch_df.withColumn(
                    "device_id", xxhash64(concat_ws("-",
                        coalesce(col("device_type"), lit("UNKNOWN")),
                        coalesce(col("os"), lit("UNKNOWN")),
                        coalesce(col("browser"), lit("UNKNOWN"))
                    ))
                ).withColumn(
                    "location_id", xxhash64(concat_ws("-",
                        coalesce(col("city"), lit("UNKNOWN")),
                        coalesce(col("region"), lit("UNKNOWN"))
                    ))
                )

                # --- Dimension tables ---
                dim_product = batch_df.select(
                    "product_id", "product_name", "category", "subcategory", "brand"
                ).distinct()

                dim_location = batch_df.select(
                    "location_id", "city", "region", "lat", "lon"
                ).distinct()

                dim_device = batch_df.select(
                    "device_id", "device_type", "os", "browser"
                ).distinct()

                # --- Fact table ---
                fact_events = batch_df.select(
                    "event_id", "session_id", "event_timestamp", "visitor_id",
                    "product_id", "location_id", "device_id",
                    "event_type", "price_unit", "quantity",
                    "total_amount", "payment_status", "traffic_source", "is_promo"
                )

                # FIX: pass primary_key for ALL tables, including fact_events,
                # so INSERT IGNORE deduplicates on event_id across batches.
                self.write_to_mysql(dim_product,  "dim_product",  primary_key="product_id")
                self.write_to_mysql(dim_location, "dim_location", primary_key="location_id")
                self.write_to_mysql(dim_device,   "dim_device",   primary_key="device_id")
                self.write_to_mysql(fact_events,  "fact_events",  primary_key="event_id")

                print(f"✅ Batch {batch_id} successfully synced to Star Schema.")

            except Exception as e:
                print(f"❌ Batch {batch_id} failed during Star Schema sync. (Check detailed error above).")

            finally:
                batch_df.unpersist()


def run_pipeline():
    print("⚙️  Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName("RetailStarSchemaStreaming") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    host     = os.getenv("HOST")
    database = os.getenv("DATABASE")
    jdbc_url = f"jdbc:mysql://{host}:3306/{database}"

    jdbc_props = {
        "user":     os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "driver":   "com.mysql.cj.jdbc.Driver"
    }

    print("🔌 Connecting to Kafka...")

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
        .option("subscribe", os.getenv("KAFKA_TOPIC")) \
        .option("startingOffsets", "earliest") \
        .load()

    kafka_schema = StructType([
        StructField("event_id",        StringType(),    True),
        StructField("session_id",      StringType(),    True),
        StructField("event_timestamp", StringType(), True),
        StructField("visitor_id",      StringType(),    True),
        StructField("product_id",      StringType(),    True),
        StructField("product_name",    StringType(),    True),
        StructField("category",        StringType(),    True),
        StructField("subcategory",     StringType(),    True),
        StructField("brand",           StringType(),    True),
        StructField("city",            StringType(),    True),
        StructField("region",          StringType(),    True),
        StructField("lat",             DoubleType(),    True),
        StructField("lon",             DoubleType(),    True),
        StructField("device_type",     StringType(),    True),
        StructField("os",              StringType(),    True),
        StructField("browser",         StringType(),    True),
        StructField("event_type",      StringType(),    True),
        StructField("price_unit",      DoubleType(),    True),
        StructField("quantity",        IntegerType(),   True),
        StructField("total_amount",    DoubleType(),    True),
        StructField("payment_status",  StringType(),    True),
        StructField("traffic_source",  StringType(),    True),
        StructField("is_promo",        BooleanType(),   True)
    ])

    parsed_stream = raw_kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kafka_schema).alias("data")) \
    .select("data.*") \
    .withColumn(                                            
        "event_timestamp",
        to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    consumer = RetailStarSchemaConsumer(spark, jdbc_url, jdbc_props)

    print(f"▶️  Starting Stream from topic '{os.getenv('KAFKA_TOPIC')}' (Press Ctrl+C to stop)...")

    streaming_query = parsed_stream.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "./schema_checkpoints") \
        .foreachBatch(consumer.process_batch) \
        .start()

    streaming_query.awaitTermination()


if __name__ == "__main__":
    run_pipeline()