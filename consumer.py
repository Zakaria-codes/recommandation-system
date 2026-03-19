import os
import mysql.connector
import sys
import urllib.request
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

load_dotenv()

class RetailSparkLocal:
    def __init__(self):
        self.current_dir = os.getcwd()
        self.jar_name = "mysql-connector-j-8.0.33.jar"
        self.jar_path = os.path.join(self.current_dir, self.jar_name)
        self.download_jar_if_missing()

        self.kafka_topic       = 'retail-events'
        self.bootstrap_servers = 'localhost:9092'
        self.db_host           = os.getenv("HOST")
        self.db_user           = os.getenv("USER")
        self.db_pass           = os.getenv("PASSWORD")
        self.db_name           = os.getenv("DATABASE")
        self.jdbc_url          = (
            f"jdbc:mysql://{self.db_host}:3306/{self.db_name}"
            f"?useSSL=false&allowPublicKeyRetrieval=true"
        )

        self.spark = SparkSession.builder \
            .appName("RetailConsumer_Local") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.jars", self.jar_path) \
            .config("spark.driver.extraClassPath", self.jar_path) \
            .master("local[*]") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.startup_cleanup()

    # ------------------------------------------------------------------ #
    #  JAR download                                                        #
    # ------------------------------------------------------------------ #

    def download_jar_if_missing(self):
        if not os.path.exists(self.jar_path):
            url = (
                f"https://repo1.maven.org/maven2/com/mysql/"
                f"mysql-connector-j/8.0.33/{self.jar_name}"
            )
            try:
                urllib.request.urlretrieve(url, self.jar_path)
            except Exception:
                sys.exit(1)

    # ------------------------------------------------------------------ #
    #  Startup cleanup                                                     #
    # ------------------------------------------------------------------ #

    def _ensure_primary_key(self, cursor, table_name, primary_key):
        """Add a PRIMARY KEY on *table_name* if one does not already exist."""
        cursor.execute(f"""
            SELECT COUNT(1)
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = '{table_name}'
              AND CONSTRAINT_TYPE = 'PRIMARY KEY'
        """)
        if cursor.fetchone()[0] == 0:
            try:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key})"
                )
                print(f"  🔑 Primary key added on '{table_name}' ({primary_key})")
            except Exception as e:
                print(f"  ⚠️  Could not add primary key on '{table_name}': {e}")

    def _ensure_datetime6(self, cursor, table_name, column_name):
        """Upgrade a DATETIME column to DATETIME(6) if not already."""
        cursor.execute(f"""
            SELECT COLUMN_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME   = '{table_name}'
              AND COLUMN_NAME  = '{column_name}'
        """)
        row = cursor.fetchone()
        if row and row[0].lower() == "datetime":
            try:
                cursor.execute(
                    f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} DATETIME(6)"
                )
                print(f"  🕐 '{table_name}.{column_name}' upgraded to DATETIME(6)")
            except Exception as e:
                print(f"  ⚠️  Could not upgrade DATETIME on '{table_name}': {e}")

    def startup_cleanup(self):
        try:
            conn = mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_name
            )
            cursor = conn.cursor()

            # 1. Drop any leftover staging tables from a previous crashed run
            cursor.execute("SHOW TABLES LIKE 'temp_staging_batch_%'")
            for (table_name,) in cursor.fetchall():
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            # 2. Create raw_data with DATETIME(6) — FIX for NULL timestamps
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_data (
                    event_id         VARCHAR(64)    NOT NULL,
                    session_id       VARCHAR(64),
                    event_timestamp  DATETIME(6),
                    visitor_id       INT,
                    city             VARCHAR(128),
                    region           VARCHAR(128),
                    lat              DOUBLE,
                    lon              DOUBLE,
                    device_type      VARCHAR(64),
                    os               VARCHAR(64),
                    browser          VARCHAR(64),
                    product_id       VARCHAR(64),
                    product_name     VARCHAR(256),
                    category         VARCHAR(128),
                    subcategory      VARCHAR(128),
                    brand            VARCHAR(128),
                    price_unit       DOUBLE,
                    event_type       VARCHAR(64),
                    quantity         INT,
                    total_amount     DOUBLE,
                    payment_status   VARCHAR(64),
                    traffic_source   VARCHAR(128),
                    traffic_medium   VARCHAR(128),
                    campaign         VARCHAR(128),
                    is_promo         BOOLEAN,
                    PRIMARY KEY (event_id)
                )
            """)

            # 3. Safety net: patch existing table if it was created with plain DATETIME
            self._ensure_datetime6(cursor, "raw_data", "event_timestamp")

            # 4. In case raw_data already existed without a PK, add it now
            self._ensure_primary_key(cursor, "raw_data", "event_id")

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            print(f"⚠️  startup_cleanup warning: {e}")

    # ------------------------------------------------------------------ #
    #  Schema                                                              #
    # ------------------------------------------------------------------ #

    def get_schema(self):
        return StructType([
            StructField("event_id",        StringType(),  True),
            StructField("session_id",      StringType(),  True),
            StructField("event_timestamp", StringType(),  True),   # String → cast manually
            StructField("visitor_id",      IntegerType(), True),
            StructField("city",            StringType(),  True),
            StructField("region",          StringType(),  True),
            StructField("lat",             DoubleType(),  True),
            StructField("lon",             DoubleType(),  True),
            StructField("device_type",     StringType(),  True),
            StructField("os",              StringType(),  True),
            StructField("browser",         StringType(),  True),
            StructField("product_id",      StringType(),  True),
            StructField("product_name",    StringType(),  True),
            StructField("category",        StringType(),  True),
            StructField("subcategory",     StringType(),  True),
            StructField("brand",           StringType(),  True),
            StructField("price_unit",      DoubleType(),  True),
            StructField("event_type",      StringType(),  True),
            StructField("quantity",        IntegerType(), True),
            StructField("total_amount",    DoubleType(),  True),
            StructField("payment_status",  StringType(),  True),
            StructField("traffic_source",  StringType(),  True),
            StructField("traffic_medium",  StringType(),  True),
            StructField("campaign",        StringType(),  True),
            StructField("is_promo",        BooleanType(), True)
        ])

    # ------------------------------------------------------------------ #
    #  Batch processor                                                     #
    # ------------------------------------------------------------------ #

    def process_batch(self, df, epoch_id):
        staging_table = f"temp_staging_batch_{epoch_id}"
        conn = None

        try:
            # --- BATCH LAYER: append raw events to the Data Lake (Parquet) ---
            df.write \
                .mode("append") \
                .parquet("./data_lake/raw_events/")

            # --- SPEED LAYER: write micro-batch to a staging table in MySQL ---
            df.write \
                .format("jdbc") \
                .option("url",      self.jdbc_url) \
                .option("dbtable",  staging_table) \
                .option("user",     self.db_user) \
                .option("password", self.db_pass) \
                .option("driver",   "com.mysql.cj.jdbc.Driver") \
                .mode("overwrite") \
                .save()

            conn = mysql.connector.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_name
            )
            cursor = conn.cursor()

            cols_str = ", ".join(df.columns)

            insert_query = (
                f"INSERT IGNORE INTO raw_data ({cols_str}) "
                f"SELECT {cols_str} FROM {staging_table}"
            )
            drop_query = f"DROP TABLE IF EXISTS {staging_table}"

            cursor.execute(insert_query)
            inserted = cursor.rowcount
            cursor.execute(drop_query)
            conn.commit()

            print(
                f"✅ Batch {epoch_id}: {inserted} rows inserted into MySQL "
                f"& archived in Data Lake."
            )
            cursor.close()

        except Exception as e:
            print(f"❌ Error processing batch {epoch_id}: {e}")

        finally:
            if conn and conn.is_connected():
                conn.close()

    # ------------------------------------------------------------------ #
    #  Stream entry-point                                                  #
    # ------------------------------------------------------------------ #

    def run(self):
        df_kafka = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe",        self.kafka_topic) \
            .option("startingOffsets",  "latest") \
            .load()

        schema = self.get_schema()

        df_transformed = df_kafka \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn(
                "event_timestamp",
                to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
            )

        checkpoint_dir = os.path.join(self.current_dir, "checkpoint_dir")
        os.makedirs(checkpoint_dir, exist_ok=True)

        query = df_transformed.writeStream \
            .outputMode("append") \
            .foreachBatch(self.process_batch) \
            .option("checkpointLocation", checkpoint_dir) \
            .start()

        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("⛔ Stopping Consumer.")


if __name__ == "__main__":
    RetailSparkLocal().run()