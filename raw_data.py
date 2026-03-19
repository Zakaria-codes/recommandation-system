import mysql.connector as mysql
from mysql.connector import errorcode
import os
from dotenv import load_dotenv

load_dotenv()

config = {
    'host': os.getenv("HOST"),
    'user': os.getenv("USER"),
    'password': os.getenv("PASSWORD")
}

database = os.getenv("DATABASE")
table = "raw_data"

try:
    con = mysql.connect(**config)
    cur = con.cursor()
    print('Connecté au serveur local')

    try:
        cur.execute(f'USE {database}')
        print(f'Base de données {database} sélectionnée')
    except mysql.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"La base {database} n'existe pas. Création en cours...")
            cur.execute(f"CREATE DATABASE {database}")
            print(f" Base {database} créée")
            con.database = database
        else:
            print(err)
            exit(1)

    cur.execute(f"DROP TABLE IF EXISTS {table}")
    schema = f"""
    CREATE TABLE {table} (
        event_id VARCHAR(36) PRIMARY KEY,
        session_id VARCHAR(36),
        event_timestamp DATETIME,
        visitor_id INT,
        city VARCHAR(50),
        region VARCHAR(50),
        lat DECIMAL(10, 6),
        lon DECIMAL(10, 6),
        device_type VARCHAR(20),
        os VARCHAR(20),
        browser VARCHAR(50),
        product_id VARCHAR(20),
        product_name VARCHAR(100),
        category VARCHAR(50),
        subcategory VARCHAR(50),
        brand VARCHAR(50),
        price_unit DECIMAL(10, 2),
        event_type VARCHAR(20),
        quantity INT,
        total_amount DECIMAL(10, 2),
        payment_status VARCHAR(20),
        traffic_source VARCHAR(50),
        traffic_medium VARCHAR(50),
        campaign VARCHAR(100),
        is_promo BOOLEAN,
        ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cur.execute(schema)
    print(f"Table '{table}' créée avec succès !")
    cur.close()
    con.close()

except mysql.Error as error:
    print(f" Erreur critique : {error}")