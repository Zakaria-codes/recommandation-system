"""
star_schema_init.py
-------------------
Run ONCE before the streaming pipeline starts.
Creates all Star Schema tables in MySQL with correct PRIMARY KEYS
so that INSERT IGNORE works properly from the first batch.
"""

import os
import sys
import mysql.connector
from dotenv import load_dotenv

load_dotenv(override=True)

# ------------------------------------------------------------------ #
#  DDL statements                                                      #
# ------------------------------------------------------------------ #

DDL_STATEMENTS = [
    # ── Dimension : Product ───────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id   VARCHAR(64)  NOT NULL,
        product_name VARCHAR(256),
        category     VARCHAR(128),
        subcategory  VARCHAR(128),
        brand        VARCHAR(128),
        PRIMARY KEY (product_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── Dimension : Location ──────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id  BIGINT       NOT NULL,
        city         VARCHAR(128),
        region       VARCHAR(128),
        lat          DOUBLE,
        lon          DOUBLE,
        PRIMARY KEY (location_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── Dimension : Device ────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dim_device (
        device_id    BIGINT       NOT NULL,
        device_type  VARCHAR(64),
        os           VARCHAR(64),
        browser      VARCHAR(64),
        PRIMARY KEY (device_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,

    # ── Fact : Events ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fact_events (
        event_id       VARCHAR(64)   NOT NULL,
        session_id     VARCHAR(64),
        event_timestamp DATETIME(6),
        visitor_id     VARCHAR(64),
        product_id     VARCHAR(64),
        location_id    BIGINT,
        device_id      BIGINT,
        event_type     VARCHAR(64),
        price_unit     DOUBLE,
        quantity       INT,
        total_amount   DOUBLE,
        payment_status VARCHAR(64),
        traffic_source VARCHAR(128),
        is_promo       BOOLEAN,
        PRIMARY KEY (event_id),
        INDEX idx_product  (product_id),
        INDEX idx_location (location_id),
        INDEX idx_device   (device_id),
        INDEX idx_ts       (event_timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("HOST"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database=os.getenv("DATABASE"),
    )


def ensure_primary_key(cursor, table_name, primary_key_col):
    """
    Add a PRIMARY KEY to an existing table if it doesn't already have one.

    If the table already has duplicate values on primary_key_col (e.g. created
    by Spark JDBC without a PK), ALTER TABLE fails. We deduplicate first:
      1. Create a clean copy with GROUP BY to keep one row per PK value.
      2. Swap it in place of the original.
      3. Add the PRIMARY KEY on the clean table.
    """
    cursor.execute(f"""
        SELECT COUNT(1)
        FROM information_schema.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA    = DATABASE()
          AND TABLE_NAME      = '{table_name}'
          AND CONSTRAINT_TYPE = 'PRIMARY KEY'
    """)
    if cursor.fetchone()[0] > 0:
        return  # PK already exists — nothing to do

    print(f"  ⚙️   No PK on '{table_name}'. Deduplicating before ALTER...")
    tmp = f"{table_name}_dedup_tmp"
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {tmp}")
        cursor.execute(f"CREATE TABLE {tmp} LIKE {table_name}")
        cursor.execute(f"""
            INSERT INTO {tmp}
            SELECT * FROM {table_name}
            GROUP BY {primary_key_col}
        """)
        cursor.execute(f"DROP TABLE {table_name}")
        cursor.execute(f"RENAME TABLE {tmp} TO {table_name}")
        cursor.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_col})")
        print(f"  🔑  Primary key added on '{table_name}' ({primary_key_col}) — duplicates removed.")
    except Exception as e:
        # Clean up temp table if something went wrong
        cursor.execute(f"DROP TABLE IF EXISTS {tmp}")
        print(f"  ⚠️   Could not add primary key on '{table_name}': {e}")


# ------------------------------------------------------------------ #
#  Main                                                                #
# ------------------------------------------------------------------ #

def main():
    print("=" * 55)
    print("  Star Schema Init — MySQL table setup")
    print("=" * 55)

    required_env = ["HOST", "USER", "PASSWORD", "DATABASE"]
    missing = [k for k in required_env if not os.getenv(k)]
    if missing:
        print(f"\n❌  Missing env variables: {', '.join(missing)}")
        print("    Check your .env file.\n")
        sys.exit(1)

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        print(f"\n✅  Connected to MySQL — database: {os.getenv('DATABASE')}\n")
    except Exception as e:
        print(f"\n❌  Cannot connect to MySQL: {e}\n")
        sys.exit(1)

    # 1. Create tables from DDL
    for ddl in DDL_STATEMENTS:
        table_name = ddl.strip().split()[5]          # CREATE TABLE IF NOT EXISTS <name>
        try:
            cursor.execute(ddl)
            print(f"  ✔  {table_name}")
        except Exception as e:
            print(f"  ✖  {table_name} — {e}")

    conn.commit()

    # 2. Safety net: patch any pre-existing table that was created without a PK
    pk_map = {
        "dim_product":  "product_id",
        "dim_location": "location_id",
        "dim_device":   "device_id",
        "fact_events":  "event_id",
    }
    print("\nChecking PRIMARY KEYS on existing tables...")
    for table, pk_col in pk_map.items():
        ensure_primary_key(cursor, table, pk_col)

    conn.commit()
    cursor.close()
    conn.close()

    print("\n✅  Star Schema ready. Starting pipeline...\n")
    sys.exit(0)


if __name__ == "__main__":
    main()