import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging

# ------------------------------
# Logging setup
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NormalizeInsert")

# ------------------------------
# PostgreSQL connection details
# ------------------------------
DB_CONFIG = {
    "dbname": "mydb",
    "user": "admin",
    "password": "admin123",
    "host": "192.168.1.8",
    "port": 5432
}

# ------------------------------
# Step 1: Read and normalize the CSV
# ------------------------------
def normalize_csv(file_path: str) -> pd.DataFrame:
    """
    Reads the anomaly CSV file and normalizes window_start and window_end columns.
    Splits them into 'date', 'start_time', and 'end_time'.
    """
    df = pd.read_csv(file_path)
    logger.info(f"Loaded {len(df)} rows from {file_path}")

    # Convert to datetime
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)

    # Extract date and time parts
    df["date"] = df["window_start"].dt.date
    df["start_time"] = df["window_start"].dt.time.astype(str)
    df["end_time"] = df["window_end"].dt.time.astype(str)

    # Drop the original columns
    df = df.drop(columns=["window_start", "window_end"])

    # Reorder columns for clarity
    cols = [
        "date", "start_time", "end_time", "event_count", "unique_messages",
        "distinct_hosts", "distinct_processes", "avg_msg_length",
        "failed_auth_count", "invalid_user_count", "entropy_tokens",
        "isolation_forest_label", "dbscan_label", "autoencoder_label", "ensemble_anomaly"
    ]
    df = df[cols]

    logger.info("Normalized datetime columns successfully.")
    return df

# ------------------------------
# Step 2: Create PostgreSQL table (if not exists)
# ------------------------------
def create_table(conn):
    """Creates the anomaly_results table if it doesn't already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS anomaly_results (
        id SERIAL PRIMARY KEY,
        date DATE,
        start_time TEXT,
        end_time TEXT,
        event_count INT,
        unique_messages INT,
        distinct_hosts INT,
        distinct_processes INT,
        avg_msg_length DOUBLE PRECISION,
        failed_auth_count INT,
        invalid_user_count INT,
        entropy_tokens DOUBLE PRECISION,
        isolation_forest_label INT,
        dbscan_label INT,
        autoencoder_label INT,
        ensemble_anomaly INT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    logger.info("Ensured table 'anomaly_results' exists.")

# ------------------------------
# Step 3: Insert normalized data
# ------------------------------
def insert_data(conn, df: pd.DataFrame):
    """Insert normalized DataFrame data into PostgreSQL table."""
    insert_query = """
    INSERT INTO anomaly_results (
        date, start_time, end_time, event_count, unique_messages,
        distinct_hosts, distinct_processes, avg_msg_length,
        failed_auth_count, invalid_user_count, entropy_tokens,
        isolation_forest_label, dbscan_label, autoencoder_label, ensemble_anomaly
    )
    VALUES %s;
    """

    # Convert DataFrame rows to list of tuples
    records = [tuple(x) for x in df.to_numpy()]

    with conn.cursor() as cur:
        execute_values(cur, insert_query, records)
        conn.commit()

    logger.info(f"Inserted {len(records)} rows into 'anomaly_results'.")

# ------------------------------
# Step 4: Main entry
# ------------------------------
if __name__ == "__main__":
    csv_file = "anomaly_results_with_features.csv"

    # Normalize CSV
    df = normalize_csv(csv_file)
    logger.info(f"Preview of normalized data:\n{df.head()}")

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to PostgreSQL successfully.")

        # Ensure table exists
        create_table(conn)

        # Insert data
        insert_data(conn, df)

    except Exception as e:
        logger.exception(f"Error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("PostgreSQL connection closed.")
