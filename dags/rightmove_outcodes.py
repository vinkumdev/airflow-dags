from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time

# Config
API_URL = "https://los.rightmove.co.uk/typeahead"
POSTGRES_CONN_ID = "oxproperties_postgres"
BATCH_SIZE = 10  # number of outcodes per DAG run
SLEEP_BETWEEN = 1  # seconds between API calls

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Helper functions
def send_discord_message(message: str):
    # Optional: add Discord notifications if needed
    print(message)

def on_failure(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    send_discord_message(f"❌ DAG `{dag_id}` task `{task.task_id}` failed!")

def ensure_table_exists():
    """
    Create 'outcodes' table if it doesn't exist
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS outcodes (
            outcode TEXT PRIMARY KEY,
            r_id INTEGER,
            display_name TEXT
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'outcodes' is ready.")

def fetch_random_outcodes(cursor, limit=BATCH_SIZE):
    cursor.execute(
        f"SELECT outcode FROM outcodes WHERE r_id IS NULL OR r_id = 0 ORDER BY RANDOM() LIMIT {limit}"
    )
    return [row[0] for row in cursor.fetchall()]

def update_outcodes(cursor, conn, updates):
    query = "UPDATE outcodes SET r_id = %s, display_name = %s WHERE outcode = %s"
    cursor.executemany(query, updates)
    conn.commit()

def fetch_data_for_outcode(outcode):
    params = {"query": outcode, "limit": 20, "exclude": "STREET"}
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json().get("matches", [])
    except requests.RequestException as e:
        print(f"Error fetching data for {outcode}: {e}")
        return []

def process_outcodes_batch():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    outcodes = fetch_random_outcodes(cursor)
    if not outcodes:
        print("All outcodes have been processed.")
        cursor.close()
        conn.close()
        return

    for outcode in outcodes:
        print(f"Fetching data for outcode: {outcode}")
        matches = fetch_data_for_outcode(outcode)

        updates = [
            (match["id"], match["displayName"], outcode)
            for match in matches if match["type"] == "OUTCODE"
        ]

        if updates:
            update_outcodes(cursor, conn, updates)
            print(f"Updated {len(updates)} rows for outcode: {outcode}")
        else:
            print(f"No valid matches for outcode: {outcode}")

        time.sleep(SLEEP_BETWEEN)

    cursor.close()
    conn.close()
    print("Batch processing completed.")

# DAG definition
with DAG(
    dag_id="process_rightmove_outcodes",
    default_args=default_args,
    start_date=datetime(2025, 9, 28),
    schedule="0 2 * * 0",   # Every Sunday 2 AM
    catchup=False,
    tags=["rightmove", "outcodes"],
) as dag:

    create_table_task = PythonOperator(
        task_id="ensure_table_exists",
        python_callable=ensure_table_exists,
        on_failure_callback=on_failure
    )

    process_batch_task = PythonOperator(
        task_id="process_outcodes_batch",
        python_callable=process_outcodes_batch,
        on_failure_callback=on_failure
    )

    # Task dependencies
    create_table_task >> process_batch_task
