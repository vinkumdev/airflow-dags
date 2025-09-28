from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time

# Config
API_URL = "https://los.rightmove.co.uk/typeahead"
POSTGRES_CONN_ID = "oxproperties_postgres"
BATCH_SIZE = 50        # increase if you want to process more each run
SLEEP_BETWEEN = 1      # seconds between API calls to be gentle
MARK_NO_MATCH_ID = -1  # r_id to set when no OUTCODE match found (optional)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def send_discord_message(msg: str):
    # Optional: hook this up to your webhook or remove
    print(msg)

def on_failure(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    send_discord_message(f"❌ DAG `{dag_id}` task `{task.task_id}` failed!")

# Ensure table exists (r_id as BIGINT for safety)
def ensure_table_exists():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rightmove_areas (
            outcode TEXT PRIMARY KEY,
            area_id BIGINT,
            display_name TEXT,
            last_updated_sale DATE
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table 'outcodes' is ready.")

# Fetch a batch of random unprocessed outcodes
def fetch_random_outcodes(cursor, limit=BATCH_SIZE):
    cursor.execute(
        "SELECT outcode FROM outcodes WHERE area_id IS NULL OR area_id = 0 ORDER BY RANDOM() LIMIT %s",
        (limit,),
    )
    return [row[0] for row in cursor.fetchall()]

# Update outcodes using case-insensitive match on outcode
def update_outcodes(cursor, conn, updates):
    """
    updates: list of tuples (r_id, display_name, original_outcode)
    Uses lower(outcode) = lower(%s) so case mismatch is handled.
    """
    query = "UPDATE outcodes SET area_id = %s, display_name = %s WHERE lower(outcode) = lower(%s)"
    try:
        cursor.executemany(query, updates)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Batch update failed: {e}")

def fetch_data_for_outcode(outcode):
    params = {"query": outcode, "limit": 20, "exclude": "STREET"}
    try:
        resp = requests.get(API_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("matches", [])
    except requests.RequestException as e:
        print(f"Error fetching data for {outcode}: {e}")
        return []

def process_outcodes_batch():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    outcodes = fetch_random_outcodes(cursor)
    if not outcodes:
        print("No outcodes to process (all done or none match criteria).")
        cursor.close()
        conn.close()
        return

    print(f"Processing {len(outcodes)} outcodes this run.")

    # We'll collect updates per outcode and execute them in small batches
    all_updates = []

    for outcode in outcodes:
        print(f"> Querying API for outcode: {outcode}")
        matches = fetch_data_for_outcode(outcode)

        # Find the first OUTCODE match (if any)
        outcode_match = next((m for m in matches if m.get("type") == "OUTCODE"), None)

        if outcode_match:
            match_id = outcode_match.get("id")
            display_name = outcode_match.get("displayName")
            # Try to cast numeric IDs to int, otherwise leave as None (NULL)
            try:
                match_id_val = int(match_id)
            except Exception:
                # some ids might be non-numeric (unlikely for OUTCODE), store NULL if not int
                match_id_val = None

            all_updates.append((match_id_val, display_name, outcode))
            print(f"  -> Found OUTCODE: id={match_id}, displayName={display_name}")
        else:
            # Optional: mark as processed but no match found so you won't retry forever
            all_updates.append((MARK_NO_MATCH_ID, None, outcode))
            print(f"  -> No OUTCODE match found for {outcode}; marking area_id={MARK_NO_MATCH_ID}")

        time.sleep(SLEEP_BETWEEN)

        # To avoid very large executemany payloads, flush every 200 updates
        if len(all_updates) >= 200:
            print(f"Flushing {len(all_updates)} updates to DB...")
            update_outcodes(cursor, conn, all_updates)
            all_updates = []

    # Flush remaining updates
    if all_updates:
        print(f"Flushing final {len(all_updates)} updates to DB...")
        update_outcodes(cursor, conn, all_updates)

    cursor.close()
    conn.close()
    print("Batch processing completed.")

# DAG
with DAG(
    dag_id="process_rightmove_outcodes",
    default_args=default_args,
    start_date=datetime(2025, 9, 28),
    schedule="0 2 * * 0",   # every Sunday at 02:00
    catchup=False,
    tags=["rightmove", "outcodes"],
) as dag:

    create_table_task = PythonOperator(
        task_id="ensure_table_exists",
        python_callable=ensure_table_exists,
        on_failure_callback=on_failure,
    )

    process_batch_task = PythonOperator(
        task_id="process_outcodes_batch",
        python_callable=process_outcodes_batch,
        on_failure_callback=on_failure,
    )

    create_table_task >> process_batch_task
