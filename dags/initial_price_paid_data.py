from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values
import requests
import pandas as pd
import io
import json

# Config
CSV_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
POSTGRES_CONN_ID = "oxproperties_postgres"
TABLE_NAME = "price_paid"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1421529157560434728/QhHlXRPjx6HvOmCsCw2N0cot7WHxDMSiI97nF8tw9xvth3dnCgONNYYu9b1fCM1NuPmT"

COLUMN_NAMES = [
    "transaction_unique_identifier","price","date_of_transfer","postcode",
    "property_type","old_new","duration","paon","saon","street",
    "locality","town_city","district","county","ppd_category_type","record_status"
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Discord helper
def send_discord_message(message: str):
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": message}, headers={"Content-Type": "application/json"})
    except Exception as e:
        print(f"Discord notification failed: {e}")

def on_failure(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    send_discord_message(f"❌ DAG `{dag_id}` task `{task.task_id}` failed!")

# DAG
with DAG(
    dag_id="initial_price_paid_data",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 9, 28),
    catchup=False,
    tags=["land_registry","postgres"],
) as dag:

    # Task 1: Ensure table exists
    def ensure_table_exists():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            );
        """, (TABLE_NAME,))
        exists = cursor.fetchone()[0]

        if not exists:
            cursor.execute(f"""
                CREATE TABLE {TABLE_NAME} (
                    transaction_unique_identifier TEXT PRIMARY KEY,
                    price NUMERIC,
                    date_of_transfer BIGINT,
                    postcode CHAR(8),
                    property_type CHAR(1),
                    old_new CHAR(1),
                    duration CHAR(1),
                    paon TEXT,
                    saon TEXT,
                    street TEXT,
                    locality TEXT,
                    town_city TEXT,
                    district TEXT,
                    county TEXT,
                    ppd_category_type CHAR(1),
                    record_status CHAR(1)
                );
                CREATE INDEX idx_postcode ON {TABLE_NAME} (postcode);
            """)
            conn.commit()
            print(f"Table {TABLE_NAME} created successfully")
        else:
            print(f"Table {TABLE_NAME} already exists")

        cursor.close()
        conn.close()

    # Task 2: Stream CSV and insert in batches
    def stream_and_load_csv():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Stream CSV
        response = requests.get(CSV_URL, stream=True)
        response.raise_for_status()

        chunksize = 100000  # rows per chunk
        buffer = ""
        rows_in_buffer = 0

        for line in response.iter_lines(decode_unicode=True):
            if line:
                buffer += line + "\n"
                rows_in_buffer += 1

            if rows_in_buffer >= chunksize:
                df_chunk = pd.read_csv(io.StringIO(buffer), header=None, names=COLUMN_NAMES)
                buffer = ""
                rows_in_buffer = 0

                # Clean chunk
                df_chunk['transaction_unique_identifier'] = df_chunk['transaction_unique_identifier'].str.replace(r"[{}]", "", regex=True)
                df_chunk['date_of_transfer'] = pd.to_datetime(df_chunk['date_of_transfer'], errors='coerce').dt.strftime('%Y%m%d').astype(float)
                df_chunk['price'] = pd.to_numeric(df_chunk['price'], errors='coerce')
                df_chunk = df_chunk.dropna(subset=['transaction_unique_identifier','date_of_transfer','price'])

                # Batch insert
                batch_rows = [tuple(x) for x in df_chunk.to_numpy()]
                try:
                    execute_values(cursor, f"""
                        INSERT INTO {TABLE_NAME} (
                            transaction_unique_identifier, price, date_of_transfer, postcode,
                            property_type, old_new, duration, paon, saon, street,
                            locality, town_city, district, county, ppd_category_type, record_status
                        ) VALUES %s
                    """, batch_rows)
                    conn.commit()
                    print(f"Inserted batch of {len(batch_rows)} rows")
                except Exception as e:
                    conn.rollback()
                    print(f"Batch insert failed: {e}")

        # Insert remaining rows
        if buffer.strip():
            df_chunk = pd.read_csv(io.StringIO(buffer), header=None, names=COLUMN_NAMES)
            df_chunk['transaction_unique_identifier'] = df_chunk['transaction_unique_identifier'].str.replace(r"[{}]", "", regex=True)
            df_chunk['date_of_transfer'] = pd.to_datetime(df_chunk['date_of_transfer'], errors='coerce').dt.strftime('%Y%m%d').astype(float)
            df_chunk['price'] = pd.to_numeric(df_chunk['price'], errors='coerce')
            df_chunk = df_chunk.dropna(subset=['transaction_unique_identifier','date_of_transfer','price'])
            batch_rows = [tuple(x) for x in df_chunk.to_numpy()]
            if batch_rows:
                execute_values(cursor, f"""
                    INSERT INTO {TABLE_NAME} (
                        transaction_unique_identifier, price, date_of_transfer, postcode,
                        property_type, old_new, duration, paon, saon, street,
                        locality, town_city, district, county, ppd_category_type, record_status
                    ) VALUES %s
                """, batch_rows)
                conn.commit()
                print(f"Inserted final batch of {len(batch_rows)} rows")

        cursor.close()
        conn.close()
        print("CSV loaded successfully!")

    # Task 3: Send success notification
    def send_success_notification():
        send_discord_message("✅ Initial Price Paid Data loaded successfully!")

    # Operators
    create_table_task = PythonOperator(
        task_id="ensure_table_exists",
        python_callable=ensure_table_exists,
        on_failure_callback=on_failure
    )

    load_csv_task = PythonOperator(
        task_id="stream_and_load_csv",
        python_callable=stream_and_load_csv,
        on_failure_callback=on_failure
    )

    notify_task = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_success_notification
    )

    # Task dependencies
    create_table_task >> load_csv_task >> notify_task
