from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values
import requests
import pandas as pd
import os
import json

# Config
CSV_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
CSV_PATH = "/tmp/pp_monthly.csv"
POSTGRES_CONN_ID = "oxproperties_postgres"
TABLE_NAME = "price_paid"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1421529157560434728/QhHlXRPjx6HvOmCsCw2N0cot7WHxDMSiI97nF8tw9xvth3dnCgONNYYu9b1fCM1NuPmT"

# Expected columns (for CSV without header)
COLUMN_NAMES = [
    "transaction_unique_identifier","price","date_of_transfer","postcode",
    "property_type","old_new","duration","paon","saon","street",
    "locality","town_city","district","county","ppd_category_type","record_status"
]

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Helper functions
def send_discord_message(message: str):
    data = {"content": message}
    try:
        requests.post(DISCORD_WEBHOOK, data=json.dumps(data), headers={"Content-Type": "application/json"})
    except Exception as e:
        print(f"Failed to send Discord notification: {e}")

def on_failure(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    try:
        send_discord_message(f"❌ DAG `{dag_id}` task `{task.task_id}` failed!")
    except Exception as e:
        print(f"Error sending failure notification: {e}")

# DAG
with DAG(
    dag_id="load_price_paid_data_full",
    default_args=default_args,
    schedule="0 2 25 * *",
    start_date=datetime(2025, 9, 25),
    catchup=False,
    tags=["land_registry", "postgres"],
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
                    date_of_transfer DATE,
                    postcode TEXT,
                    property_type TEXT,
                    old_new TEXT,
                    duration TEXT,
                    paon TEXT,
                    saon TEXT,
                    street TEXT,
                    locality TEXT,
                    town_city TEXT,
                    district TEXT,
                    county TEXT,
                    ppd_category_type TEXT,
                    record_status TEXT
                );
            """)
            conn.commit()
            print(f"Table {TABLE_NAME} created successfully")
        else:
            print(f"Table {TABLE_NAME} already exists")

        cursor.close()
        conn.close()

    # Task 2: Download CSV
    def download_csv():
        response = requests.get(CSV_URL)
        response.raise_for_status()
        with open(CSV_PATH, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"CSV downloaded to {CSV_PATH}")


    def load_csv_to_postgres():
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"{CSV_PATH} not found. Did download_csv task run successfully?")

        # Read CSV without header
        df = pd.read_csv(CSV_PATH, header=None, names=COLUMN_NAMES, encoding='utf-8-sig')
        df.columns = df.columns.str.strip()  # remove extra spaces

        # Clean data
        df['transaction_unique_identifier'] = df['transaction_unique_identifier'].str.replace(r"[{}]", "", regex=True)
        df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'], errors='coerce').dt.date
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Drop rows with essential missing data
        df = df.dropna(subset=['transaction_unique_identifier', 'date_of_transfer', 'price'])

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        batch_size = 10000  # insert 10k rows per batch
        total_rows = len(df)
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            rows = [tuple(x) for x in batch_df.to_numpy()]

            try:
                query = f"""
                    INSERT INTO {TABLE_NAME} (
                        transaction_unique_identifier, price, date_of_transfer, postcode,
                        property_type, old_new, duration, paon, saon, street,
                        locality, town_city, district, county, ppd_category_type, record_status
                    ) VALUES %s
                    ON CONFLICT (transaction_unique_identifier) DO NOTHING;
                """
                execute_values(cursor, query, rows)
                conn.commit()
                print(f"Inserted batch {i} to {i + len(rows)} successfully.")
            except Exception as e:
                conn.rollback()
                print(f"Batch {i} failed due to {e}. Skipping this batch and continuing.")

        cursor.close()
        conn.close()
        print("CSV loaded into Postgres successfully (with batch insert).")


    # Task 4: Send success notification
    def send_success_notification():
        send_discord_message("✅ Price Paid Data loaded successfully!")

    # Operators
    create_table_task = PythonOperator(
        task_id="ensure_table_exists",
        python_callable=ensure_table_exists,
        on_failure_callback=on_failure
    )

    download_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
        on_failure_callback=on_failure
    )

    load_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
        on_failure_callback=on_failure
    )

    notify_task = PythonOperator(
        task_id="send_success_notification",
        python_callable=send_success_notification
    )

    # Task dependencies
    create_table_task >> download_task >> load_task >> notify_task
