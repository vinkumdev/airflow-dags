from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
import requests
import pandas as pd
import io
import json
import os

# Config
CSV_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
CSV_PATH = "/tmp/pp_monthly.csv"  # temporary local path
POSTGRES_CONN_ID = "oxproperties_postgres"
TABLE_NAME = "price_paid"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1421529157560434728/QhHlXRPjx6HvOmCsCw2N0cot7WHxDMSiI97nF8tw9xvth3dnCgONNYYu9b1fCM1NuPmT"

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

    # Task 3: Load CSV into Postgres
    def load_csv_to_postgres():
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"{CSV_PATH} not found. Did download_csv task run successfully?")

        df = pd.read_csv(CSV_PATH)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        for row in df.itertuples(index=False, name=None):
            try:
                cursor.execute(f"""
                    INSERT INTO {TABLE_NAME} (
                        transaction_unique_identifier, price, date_of_transfer, postcode,
                        property_type, old_new, duration, paon, saon, street,
                        locality, town_city, district, county, ppd_category_type, record_status
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transaction_unique_identifier) DO NOTHING;
                """, row)
            except Exception as e:
                print(f"Skipping row {row[0]} due to error: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        print("CSV loaded into Postgres successfully.")

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
