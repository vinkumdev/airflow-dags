from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import requests
import pandas as pd
import io
import json

# Config
CSV_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
POSTGRES_CONN_ID = "oxproperties_postgres"
TABLE_NAME = "price_paid_data"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1421529157560434728/QhHlXRPjx6HvOmCsCw2N0cot7WHxDMSiI97nF8tw9xvth3dnCgONNYYu9b1fCM1NuPmT"

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# DAG
with DAG(
    dag_id="load_price_paid_data",
    default_args=default_args,
    schedule="0 2 25 * *",  # 25th of each month at 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["land_registry", "postgres"],
) as dag:

    def download_and_load():
        # Download CSV
        response = requests.get(CSV_URL)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))

        # Connect to Postgres
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                transaction_unique_identifier TEXT,
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

        # Insert data
        for _, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {TABLE_NAME} (
                    transaction_unique_identifier, price, date_of_transfer, postcode,
                    property_type, old_new, duration, paon, saon, street,
                    locality, town_city, district, county, ppd_category_type, record_status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
            """, tuple(row))
        conn.commit()
        cursor.close()
        conn.close()

    def send_discord_notification():
        data = {"content": "✅ Price Paid Data loaded successfully into Postgres!"}
        response = requests.post(DISCORD_WEBHOOK, data=json.dumps(data), headers={"Content-Type": "application/json"})
        response.raise_for_status()

    load_task = PythonOperator(
        task_id="download_and_load_csv",
        python_callable=download_and_load
    )

    notify_task = PythonOperator(
        task_id="send_discord_notification",
        python_callable=send_discord_notification
    )

    load_task >> notify_task
