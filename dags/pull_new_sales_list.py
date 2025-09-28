from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

import requests
import json
import re
import time
from datetime import datetime, timedelta

# Config
POSTGRES_CONN_ID = "oxproperties_postgres"
TABLE_SALES = "sales_properties"
TABLE_AREAS = "rightmove_areas"
API_BASE = "https://www.rightmove.co.uk/property-for-sale/find.html"
BATCH_AREAS = 5            # number of areas to process per DAG run
PAGE_SLEEP = 0.5           # sleep between page requests (seconds)
AREA_SLEEP = 1             # sleep between area requests (seconds)
REQUEST_TIMEOUT = 15       # requests timeout (seconds)
CHUNK_FLUSH = 200          # how many rows to collect before flushing (not used for execute_values here)
SCHEDULE = "0 2 * * *"     # daily at 02:00 — change as needed

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def send_discord_message(msg: str):
    # Optional — wire to your webhook or remove
    print(msg)

def on_failure(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    send_discord_message(f"❌ DAG `{dag_id}` task `{task.task_id}` failed!")

# Utility: extract JSON object following `window.jsonModel = {...};` robustly
def _extract_json_from_script(script_text: str, marker: str = "window.jsonModel"):
    start = script_text.find(marker)
    if start == -1:
        return None
    # find first '{' after marker
    brace_start = script_text.find("{", start)
    if brace_start == -1:
        return None
    i = brace_start
    depth = 0
    while i < len(script_text):
        if script_text[i] == "{":
            depth += 1
        elif script_text[i] == "}":
            depth -= 1
            if depth == 0:
                # include the closing brace
                json_text = script_text[brace_start:i+1]
                return json_text
        i += 1
    return None

def ensure_tables():
    """
    Create sales_properties table if not exists with unique constraint on property_id.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SALES} (
            id SERIAL PRIMARY KEY,
            property_id TEXT UNIQUE,
            is_processed BOOLEAN DEFAULT FALSE,
            created_date INTEGER,
            updated_date INTEGER
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Table {TABLE_SALES} ready.")

def fetch_random_areas(cur, limit=BATCH_AREAS):
    """
    Return list of tuples (area_id, outcode) for areas needing update.
    We use yesterday threshold (<= yesterday) so areas older than a day are eligible.
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    cur.execute(
        f"""
        SELECT area_id, outcode
        FROM {TABLE_AREAS}
        WHERE last_updated_sale IS NULL OR last_updated_sale <= %s
        ORDER BY RANDOM()
        LIMIT %s
        """,
        (yesterday, limit),
    )
    return cur.fetchall()

def fetch_page_properties(base_url, params, headers):
    """
    Fetch a single page, parse property ids, and return (properties_list, pagination_info)
    properties_list = list of dicts {property_id: '12345'}
    pagination_info is parsed JSON pagination if found, else None
    """
    try:
        resp = requests.get(base_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        print(f"HTTP error fetching {params.get('searchLocation')} index {params.get('index')}: {e}")
        return [], None

    soup = BeautifulSoup(resp.text, "html.parser")
    properties = []

    # Rightmove HTML structure can vary; try expected container
    for listing in soup.find_all("div", class_=lambda c: c and "l-searchResult" in c):
        try:
            a = listing.find("a", class_="propertyCard-link")
            if not a:
                a = listing.find("a", href=True)
            href = a.get("href")
            if not href:
                continue
            full_link = href if href.startswith("http") else f"https://www.rightmove.co.uk{href}"
            m = re.search(r"/properties/(\d+)", full_link)
            if m:
                property_id = m.group(1)
                properties.append({"property_id": property_id})
        except Exception:
            continue

    # find script tag with window.jsonModel and parse pagination if possible
    script_tag = None
    for s in soup.find_all("script"):
        text = s.string
        if not text:
            continue
        if "window.jsonModel" in text:
            script_tag = text
            break

    pagination_info = None
    if script_tag:
        try:
            json_text = _extract_json_from_script(script_tag, marker="window.jsonModel")
            if json_text:
                parsed = json.loads(json_text)
                pagination_info = parsed.get("pagination", {})
        except Exception as e:
            print(f"Failed to parse window.jsonModel JSON: {e}")

    return properties, pagination_info

def get_rightmove_properties(search_location, area_id, radius=3):
    """
    Walk through pagination and return deduplicated list of property dicts.
    """
    params = {
        "searchLocation": search_location,
        "useLocationIdentifier": "true",
        "locationIdentifier": f"OUTCODE^{area_id}",
        "radius": radius,
        "_includeSSTC": "on",
        "sortType": "1",
        "propertyTypes": "",
        "maxDaysSinceAdded": "1",
        "includeSSTC": "true",
        "mustHave": "",
        "dontShow": "newHome,retirement,sharedOwnership",
        "index": 0,
    }
    ua = UserAgent()
    headers = {"User-Agent": ua.random}

    all_props = []
    seen = set()
    page_count = 0

    while True:
        page_count += 1
        props, pagination = fetch_page_properties(API_BASE, params, headers)
        for p in props:
            pid = p.get("property_id")
            if pid and pid not in seen:
                seen.add(pid)
                all_props.append({"property_id": pid})

        # if no pagination info or no next, stop
        if not pagination or not pagination.get("next"):
            break

        # Advance index (Rightmove uses index offset in results; typical page size 24)
        params["index"] = params.get("index", 0) + 24
        time.sleep(PAGE_SLEEP)

        # safety guard to avoid infinite loops
        if page_count > 200:
            print("Page loop safety break after 200 pages")
            break

    return all_props

def process_sales():
    """
    Main DAG function — processes a batch of areas and inserts property ids.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # pick areas to process
    areas = fetch_random_areas(cur, limit=BATCH_AREAS)
    if not areas:
        print("No eligible areas found to update.")
        cur.close()
        conn.close()
        return

    today_int = int(datetime.now().strftime("%Y%m%d"))

    for (area_id, outcode) in areas:
        try:
            print(f"Processing area {area_id} / outcode {outcode}")
            props = get_rightmove_properties(search_location=outcode, area_id=area_id)
            print(f"Found {len(props)} properties for {outcode}")

            if not props:
                # still update last_updated_sale so it won't be retried every run
                cur.execute(
                    f"UPDATE {TABLE_AREAS} SET last_updated_sale = %s WHERE area_id = %s",
                    (today_int, area_id),
                )
                conn.commit()
                time.sleep(AREA_SLEEP)
                continue

            # Prepare rows for bulk upsert
            rows = []
            for p in props:
                pid = p.get("property_id")
                if pid:
                    rows.append((pid, today_int, today_int))

            # Upsert: insert new rows, update updated_date for existing ones
            insert_query = f"""
                INSERT INTO {TABLE_SALES} (property_id, created_date, updated_date)
                VALUES %s
                ON CONFLICT (property_id) DO UPDATE
                SET updated_date = EXCLUDED.updated_date
            """
            try:
                execute_values(cur, insert_query, rows)
                conn.commit()
                print(f"Inserted/updated {len(rows)} rows for area {area_id}")
            except Exception as e:
                conn.rollback()
                print(f"Bulk insert failed for area {area_id}: {e}")

            # Mark area as updated
            cur.execute(
                f"UPDATE {TABLE_AREAS} SET last_updated_sale = %s WHERE area_id = %s",
                (today_int, area_id),
            )
            conn.commit()

            time.sleep(AREA_SLEEP)

        except Exception as e:
            print(f"Error processing area {area_id}: {e}")
            # don't abort whole run — continue with next area
            try:
                conn.rollback()
            except Exception:
                pass

    cur.close()
    conn.close()
    print("Processing of batch completed.")

# DAG definition
with DAG(
    dag_id="pull_new_sales_list",
    default_args=default_args,
    start_date=datetime(2025, 9, 28),
    schedule=SCHEDULE,   # daily at 02:00
    catchup=False,
    tags=["rightmove", "sales"],
) as dag:

    create_tables_task = PythonOperator(
        task_id="ensure_tables",
        python_callable=ensure_tables,
        on_failure_callback=on_failure,
    )

    fetch_sales_task = PythonOperator(
        task_id="process_sales_batch",
        python_callable=process_sales,
        on_failure_callback=on_failure,
    )

    create_tables_task >> fetch_sales_task
