from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def query_postgres():
    # Create Postgres connection (must exist in Airflow Connections UI as 'my_postgres')
    hook = PostgresHook(postgres_conn_id="oxproperties_postgres")

    # Run query
    sql = "SELECT * FROM sample_data LIMIT 10;"
    results = hook.get_records(sql)

    # Print results
    for row in results:
        print(row)


with DAG(
        dag_id="postgres_query_example",
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
        catchup=False,
) as dag:
    query_task = PythonOperator(
        task_id="query_postgres_task",
        python_callable=query_postgres,
    )
