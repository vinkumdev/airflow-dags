from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow 3!")

dag = DAG(
    dag_id="hello_world_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"]
)

task = PythonOperator(
    task_id="say_hello",
    python_callable=hello_world,
    dag=dag,
)