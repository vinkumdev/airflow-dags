from airflow import DAG
from datetime import datetime
from file_transformer_plugin import JsonToCsvTransformerOperator

with DAG(
    dag_id="transform_json_to_csv",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False
) as dag:

    transform_task = JsonToCsvTransformerOperator(
        task_id="transform_api_json",
        url="https://jsonplaceholder.typicode.com/todos/1",
        output_path="/tmp/transformed_data.csv",
        extra_column_name="fetched_from",
        extra_column_value="jsonplaceholder"
    )
