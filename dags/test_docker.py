from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

@dag(
    dag_id="ecs_python_hello_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ecs", "python", "ghcr"],
)
def ecs_python_hello():
    """Run GHCR Python image on ECS using Fargate"""

    run_hello_task = EcsRunTaskOperator(
        task_id="run_python_hello",
        cluster="airflow-demo-cluster",
        task_definition="python-hello-task",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [
                {
                    "name": "python-hello",
                    "command": ["python", "app.py"],
                }
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets": ["subnet-xxxxxxxx"],  # replace with your subnet ID
                "assignPublicIp": "ENABLED"
            }
        },
        aws_conn_id="aws_default",
        region_name="eu-west-1"
    )

    run_hello_task  # declarative style, no need for .set_upstream()

ecs_python_hello()
