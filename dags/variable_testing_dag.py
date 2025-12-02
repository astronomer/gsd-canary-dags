import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

with DAG(
    "variable_testing",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["Test Variables"],
):
    variables = [
        "environment",
        "log_level",
        "api_endpoint",
        "batch_size",
        "notification_channel",
        "example_config",
    ]

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for var in variables:

        @task(task_id=f"check_var_exists_{var}")
        def check_var_exists():
            try:
                Variable.get(var)
                logging.info(f"Variable {var} exists.")
            except Exception:
                logging.error(f"Could not find variable: {var}")
                raise

        start >> check_var_exists() >> end
