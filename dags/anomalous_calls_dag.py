from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="anomalous_call_detection_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cdr", "batch"],
) as dag:
    BashOperator(
        task_id="placeholder_anomalous_calls",
        bash_command='echo "anomalous calls pipeline placeholder"',
    )