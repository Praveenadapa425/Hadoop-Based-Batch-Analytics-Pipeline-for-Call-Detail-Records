from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="top_callers_by_spend_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cdr", "batch"],
) as dag:
    BashOperator(
        task_id="placeholder_top_callers",
        bash_command='echo "top callers pipeline placeholder"',
    )