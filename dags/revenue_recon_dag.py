from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="revenue_reconciliation_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cdr", "batch"],
) as dag:
    BashOperator(
        task_id="placeholder_revenue_recon",
        bash_command='echo "revenue reconciliation pipeline placeholder"',
    )