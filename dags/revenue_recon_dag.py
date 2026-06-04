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
    run_cmd = """
    RUN_ID={{ dag_run.conf.get('run_id') or ts_nodash }}
    echo "Running revenue_recon with run_id=$RUN_ID"
    spark-submit --master spark://spark-master:7077 /opt/cdr/jobs/revenue_recon.py --input /data/cdr_data.csv --output /output/revenue_reconciliation --run_id $RUN_ID
    """

    BashOperator(
        task_id="run_revenue_recon",
        bash_command=run_cmd,
    )