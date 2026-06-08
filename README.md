# Hadoop-Based Batch Analytics Pipeline for Call Detail Records

Production-style batch analytics pipeline for simulated telecom Call Detail Records (CDRs). The stack is fully containerized with Hadoop HDFS services, Spark standalone master/worker services, Apache Airflow orchestration, and a data-generator service.

## Architecture

- `data-generator` runs `data/generate_records.sh` and writes `/data/cdr_data.csv`.
- `namenode` and `datanode` provide the Hadoop storage layer required by the project.
- `spark-master` and `spark-worker` execute the PySpark batch jobs.
- `airflow` auto-discovers DAGs from `dags/` and submits Spark jobs with `spark-submit --master spark://spark-master:7077`.
- `run_pipeline.sh` maps a logical query name to the correct Airflow DAG and passes a timestamp `run_id`.

## Dataset

`data/generate_records.sh` creates at least 2,100,000 records with this schema:

```text
caller_id,receiver_id,duration_sec,tower_id,timestamp,call_type,charge_amount
```

The generated dataset includes `caller_whale_0001`, which accounts for at least 10% of all records to simulate data skew. It also includes deterministic anomalous caller groups so the anomaly detection job has verifiable output.

## Run

Start the full stack:

```bash
docker-compose up --build
```

Airflow is available at:

```text
http://localhost:8080
```

Default credentials:

```text
admin / admin
```

Trigger a logical query:

```bash
./run_pipeline.sh top_callers
./run_pipeline.sh tower_heatmap
./run_pipeline.sh anomalous_calls
./run_pipeline.sh revenue_recon
```

The script maps logical names to DAG IDs:

```text
top_callers      -> top_callers_by_spend_dag
tower_heatmap    -> tower_utilization_heatmap_dag
anomalous_calls  -> anomalous_call_detection_dag
revenue_recon    -> revenue_reconciliation_dag
```

## Outputs

Each run writes to:

```text
/output/top_callers_by_spend/{run_id}/
/output/tower_utilization_heatmap/{run_id}/
/output/anomalous_call_detection/{run_id}/
/output/revenue_reconciliation/{run_id}/
```

Each output directory contains result data and `_MANIFEST.json`:

```json
{
  "job_name": "string",
  "run_id": "string",
  "execution_timestamp_utc": "string",
  "input_path": "string",
  "output_path": "string",
  "input_record_count": 0,
  "output_record_count": 0,
  "status": "SUCCESS"
}
```

## Jobs

- `jobs/top_callers.py`: top 100 callers by total spend, sorted descending.
- `jobs/tower_heatmap.py`: number of calls per tower for each hour of day.
- `jobs/anomalous_calls.py`: per-caller duration anomalies where `abs(duration - mean) > 3 * stddev`.
- `jobs/revenue_recon.py`: total revenue across all CDR records.

The anomaly job uses an explicit `caller_id_partitioner` with Spark RDD `partitionBy` so all records for a caller are processed together before per-user statistics are calculated.
