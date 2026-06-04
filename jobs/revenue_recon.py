from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum as _sum

from src.io import make_output_paths, resolve_input_path
from src.manifest import write_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Revenue reconciliation PySpark job")
    parser.add_argument("--input", required=False, help="Input CSV path", default=None)
    parser.add_argument("--output", required=True, help="Output directory for this job (parent, run_id appended)")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    job_name = "revenue_reconciliation"
    run_id = args.run_id

    spark = SparkSession.builder.appName("revenue_recon").getOrCreate()

    schema = StructType([
        StructField("caller_id", StringType(), True),
        StructField("receiver_id", StringType(), True),
        StructField("duration_sec", IntegerType(), True),
        StructField("tower_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("charge_amount", DoubleType(), True),
    ])

    df = spark.read.csv(input_path, schema=schema, header=False)
    input_count = df.count()

    total_row = df.agg(_sum("charge_amount").alias("total_revenue")).collect()[0]
    total_revenue = float(total_row["total_revenue"]) if total_row["total_revenue"] is not None else 0.0

    out_dir, manifest_path = make_output_paths(job_name, run_id)
    out_path = Path(out_dir)

    # write total as a single-line text file
    (out_path / "total_revenue.txt").write_text(f"{total_revenue}\n", encoding="utf-8")

    # write manifest
    write_manifest(manifest_path, job_name=job_name, run_id=run_id, input_path=input_path, output_path=str(out_path), input_record_count=input_count, output_record_count=1, status="SUCCESS")


if __name__ == "__main__":
    main()