from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col, abs as _abs

from src.io import make_output_paths, resolve_input_path
from src.manifest import write_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Anomalous call detection per caller (>3 sigma)")
    parser.add_argument("--input", required=False, help="Input CSV path", default=None)
    parser.add_argument("--output", required=True, help="Output directory parent for this job")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    job_name = "anomalous_call_detection"
    run_id = args.run_id

    spark = SparkSession.builder.appName("anomalous_call_detection").getOrCreate()

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

    # Ensure all records for a caller_id are co-located by repartitioning by caller_id
    df = df.repartition(col("caller_id"))

    stats = df.groupBy("caller_id").agg(_mean("duration_sec").alias("user_mean_duration"), _stddev("duration_sec").alias("user_stddev"))

    joined = df.join(stats, on="caller_id", how="inner")

    anomalies = joined.filter(_abs(col("duration_sec") - col("user_mean_duration")) > 3 * col("user_stddev"))

    # select required schema: caller_id, call_timestamp, duration_sec, user_mean_duration, user_stddev
    out_df = anomalies.select(
        col("caller_id"),
        col("timestamp").alias("call_timestamp"),
        col("duration_sec"),
        col("user_mean_duration"),
        col("user_stddev"),
    )

    out_dir, manifest_path = make_output_paths(job_name, run_id)
    out_path = Path(out_dir)

    tmp_path = out_path / "tmp_anom"
    tmp_path_str = str(tmp_path)
    out_df.coalesce(1).write.csv(tmp_path_str, header=False, mode="overwrite")

    part_files = list(tmp_path.glob("part-*.csv"))
    output_count = 0
    if part_files:
        part = part_files[0]
        dest = out_path / "anomalous_calls.csv"
        part.replace(dest)
        output_count = sum(1 for _ in dest.read_text(encoding="utf-8").splitlines())

    try:
        import shutil

        shutil.rmtree(tmp_path_str)
    except Exception:
        pass

    write_manifest(manifest_path, job_name=job_name, run_id=run_id, input_path=input_path, output_path=str(out_path), input_record_count=input_count, output_record_count=output_count, status="SUCCESS")


if __name__ == "__main__":
    main()