from __future__ import annotations

import argparse
import math
from pathlib import Path
import zlib

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.io import make_output_paths, resolve_input_path
from src.manifest import write_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Anomalous call detection per caller (>3 sigma)")
    parser.add_argument("--input", required=False, help="Input CSV path", default=None)
    parser.add_argument("--output", required=True, help="Output directory parent for this job")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    return parser.parse_args()


def caller_id_partitioner(caller_id: str) -> int:
    """Stable custom partitioner for caller-level stateful processing."""
    return zlib.crc32(caller_id.encode("utf-8"))


def detect_anomalies_in_partition(records):
    by_caller = {}
    for caller_id, payload in records:
        by_caller.setdefault(caller_id, []).append(payload)

    for caller_id, calls in by_caller.items():
        durations = [duration for _, duration in calls]
        count = len(durations)
        if count < 2:
            continue

        mean_duration = sum(durations) / count
        variance = sum((duration - mean_duration) ** 2 for duration in durations) / (count - 1)
        stddev_duration = math.sqrt(variance)
        if stddev_duration == 0:
            continue

        threshold = 3 * stddev_duration
        for timestamp, duration in calls:
            if abs(duration - mean_duration) > threshold:
                yield (
                    caller_id,
                    timestamp,
                    int(duration),
                    float(mean_duration),
                    float(stddev_duration),
                )


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

    partition_count = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
    keyed_calls = df.select("caller_id", "timestamp", "duration_sec").rdd.map(
        lambda row: (row["caller_id"], (row["timestamp"], int(row["duration_sec"])))
    )
    partitioned_calls = keyed_calls.partitionBy(partition_count, caller_id_partitioner)
    anomaly_rows = partitioned_calls.mapPartitions(detect_anomalies_in_partition)

    output_schema = StructType([
        StructField("caller_id", StringType(), False),
        StructField("call_timestamp", StringType(), False),
        StructField("duration_sec", IntegerType(), False),
        StructField("user_mean_duration", DoubleType(), False),
        StructField("user_stddev", DoubleType(), False),
    ])
    out_df = spark.createDataFrame(anomaly_rows, schema=output_schema)
    output_count = out_df.count()

    out_dir, manifest_path = make_output_paths(args.output, run_id)
    out_path = Path(out_dir)

    tmp_path = out_path / "tmp_anom"
    tmp_path_str = str(tmp_path)
    out_df.coalesce(1).write.csv(tmp_path_str, header=False, mode="overwrite")

    part_files = list(tmp_path.glob("part-*.csv"))
    if part_files:
        part = part_files[0]
        dest = out_path / "anomalous_calls.csv"
        part.replace(dest)

    try:
        import shutil

        shutil.rmtree(tmp_path_str)
    except Exception:
        pass

    write_manifest(manifest_path, job_name=job_name, run_id=run_id, input_path=input_path, output_path=str(out_path), input_record_count=input_count, output_record_count=output_count, status="SUCCESS")


if __name__ == "__main__":
    main()
