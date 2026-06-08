from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, count as _count, substring

from src.io import make_output_paths, resolve_input_path
from src.manifest import write_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tower utilization heatmap: calls per tower per hour")
    parser.add_argument("--input", required=False, help="Input CSV path", default=None)
    parser.add_argument("--output", required=True, help="Output directory parent for this job")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    job_name = "tower_utilization_heatmap"
    run_id = args.run_id

    spark = SparkSession.builder.appName("tower_utilization_heatmap").getOrCreate()

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

    # extract hour_of_day from ISO timestamp like 2026-01-01T00:00:00Z -> substring pos 12 length 2
    hour_col = substring(col("timestamp"), 12, 2).cast("int").alias("hour_of_day")
    df2 = df.select(col("tower_id"), hour_col)

    agg = df2.groupBy("tower_id", "hour_of_day").agg(_count("tower_id").alias("call_count"))

    out_dir, manifest_path = make_output_paths(args.output, run_id)
    out_path = Path(out_dir)

    tmp_path = out_path / "tmp_heat"
    tmp_path_str = str(tmp_path)
    agg.coalesce(1).write.csv(tmp_path_str, header=False, mode="overwrite")

    part_files = list(tmp_path.glob("part-*.csv"))
    if part_files:
        part = part_files[0]
        dest = out_path / "tower_heatmap.csv"
        part.replace(dest)

    try:
        import shutil

        shutil.rmtree(tmp_path_str)
    except Exception:
        pass

    write_manifest(manifest_path, job_name=job_name, run_id=run_id, input_path=input_path, output_path=str(out_path), input_record_count=input_count, output_record_count=agg.count(), status="SUCCESS")


if __name__ == "__main__":
    main()
