from __future__ import annotations

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum as _sum, desc

from src.io import make_output_paths, resolve_input_path
from src.manifest import write_manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Top callers by total spend")
    parser.add_argument("--input", required=False, help="Input CSV path", default=None)
    parser.add_argument("--output", required=True, help="Output directory parent for this job")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_input_path(args.input)
    job_name = "top_callers_by_spend"
    run_id = args.run_id

    spark = SparkSession.builder.appName("top_callers_by_spend").getOrCreate()

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

    agg = df.groupBy("caller_id").agg(_sum("charge_amount").alias("total_spend"))
    top100 = agg.orderBy(desc("total_spend")).limit(100)

    out_dir, manifest_path = make_output_paths(args.output, run_id)
    out_path = Path(out_dir)

    # write single CSV file with caller_id,total_spend
    tmp_path = out_path / "tmp_top"
    tmp_path_str = str(tmp_path)
    top100.coalesce(1).write.csv(tmp_path_str, header=False, mode="overwrite")

    # move single part file to expected filename
    part_files = list(tmp_path.glob("part-*.csv"))
    if part_files:
        part = part_files[0]
        dest = out_path / "top_callers.csv"
        part.replace(dest)
    # cleanup tmp
    try:
        import shutil

        shutil.rmtree(tmp_path_str)
    except Exception:
        pass

    # write manifest
    output_count = top100.count()
    write_manifest(manifest_path, job_name=job_name, run_id=run_id, input_path=input_path, output_path=str(out_path), input_record_count=input_count, output_record_count=output_count, status="SUCCESS")


if __name__ == "__main__":
    main()
