#!/usr/bin/env bash
set -euo pipefail

OUTPUT_FILE="${OUTPUT_FILE:-/data/cdr_data.csv}"
TOTAL_RECORDS="${TOTAL_RECORDS:-2100000}"
WHALE_CALLER_ID="${WHALE_CALLER_ID:-caller_whale_0001}"
WHALE_RATIO="${WHALE_RATIO:-0.12}"
SEED="${SEED:-42}"

mkdir -p "$(dirname "$OUTPUT_FILE")"

PYTHON_BIN=""
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "python3 or python is required to generate the dataset" >&2
  exit 1
fi

"$PYTHON_BIN" - "$OUTPUT_FILE" "$TOTAL_RECORDS" "$WHALE_CALLER_ID" "$WHALE_RATIO" "$SEED" <<'PY'
import csv
import datetime as dt
import random
import sys

output_file = sys.argv[1]
total_records = int(sys.argv[2])
whale_caller_id = sys.argv[3]
whale_ratio = float(sys.argv[4])
seed = int(sys.argv[5])

if total_records < 2_000_000:
	raise SystemExit("TOTAL_RECORDS must be at least 2000000")

random.seed(seed)
whale_records = max(int(total_records * whale_ratio), total_records // 10)
whale_records = min(whale_records, total_records)
other_records = total_records - whale_records

call_types = ("VOICE", "SMS", "DATA")
towers = [f"tower_{i:04d}" for i in range(1, 501)]
base_time = dt.datetime(2026, 1, 1, 0, 0, 0)


def make_timestamp(index: int) -> str:
	stamp = base_time + dt.timedelta(seconds=index * 17)
	return stamp.isoformat(timespec="seconds") + "Z"


def make_caller(index: int) -> str:
	return f"caller_{index:07d}"


def make_receiver(index: int) -> str:
	return f"receiver_{index:07d}"


with open(output_file, "w", newline="", encoding="utf-8") as handle:
	writer = csv.writer(handle)
	for index in range(whale_records):
		duration_sec = random.randint(30, 1800)
		call_type = random.choice(call_types)
		if call_type == "VOICE":
			charge_amount = round(duration_sec * 0.0045 + random.uniform(0.1, 1.25), 2)
		elif call_type == "SMS":
			charge_amount = round(random.uniform(0.01, 0.20), 2)
		else:
			charge_amount = round(duration_sec * 0.0012 + random.uniform(0.25, 1.80), 2)

		writer.writerow([
			whale_caller_id,
			make_receiver(index),
			duration_sec,
			random.choice(towers),
			make_timestamp(index),
			call_type,
			charge_amount,
		])

	for index in range(other_records):
		caller_id = make_caller(index + 1)
		if caller_id == whale_caller_id:
			caller_id = f"caller_{index + 1000000:07d}"

		duration_sec = random.randint(15, 2400)
		call_type = random.choices(call_types, weights=(65, 20, 15), k=1)[0]
		if call_type == "VOICE":
			charge_amount = round(duration_sec * 0.0042 + random.uniform(0.05, 1.10), 2)
		elif call_type == "SMS":
			charge_amount = round(random.uniform(0.01, 0.15), 2)
		else:
			charge_amount = round(duration_sec * 0.0010 + random.uniform(0.20, 1.50), 2)

		writer.writerow([
			caller_id,
			make_receiver(index + whale_records),
			duration_sec,
			random.choice(towers),
			make_timestamp(index + whale_records),
			call_type,
			charge_amount,
		])
PY
