from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def write_manifest(path: Path, *, job_name: str, run_id: str, input_path: str, output_path: str, input_record_count: int, output_record_count: int, status: str) -> None:
    payload = {
        "job_name": job_name,
        "run_id": run_id,
        "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": int(input_record_count),
        "output_record_count": int(output_record_count),
        "status": status,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
