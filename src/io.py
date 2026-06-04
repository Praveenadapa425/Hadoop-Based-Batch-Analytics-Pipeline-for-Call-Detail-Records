from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple


def make_output_paths(job_dir: str, run_id: str, output_root: str = "/output") -> Tuple[Path, Path]:
    """Create and return (output_dir, manifest_path).

    output_dir: /output/{job_dir}/{run_id}
    manifest_path: output_dir/_MANIFEST.json
    """
    out = Path(output_root) / job_dir / run_id
    out.mkdir(parents=True, exist_ok=True)
    manifest = out / "_MANIFEST.json"
    return out, manifest


def resolve_input_path(env_path: str | None) -> str:
    if env_path:
        return env_path
    # default input path used by the pipeline
    return os.environ.get("DATA_PATH", "/data/cdr_data.csv")
