from __future__ import annotations

from pathlib import Path
from typing import Tuple


def make_output_paths(output_parent: str, run_id: str, output_root: str = "/output") -> Tuple[Path, Path]:
    """Create and return (output_dir, manifest_path)."""
    parent = Path(output_parent)
    if not parent.is_absolute():
        parent = Path(output_root) / output_parent

    out = parent / run_id
    out.mkdir(parents=True, exist_ok=True)
    manifest = out / "_MANIFEST.json"
    return out, manifest


def resolve_input_path(env_path: str | None) -> str:
    if env_path:
        return env_path
    return "/data/cdr_data.csv"
