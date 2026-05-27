"""Low-level Parquet read/write helpers (SPEC §2, §6.1).

The only module that touches Polars' Parquet engine directly. Writes are atomic
(temp file in the same directory, then ``os.replace``) so a crash mid-write never
leaves a partial Parquet behind — a property the read-merge-write append in
``backend.py`` relies on.
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def read_parquet(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def write_parquet(
    path: Path,
    df: pl.DataFrame,
    *,
    compression: str = "zstd",
    row_group_size: int = 100_000,
) -> None:
    """Write ``df`` atomically to ``path`` (parents created as needed)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        df.write_parquet(tmp, compression=compression, row_group_size=row_group_size)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
