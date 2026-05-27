"""Storage backend abstraction (SPEC §6.0.1).

All data I/O goes through a ``StorageBackend``, addressed by a logical key (a
POSIX-style relative path under the data root, built by ``storage/paths.py``).
No module outside ``storage/`` constructs ``Path`` objects against the data
directory. v1 ships ``LocalBackend``; cloud backends (``S3Backend``) are Phase 2.

The manifest is deliberately *not* routed through this interface — SQLite needs a
true local file, surfaced via ``local_path`` (SPEC §6.0.1).
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

import polars as pl

from massive_fetch.storage import parquet_io


@runtime_checkable
class StorageBackend(Protocol):
    """Abstract storage interface. v1: ``LocalBackend``. Phase 2: S3/GCS."""

    def write_parquet(self, key: str, df: pl.DataFrame) -> None: ...
    def read_parquet(self, key: str) -> pl.DataFrame: ...
    def append_parquet(self, key: str, df: pl.DataFrame, dedupe_on: list[str]) -> None: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def size_bytes(self, key: str) -> int: ...
    def local_path(self, key: str) -> Path | None: ...


class LocalBackend:
    """File-system implementation. Default for v1."""

    def __init__(
        self,
        root: Path,
        *,
        compression: str = "zstd",
        row_group_size: int = 100_000,
    ) -> None:
        self.root = Path(root).expanduser().resolve()
        self._compression = compression
        self._row_group_size = row_group_size

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_parquet(self, key: str, df: pl.DataFrame) -> None:
        parquet_io.write_parquet(
            self._path(key),
            df,
            compression=self._compression,
            row_group_size=self._row_group_size,
        )

    def read_parquet(self, key: str) -> pl.DataFrame:
        return parquet_io.read_parquet(self._path(key))

    def append_parquet(self, key: str, df: pl.DataFrame, dedupe_on: list[str]) -> None:
        """Read existing (if any), concat, dedupe, sort, write back — one cycle.

        Parquet has no true append, so this is a single read-merge-write (SPEC §8
        step 6). Dedupe keeps the *first* row of each ``dedupe_on`` group, matching
        the validation default (SPEC §9.1 ``on_duplicate_timestamp: keep_first``);
        because existing rows are concatenated ahead of ``df``, a re-fetched
        duplicate never overwrites data already on disk. Sort is ascending by
        ``timestamp`` (SPEC §6.1) when that column is present.
        """
        if self.exists(key):
            merged = pl.concat([self.read_parquet(key), df])
        else:
            merged = df
        merged = merged.unique(subset=dedupe_on, keep="first")
        if "timestamp" in merged.columns:
            merged = merged.sort("timestamp")
        self.write_parquet(key, merged)

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(
            p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file()
        )

    def size_bytes(self, key: str) -> int:
        return self._path(key).stat().st_size

    def local_path(self, key: str) -> Path | None:
        return self._path(key)


class S3Backend:
    """Phase 2 stub — not implemented in v1 (SPEC §17)."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        raise NotImplementedError("S3Backend is a Phase 2 feature. See SPEC.md §17.")
