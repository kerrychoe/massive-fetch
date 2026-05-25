"""Logging configuration (SPEC §11).

- Console: human-readable, INFO by default (DEBUG with ``--verbose``).
- File: JSON lines, DEBUG, rotated at 10 MB x 5 — attached only when the
  ``logs`` directory exists, so ``status`` works before ``init`` runs.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

import structlog

from massive_fetch.config import LoggingConfig


def setup_logging(
    config: LoggingConfig,
    *,
    logs_dir: Path | None = None,
    verbose: bool = False,
) -> structlog.stdlib.BoundLogger:
    """Configure ``structlog`` + stdlib logging and return a bound logger."""
    console_level = logging.DEBUG if verbose else logging.getLevelName(config.console_level)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    structlog.configure(
        processors=shared_processors + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            foreign_pre_chain=shared_processors,
        )
    )
    root.addHandler(console_handler)

    if logs_dir is not None and logs_dir.exists():
        file_handler = logging.handlers.RotatingFileHandler(
            logs_dir / "massive-fetch.log",
            maxBytes=config.file_max_bytes,
            backupCount=config.file_backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.getLevelName(config.file_level))
        file_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(
                processors=[
                    structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                    structlog.processors.JSONRenderer(),
                ],
                foreign_pre_chain=shared_processors,
            )
        )
        root.addHandler(file_handler)

    return structlog.get_logger()
