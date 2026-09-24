"""Structured logging for the ingestion framework.

Provides a logger adapter that injects ``source_id``, ``run_id``, and
``batch_id`` context into every log message.  Additional keyword arguments
passed to log methods (e.g., ``logger.info("msg", records=100)``) are
appended to the message as structured key=value pairs.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, MutableMapping, Optional


class IngestionLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds ingestion context to every message.

    Usage::

        logger = create_ingestion_logger("faostat_trade", "run_123", "batch_1")
        logger.info("Chunk processed", chunk_id=5, records=50000)
        # Output: [source_id=faostat_trade] [run_id=run_123] [batch_id=batch_1] Chunk processed | chunk_id=5 records=50000
    """

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        # Build context prefix from adapter extra
        ctx = self.extra or {}
        prefix = (
            f"[source_id={ctx.get('source_id', '?')}] "
            f"[run_id={ctx.get('run_id', '?')}] "
            f"[batch_id={ctx.get('batch_id', '?')}]"
        )

        # Extract non-standard kwargs (structured fields) so they don't
        # propagate into Logger._log() which only accepts exc_info,
        # stack_info, stacklevel, extra.
        _STDLIB_KWARGS = {"exc_info", "stack_info", "stacklevel", "extra"}
        structured: dict[str, Any] = {}
        to_remove: list[str] = []
        for key in kwargs:
            if key not in _STDLIB_KWARGS:
                structured[key] = kwargs[key]
                to_remove.append(key)
        for key in to_remove:
            del kwargs[key]

        # Append structured fields to the message
        if structured:
            fields = " ".join(f"{k}={v}" for k, v in structured.items())
            full_msg = f"{prefix} {msg} | {fields}"
        else:
            full_msg = f"{prefix} {msg}"

        return full_msg, kwargs


def get_logger(name: str) -> logging.Logger:
    """Return (or create) a logger with a stdout handler."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def create_ingestion_logger(
    source_id: str,
    run_id: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> IngestionLoggerAdapter:
    """Create a structured logger with ingestion context."""
    logger = get_logger(f"ingestion.{source_id}")
    extra = {
        "source_id": source_id,
        "run_id": run_id or "?",
        "batch_id": batch_id or "?",
    }
    return IngestionLoggerAdapter(logger, extra)
