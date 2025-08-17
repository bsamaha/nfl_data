# pyright: reportMissingImports=false, reportMissingModuleSource=false
from __future__ import annotations

import logging
import os
from pathlib import Path
import orjson
import structlog


def configure_logging(run_id: str | None = None) -> None:
    logs_dir = Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(level=log_level)

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        cache_logger_on_first_use=True,
    )


def log_run_event(run_id: str, event: str, **fields) -> None:
    path = Path("logs") / f"{run_id}.jsonl"
    rec = {"event": event, **fields}
    with path.open("ab") as f:
        f.write(orjson.dumps(rec) + b"\n")

