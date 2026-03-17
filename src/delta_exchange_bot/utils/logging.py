import json
import logging
import sys
from datetime import datetime


def _configure_std_logging(level: str = "INFO") -> None:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [handler]


def _serialize_loguru(record) -> str:
    payload = {
        "ts": datetime.utcnow().isoformat(),
        "level": record["level"].name,
        "name": record["name"],
        "message": record["message"],
        "extra": record["extra"],
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def configure_logging(level: str = "INFO", structured: bool = True) -> None:
    if not structured:
        _configure_std_logging(level=level)
        return

    try:
        from loguru import logger as loguru_logger
    except Exception:
        _configure_std_logging(level=level)
        return

    loguru_logger.remove()
    loguru_logger.add(
        sys.stdout,
        level=level.upper(),
        serialize=True,
        backtrace=False,
        diagnose=False,
    )
    _configure_std_logging(level=level)
