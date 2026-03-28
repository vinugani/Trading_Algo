import json
import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _console_handler(level: str) -> logging.StreamHandler:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(_LOG_FMT, datefmt=_DATE_FMT))
    h.setLevel(level.upper())
    return h


def _file_handler(path: Path, level: str) -> logging.handlers.RotatingFileHandler:
    """Rotating file handler — 10 MB per file, keeps 5 backups."""
    path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(
        path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    h.setFormatter(logging.Formatter(_LOG_FMT, datefmt=_DATE_FMT))
    h.setLevel(level.upper())
    return h


def _error_file_handler(path: Path) -> logging.handlers.RotatingFileHandler:
    """Separate WARNING+ rotating file for fast issue scanning."""
    path.parent.mkdir(parents=True, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(
        path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    h.setFormatter(logging.Formatter(_LOG_FMT, datefmt=_DATE_FMT))
    h.setLevel(logging.WARNING)
    return h


def _configure_std_logging(
    level: str = "INFO",
    log_dir: Optional[Path] = None,
    session_tag: Optional[str] = None,
) -> Optional[Path]:
    """Configure the stdlib root logger.

    Returns the path of the session log file if one was created, else None.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers = [_console_handler(level)]

    session_log_path: Optional[Path] = None
    if log_dir is not None:
        tag = session_tag or datetime.now().strftime("%Y%m%d_%H%M%S")

        # Per-session log file — all levels
        session_log_path = log_dir / f"bot_{tag}.log"
        root.addHandler(_file_handler(session_log_path, level))

        # Rolling errors-only file — WARNING and above
        error_log_path = log_dir / "errors.log"
        root.addHandler(_error_file_handler(error_log_path))

        root.info(
            "File logging enabled session_log=%s errors_log=%s",
            session_log_path,
            error_log_path,
        )

    return session_log_path


def _serialize_loguru(record) -> str:
    payload = {
        "ts": datetime.utcnow().isoformat(),
        "level": record["level"].name,
        "name": record["name"],
        "message": record["message"],
        "extra": record["extra"],
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def configure_logging(
    level: str = "INFO",
    structured: bool = True,
    log_dir: Optional[str | Path] = None,
    session_tag: Optional[str] = None,
) -> Optional[Path]:
    """Configure logging for the bot.

    Args:
        level:       Minimum log level (DEBUG / INFO / WARNING / ERROR).
        structured:  If True and loguru is installed, use structured JSON on stdout.
        log_dir:     Directory to write log files into.  If None, file logging
                     is skipped (console-only).  The directory is created
                     automatically if it does not exist.
        session_tag: Optional suffix for the session log filename.
                     Defaults to ``YYYYMMDD_HHMMSS`` of the current time.

    Returns:
        Path to the session log file, or None if file logging is disabled.
    """
    log_dir_path = Path(log_dir) if log_dir is not None else None

    if not structured:
        return _configure_std_logging(level=level, log_dir=log_dir_path, session_tag=session_tag)

    try:
        from loguru import logger as loguru_logger  # noqa: PLC0415
    except Exception:
        return _configure_std_logging(level=level, log_dir=log_dir_path, session_tag=session_tag)

    # loguru on stdout (structured JSON)
    loguru_logger.remove()
    loguru_logger.add(
        sys.stdout,
        level=level.upper(),
        serialize=True,
        backtrace=False,
        diagnose=False,
    )

    # stdlib handles file writing (loguru file sinks would duplicate)
    return _configure_std_logging(level=level, log_dir=log_dir_path, session_tag=session_tag)
