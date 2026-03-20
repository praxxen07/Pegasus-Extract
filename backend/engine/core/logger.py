"""
core/logger.py — Structured logging to file + console.

Provides a single `setup_logger` function that creates a logger writing to
both a timestamped log file and the terminal, with consistent formatting.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logger(
    name: str = "extraction-engine",
    log_dir: Path | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Create and return a logger with file + console handlers.

    Args:
        name: Logger name (used as prefix in log file).
        log_dir: Directory for log files. If None, logs to console only.
        level: Logging level.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler
    if log_dir is not None:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{name}_{stamp}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info("Log file: %s", log_file)

    return logger
