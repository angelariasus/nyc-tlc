"""
Structured logging setup for the TLC pipeline.
Matches the same pattern used in the MEF Data Lake project.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logger(name: str, log_dir: Path | None = None) -> logging.Logger:
    """
    Create or retrieve a named logger.

    Parameters
    ----------
    name:
        Dotted logger name (e.g. ``"tlc.audit.control_manager"``).
    log_dir:
        Optional directory to write a file handler alongside the console.
        The file will be named ``{last_segment_of_name}.log``.

    Returns
    -------
    logging.Logger
    """
    logger = logging.getLogger(name)

    # Guard against duplicate handlers when the function is called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (always present)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Optional file handler
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{name.split('.')[-1]}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    return logger
