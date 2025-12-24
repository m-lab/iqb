"""Logging helpers for the IQB CLI."""

from __future__ import annotations

import logging
import os
import sys

import colorlog

LOG_COLORS = {
    "DEBUG": "bold_cyan",
    "INFO": "bold_green",
    "WARNING": "bold_yellow",
    "ERROR": "bold_red",
    "CRITICAL": "bold_red,bg_white",
}


def _use_color() -> bool:
    if os.getenv("NO_COLOR") is not None:
        return False
    if not sys.stderr.isatty():
        return False
    return True


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    datefmt = "%Y-%m-%d %H:%M:%S"
    if _use_color():
        handler = logging.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                fmt="%(log_color)s[%(asctime)s] <%(name)s> %(levelname)s:%(reset)s %(message)s",
                log_colors=LOG_COLORS,
                datefmt=datefmt,
            )
        )
        logging.basicConfig(level=level, handlers=[handler])
        return
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] <%(name)s> %(levelname)s: %(message)s",
        datefmt=datefmt,
    )
