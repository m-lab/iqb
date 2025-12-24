"""Optional scripting extensions to configure logging."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from rich.logging import RichHandler


class LocalTZRichHandler(RichHandler):
    """Extend the RichHandler to provide timezone aware timestamps."""

    def render(self, *, record, traceback, message_renderable):
        path = Path(record.pathname).name
        level = self.get_level_text(record)
        time_format = None if self.formatter is None else self.formatter.datefmt
        log_time = datetime.fromtimestamp(record.created).astimezone()
        return self._log_render(
            self.console,
            [message_renderable] if not traceback else [message_renderable, traceback],
            log_time=log_time,
            time_format=time_format,
            level=level,
            path=path,
            line_no=record.lineno,
            link_path=record.pathname if self.enable_link_path else None,
        )


def configure(verbose: bool) -> None:
    """Configure the logging subsystem to use LocalTZRichHandler."""
    level = logging.DEBUG if verbose else logging.INFO
    handler = LocalTZRichHandler(
        show_time=True,
        show_level=True,
        show_path=False,
        log_time_format="[%Y-%m-%d %H:%M:%S %z]",
    )
    logging.basicConfig(
        level=level,
        format="<%(name)s> %(message)s",
        handlers=[handler],
        force=True,
    )


log = logging.getLogger("scripting")
"""Logger that the scripting package should use."""
