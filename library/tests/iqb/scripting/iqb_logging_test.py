"""Tests for the iqb.scripting.iqb_logging module."""

import logging

import pytest
from rich.text import Text

from iqb.scripting import iqb_logging


@pytest.mark.parametrize(
    ("verbose", "expected_level"),
    [
        (True, logging.DEBUG),
        (False, logging.INFO),
    ],
)
def test_configure_sets_level_and_handler(verbose: bool, expected_level: int) -> None:
    iqb_logging.configure(verbose=verbose)

    root = logging.getLogger()
    assert root.level == expected_level
    assert any(isinstance(handler, iqb_logging.LocalTZRichHandler) for handler in root.handlers)


def test_render_uses_local_tz(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = iqb_logging.LocalTZRichHandler()
    captured = {}

    def fake_log_render(*_args, **kwargs):
        _ = _args
        captured.update(kwargs)
        return None

    monkeypatch.setattr(handler, "_log_render", fake_log_render)

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )

    handler.render(record=record, traceback=None, message_renderable=Text("hello"))

    log_time = captured.get("log_time")
    assert log_time is not None
    assert log_time.tzinfo is not None


def test_log_is_named_scripting() -> None:
    assert iqb_logging.log.name == "scripting"
