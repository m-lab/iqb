"""Tests for the iqb.scripting.iqb_exception module."""

from unittest.mock import patch

from iqb.scripting import iqb_exception


class TestInterceptor:
    """Tests for Interceptor."""

    def test_no_exception(self) -> None:
        interceptor = iqb_exception.Interceptor()

        with patch("iqb.scripting.iqb_exception.log") as log, interceptor:
            pass

        assert interceptor.failed is False
        assert interceptor.exitcode() == 0
        log.error.assert_not_called()

    def test_exception_sets_failed_and_logs(self) -> None:
        interceptor = iqb_exception.Interceptor()

        with patch("iqb.scripting.iqb_exception.log") as log, interceptor:
            raise ValueError("boom")

        assert interceptor.failed is True
        assert interceptor.exitcode() == 1
        log.error.assert_called_once()
        assert log.error.call_args[0][0] == "operation failed: %s"
        assert str(log.error.call_args[0][1]) == "boom"
