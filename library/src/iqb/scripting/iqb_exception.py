"""Optional scripting extensions to route exceptions to errors."""

from __future__ import annotations

from .iqb_logging import log


class Interceptor:
    """
    Utility class to intercept exceptions.

    Use as a context manager:

        interceptor = iqb_exception.Interceptor()
        with interceptor:
            func()
        print(interceptor.failed)

    Exceptions are logged and otherwise ignored. The failed
    field tells you whether there were any exeptions.
    """

    def __init__(self):
        self.failed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            log.error("operation failed: %s", exc_value)
            _ = traceback
            self.failed = True
