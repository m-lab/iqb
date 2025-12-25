"""Optional scripting helpers to log exceptions and convert them to exit codes."""

from __future__ import annotations

from .iqb_logging import log


class Interceptor:
    """
    Context manager to intercept exceptions.

    Use as a context manager:

        interceptor = iqb_exception.Interceptor()
        with interceptor:
            func()
        print(interceptor.failed)
        sys.exit(interceptor.exitcode())

    Exceptions are logged and suppressed. The failed field
    tells you whether there were any exceptions.

    Use interceptor.exitcode() to get the suitable exit code
    to pass to the sys.exit() function.
    """

    def __init__(self):
        self.failed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return False
        if issubclass(exc_type, KeyboardInterrupt):
            return False
        if exc_type:
            log.error("operation failed: %s", exc_value)
            _ = traceback
            self.failed = True
        return True  # suppress the exception

    def exitcode(self) -> int:
        """
        Return the exitcode to pass to sys.exit.

        Zero on success, 1 on failure.
        """
        return int(self.failed)
