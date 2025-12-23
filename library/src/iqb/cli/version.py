"""The version subcommand."""

import sys

from .. import __version__

brief_help_message = "Try `iqb version --help` for more help.\n"

long_help_message = r"""
Usage

    iqb version [flags]

Description

    Print the iqb tool version and exit.

Flags

    -h
    --help

        Show this help message and exit.

"""


def run(args: list[str]) -> int:
    # print version when invoked without arguments
    if len(args) <= 0:
        sys.stdout.write(f"{__version__}\n")
        return 0

    # handle request for help
    if any(arg in ("-h", "--help") for arg in args):
        sys.stdout.write(long_help_message)
        return 0

    # error
    sys.stderr.write(f"error: unhandled CLI flags: {args}\n")
    sys.stderr.write(brief_help_message)
    return 2
