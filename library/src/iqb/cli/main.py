"""The main command."""

import sys

from . import sync, version

brief_help_message = "Try `iqb help` for more help.\n"

long_help_message = r"""
Usage

    iqb SUBCOMMAND [flags]

Description

    Internet Quality Barometer (IQB) command line tool.

    When invoked without a SUBCOMMAND, we print this help screen.

Commands

    help

        Print this help screen and exit.

    sync
        Synchronize the local cache with remote data sources providing
        data for IQB, including remote caches and BigQuery.

    version

        Print the tool version number and exit.

Examples

    Use `--help` to get more information about the `sync` command:

        iqb sync --help

    Same as above but using the `-h` short flag instead:

        iqb sync -h

    Every SUBCOMMAND accepts and correctly handles `-h` and `--help`.

Exit Code

    Zero on success, `1` on failure, `2` on command line usage error.

"""


def run(args: list[str]) -> int:
    # print help when invoked without arguments
    if len(args) <= 0:
        sys.stdout.write(long_help_message)
        return 0

    # consume the command name
    subcmd, args = args[0], args[1:]

    # handle `help`
    if subcmd in ("-h", "--help", "help"):
        sys.stdout.write(long_help_message)
        return 0

    # handle `sync`
    if subcmd == "sync":
        return sync.run(args)

    # handle `version`
    if subcmd == "version":
        return version.run(args)

    # usage error
    sys.stderr.write(f"error: unknown subcommand: {subcmd}\n")
    sys.stderr.write(brief_help_message)
    return 2


def main() -> None:
    sys.exit(run(sys.argv[1:]))
