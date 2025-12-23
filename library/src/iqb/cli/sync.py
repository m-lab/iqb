"""The sync subcommand."""

import getopt
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime

from iqb import (
    IQBDatasetGranularity,
    IQBDatasetMLabTable,
    IQBGitHubRemoteCache,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)
from iqb.pipeline import parse_date

brief_help_message = "hint: try `iqb sync --help` for more help.\n"

long_help_message = r"""
Usage

    iqb sync --granularity GRANULARITY
             --start-date YYYY-MM-DD
             --end-date YYYY-MM-DD
             [options]

Synchronize IQB parquet files from a dataset (default: `mlab`) with the given
time range and using the given GRANULARITY. We first use the GitHub cache. If
that fails, we fallback to generating data using BigQuery queries.

Flags

    --bigquery-project NAME

        Set billing project [default: `measurement-lab`].

    -d DIR
    --data-dir DIR

        Set the data directory containing the cache [default: `.iqb`].

    --dataset DATASET

        Set the dataset [default: `mlab`].

    --end-date YYYY-MM-DD

        Set end date (exclusive).

    --granularity GRANULARITY

        Set the dataset geographical granularity.

        Must be one of: `country`, `country_asn`, `subdivision1`,
        `subdivision1_asn`, `city`, `city_asn`.

    -h
    --help

        Show this help message and exit.

    --start-date YYYY-MM-DD

        Set start date (inclusive).

    -v
    --verbose

        Run in verbose mode

Examples

    Download June-2025 country-asn data for `mlab`

        iqb sync --dataset mlab \
                 --start-date 2025-06-01 \
                 --end-date 2025-07-01 \
                 --granularity country_asn

    Same as above but fetch `city_asn` instead

        iqb sync --dataset mlab \
                 --start-date 2025-06-01 \
                 --end-date 2025-07-01 \
                 --granularity city_asn

Directories

    DATADIR/state/ghremote/
        Directory containing the GitHub cache manifest.

    DATADIR/cache/v1/
        Directory containing the local cache.

    By default DATADIR is `.iqb`.

Exit Code

    Zero on success, `1` on failure, `2` on command line usage error.

"""


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "[%(asctime)s] <%(name)s> %(levelname)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, datefmt="%Y-%m-%d %H:%M:%S")


@dataclass
class Command:
    """Model the sync command."""

    _errors: list[str] = field(default_factory=list)
    bigquery_project: str = "measurement-lab"
    data_dir: str = ".iqb"
    dataset: str = "mlab"
    end_date: str = ""
    end_time: datetime | None = None
    granularity: str = ""
    start_date: str = ""
    start_time: datetime | None = None
    verbose: bool = False

    def run(self) -> int:
        # process the given command line flags
        self._parse_dataset_flag()
        self._parse_end_date_flag()
        granularity = self._parse_granularity_flag()
        self._parse_start_date_flag()
        self._validate_both_dates()
        if self._errors:
            for error in self._errors:
                sys.stderr.write(f"error: {error}\n")
            sys.stderr.write(brief_help_message)
            return 2

        # init logger
        configure_logging(self.verbose)

        # init github cache
        ghcache = IQBGitHubRemoteCache(data_dir=self.data_dir)

        # init pipeline
        pipeline = IQBPipeline(
            data_dir=self.data_dir,
            project=self.bigquery_project,
            remote_cache=ghcache,
        )

        # default to running mlab until we have more dataset
        # TODO(bassosimone): support more datasets
        try:
            self._run_mlab(granularity, pipeline)
        except Exception as exc:
            sys.stderr.write(f"error: {exc}\n")
            return 1
        else:
            return 0

    def _run_mlab(
        self,
        granularity: IQBDatasetGranularity,
        pipeline: IQBPipeline,
    ):
        # construct the dataset names
        dataset_names = [
            iqb_dataset_name_for_mlab(granularity=granularity, table=table)
            for table in (IQBDatasetMLabTable.DOWNLOAD, IQBDatasetMLabTable.UPLOAD)
        ]

        # sync each dataset
        for dataset_name in dataset_names:
            entry = pipeline.get_cache_entry(
                dataset_name=dataset_name,
                start_date=self.start_date,
                end_date=self.end_date,
            )
            with entry.lock():
                if not entry.exists():
                    entry.sync()

    def _validate_both_dates(self) -> None:
        # Use the pipeline date parser, but keep range checking here for clear CLI errors.
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            self._errors.append("start-date >= end-date")
            self._errors.append(f"--start-date value: {self.start_date}")
            self._errors.append(f"--end-date value: {self.end_date}")

    def _parse_dataset_flag(self) -> None:
        if self.dataset != "mlab":
            self._errors.append(f"invalid --dataset value: {self.dataset}")
            return

    def _parse_end_date_flag(self) -> None:
        self.end_time = self._parse_date_flag(name="end-date", value=self.end_date)

    def _parse_granularity_flag(self) -> IQBDatasetGranularity:
        # TODO(bassosimone): we should probably change the names for the enum
        # because they make more sense (but we cannot change the values).
        valids = {
            "country": IQBDatasetGranularity.COUNTRY,
            "country_asn": IQBDatasetGranularity.COUNTRY_ASN,
            "subdivision1": IQBDatasetGranularity.COUNTRY_SUBDIVISION1,
            "subdivision1_asn": IQBDatasetGranularity.COUNTRY_SUBDIVISION1_ASN,
            "city": IQBDatasetGranularity.COUNTRY_CITY,
            "city_asn": IQBDatasetGranularity.COUNTRY_CITY_ASN,
        }
        if not self.granularity:
            self._errors.append("missing `--granularity` flag")
            return IQBDatasetGranularity.COUNTRY
        if self.granularity not in valids:
            self._errors.append(f"invalid --granularity value: {self.granularity}")
            return IQBDatasetGranularity.COUNTRY
        return valids[self.granularity]

    def _parse_start_date_flag(self) -> None:
        self.start_time = self._parse_date_flag(
            name="start-date", value=self.start_date
        )

    def _parse_date_flag(self, *, name: str, value: str | None) -> datetime | None:
        if not value:
            self._errors.append(f"missing `--{name}` flag")
            return None
        try:
            return parse_date(value)
        except ValueError:
            self._errors.append(f"invalid --{name} value: {value}")
            return None


def run(args: list[str]) -> int:
    # print help if invoked without args
    if len(args) <= 0:
        print(long_help_message)
        return 0
    if any(arg in ("--help", "-h") for arg in args):
        sys.stdout.write(long_help_message)
        return 0

    # parse command line options
    try:
        options, arguments = getopt.getopt(
            args,
            "d:v",
            [
                "bigquery-project=",
                "data-dir=",
                "dataset=",
                "end-date=",
                "granularity=",
                "start-date=",
                "verbose",
            ],
        )
    except getopt.GetoptError as exc:
        sys.stderr.write(f"error: {exc}\n")
        sys.stderr.write(brief_help_message)
        return 2
    if len(arguments) != 0:
        sys.stderr.write("error: we expect exactly zero positional arguments\n")
        sys.stderr.write(brief_help_message)
        return 2

    # fill the command state with options
    cmd = Command()
    for name, value in options:
        if name == "--bigquery-project":
            cmd.bigquery_project = value
            continue

        if name in ("-d", "--data-dir"):
            cmd.data_dir = value
            continue

        if name == "--dataset":
            cmd.dataset = value
            continue

        if name == "--end-date":
            cmd.end_date = value
            continue

        if name == "--granularity":
            cmd.granularity = value
            continue

        if name == "--start-date":
            cmd.start_date = value
            continue

        if name in ("-v", "--verbose"):
            cmd.verbose = True
            continue

        raise RuntimeError(f"Unexpected option: {name} => {value}")

    # Run command
    return cmd.run()
