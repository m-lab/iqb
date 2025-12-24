"""Module for scripting the pipeline."""

from __future__ import annotations

import getopt
import sys
from dataclasses import dataclass, field
from datetime import datetime

from .. import (
    IQBDatasetGranularity,
    IQBDatasetMLabTable,
    IQBGitHubRemoteCache,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)
from ..pipeline import parse_date

brief_help_message = "hint: try `iqb sync --help` for more help.\n"


class IQBEasyError(Exception):
    """Error emitted by this package."""

    def __init__(self) -> None:
        self.errors = []

    def append(self, err: str) -> None:
        self.errors.append(err)

    def contains_errors(self) -> bool:
        return len(self.errors) > 0


def iqb_easy_date(*, key: str, value: str, err: IQBEasyError) -> datetime | None:
    if not value:
        err.append(f"{key} is empty")
        return None
    try:
        return parse_date(value)
    except ValueError as exc:
        err.append(f"{key} is not a valid YYYY-MM-DD date: {exc}")
        return None


# TODO(bassosimone): we should probably change the names for the enum
# because they make more sense (but we cannot change the values).
_granularity_map = {
    "country": IQBDatasetGranularity.COUNTRY,
    "country_asn": IQBDatasetGranularity.COUNTRY_ASN,
    "subdivision1": IQBDatasetGranularity.COUNTRY_SUBDIVISION1,
    "subdivision1_asn": IQBDatasetGranularity.COUNTRY_SUBDIVISION1_ASN,
    "city": IQBDatasetGranularity.COUNTRY_CITY,
    "city_asn": IQBDatasetGranularity.COUNTRY_CITY_ASN,
}


def iqb_easy_granularity(value: str, err: IQBEasyError) -> IQBDatasetGranularity | None:
    try:
        return _granularity_map[value]
    except KeyError:
        err.append(f"invalid granularity value: {value}")
        return


@dataclass(frozen=True, kw_only=True)
class PipelineJob:
    end_date: str
    granularity: IQBDatasetGranularity
    start_date: str


@dataclass(frozen=True, kw_only=True)
class IQBEasyPipelineJob:
    """Job for IQBEasyPipeline"""

    end_date: str = ""
    granularity: str = ""
    start_date: str = ""

    def build(self, err: IQBEasyError) -> PipelineJob | None:
        """Build Pipelinejob or raise IQBEasyError"""
        end_date = iqb_easy_date(key="end_date", value=self.end_date, err=err)
        granularity = iqb_easy_granularity(self.granularity, err)
        start_date = iqb_easy_date(key="start_date", value=self.start_date, err=err)
        return (
            PipelineJob(
                start_date=self.start_date,
                end_date=self.end_date,
                granularity=granularity,
            )
            if start_date is not None
            and granularity is not None
            and end_date is not None
            else None
        )


_mlab_table_map = {
    "download": IQBDatasetMLabTable.DOWNLOAD,
    "upload": IQBDatasetMLabTable.UPLOAD,
}


def iqb_easy_mlab_table(value: str, err: IQBEasyError) -> IQBDatasetMLabTable | None:
    try:
        return _mlab_table_map[value]
    except KeyError:
        err.append(f"invalid granularity value: {value}")
        return


@dataclass(frozen=True, kw_only=True)
class IQBEasyPipeline:
    """Simplified pipeline for scripting the CLI."""

    pipeline: IQBPipeline

    def sync_mlab(
        self,
        job: IQBEasyPipelineJob,
        table: str,
        err: IQBEasyError,
    ):
        rjob = job.build(err)
        if rjob is None:
            return

        rtable = iqb_easy_mlab_table(table, err)
        if rtable is None:
            return

        dataset_name = iqb_dataset_name_for_mlab(
            granularity=rjob.granularity,
            table=rtable,
        )

        try:
            entry = self.pipeline.get_cache_entry(
                dataset_name=dataset_name,
                start_date=job.start_date,
                end_date=job.end_date,
            )
        except Exception as exc:
            err.append(f"cannot get cache entry: {exc}")
            return

        try:
            with entry.lock():
                if not entry.exists():
                    entry.sync()
        except Exception as exc:
            err.append(f"cannot sync cache entry: {exc}")
            return


@dataclass(frozen=True, kw_only=True)
class IQBEasyPipelineConfig:
    """Configuration for creating an IQBEasyPipeline."""

    bigquery_project: str
    data_dir: str = ".iqb"

    def build(self) -> IQBPipeline:
        ghcache = IQBGitHubRemoteCache(data_dir=self.data_dir)
        return IQBPipeline(
            data_dir=self.data_dir,
            project=self.bigquery_project,
            remote_cache=ghcache,
        )
