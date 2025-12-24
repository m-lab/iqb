"""Optional scripting extensions to use IQBPipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .. import (
    IQBDatasetMLabTable,
    IQBGitHubRemoteCache,
    IQBPipeline,
    iqb_dataset_name_for_mlab,
)
from . import iqb_granularity
from .iqb_logging import log


@dataclass(frozen=True, kw_only=True)
class Pipeline:
    """Wrapper for IQBPipeline providing convenience methods for scripting."""

    pipeline: IQBPipeline

    def sync_mlab(
        self,
        granularity: str,
        *,
        end_date: str,
        start_date: str,
    ) -> None:
        """
        Helper function to synchronize mlab data to the local cache.

        The synchronization will typically attempt to use a remote cache and
        fall back to querying using BigQuery otherwise.

        Arguments:
            end_date: exclusive end date as a YYYY-MM-DD string.
            granularity: geographical granularity to use.
            start_date: incluive start date as a YYYY-MM-DD string.

        Raises:
            Exceptions in case of failure.
        """
        granularity = iqb_granularity.parse(granularity)

        log.info(
            "sync mlab with start_date=%s end_date=%s granularity=%s... start",
            start_date,
            end_date,
            granularity,
        )

        for table in (IQBDatasetMLabTable.DOWNLOAD, IQBDatasetMLabTable.UPLOAD):
            entry = self.pipeline.get_cache_entry(
                dataset_name=iqb_dataset_name_for_mlab(
                    granularity=granularity,
                    table=table,
                ),
                start_date=start_date,
                end_date=end_date,
            )
            with entry.lock():
                if not entry.exists():
                    entry.sync()

        log.info(
            "sync mlab with start_date=%s end_date=%s granularity=%s... ok",
            start_date,
            end_date,
            granularity,
        )


def create(
    data_dir: str | Path | None = None,
    *,
    project: str = "measurement-lab",
) -> Pipeline:
    """
    Helper function to create a Pipeline instance.

    The created Pipeline will use the best remote cache available.

    Arguments:
       data_dir: the data directory to use or None, in which case we use `.iqb`.
       project: the BigQuery project to use for billing.

    Returns:
       A fully configured Pipeline ready to use.
    """
    return Pipeline(
        pipeline=IQBPipeline(
            project=project,
            data_dir=data_dir,
            remote_cache=IQBGitHubRemoteCache(data_dir=data_dir),
        )
    )
