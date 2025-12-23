"""Tests for the iqb.pipeline.dataset module."""

from iqb.pipeline.dataset import (
    IQBDatasetGranularity,
    IQBDatasetMLabTable,
    iqb_dataset_name_for_mlab,
)


class TestIQBDatasetNameForMLab:
    """Test for iqb_dataset_name_for_mlab function."""

    def test_downloads_by_country(self):
        value = iqb_dataset_name_for_mlab(
            granularity=IQBDatasetGranularity.COUNTRY,
            table=IQBDatasetMLabTable.DOWNLOAD,
        )
        assert value == "downloads_by_country"

    def test_uploads_by_country_city_asn(self):
        value = iqb_dataset_name_for_mlab(
            granularity=IQBDatasetGranularity.COUNTRY_CITY_ASN,
            table=IQBDatasetMLabTable.UPLOAD,
        )
        assert value == "uploads_by_country_city_asn"
