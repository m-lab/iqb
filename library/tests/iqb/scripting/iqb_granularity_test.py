"""Tests for the iqb.scripting.iqb_granularity module."""

import pytest

from iqb import IQBDatasetGranularity
from iqb.scripting import iqb_granularity


class TestParse:
    """Tests for iqb_granularity.parse."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("country", IQBDatasetGranularity.COUNTRY),
            ("country_asn", IQBDatasetGranularity.COUNTRY_ASN),
            ("subdivision1", IQBDatasetGranularity.COUNTRY_SUBDIVISION1),
            ("subdivision1_asn", IQBDatasetGranularity.COUNTRY_SUBDIVISION1_ASN),
            ("city", IQBDatasetGranularity.COUNTRY_CITY),
            ("city_asn", IQBDatasetGranularity.COUNTRY_CITY_ASN),
        ],
    )
    def test_success(self, value: str, expected: IQBDatasetGranularity) -> None:
        assert iqb_granularity.parse(value) is expected

    def test_invalid_value(self) -> None:
        with pytest.raises(ValueError, match="invalid granularity value"):
            iqb_granularity.parse("nope")
