"""Optional scripting extensions to parse granularity."""

from .. import IQBDatasetGranularity

_mapper = {
    "country": IQBDatasetGranularity.COUNTRY,
    "country_asn": IQBDatasetGranularity.COUNTRY_ASN,
    "subdivision1": IQBDatasetGranularity.COUNTRY_SUBDIVISION1,
    "subdivision1_asn": IQBDatasetGranularity.COUNTRY_SUBDIVISION1_ASN,
    "city": IQBDatasetGranularity.COUNTRY_CITY,
    "city_asn": IQBDatasetGranularity.COUNTRY_CITY_ASN,
}


def parse(v: str) -> IQBDatasetGranularity:
    """
    Parse a granularity string into a valid granularity enum.

    Accepted values:

        - country
        - country_asn
        - subdivision1
        - subdivision1_asn
        - city
        - city_asn

    Raises:
        ValueError if the value is invalid.
    """
    try:
        return _mapper[v]
    except KeyError as exc:
        valid = ", ".join(sorted(_mapper.keys()))
        raise ValueError(f"invalid granularity value {v}; valid values: {valid}") from exc
