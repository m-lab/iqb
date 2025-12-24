# Test Fixtures

The [real-data](real-data) directory contains real data with
country granularity. We cannot include real data using larger
granularities because they would be too large.

For this reason, we have [fake-data](fake-data) that contains
fake data with `city_asn` granularity and are small.

Pytest fixtures:
- `real_data_dir` points at `library/tests/fixtures/real-data`.
- `fake_data_dir` points at `library/tests/fixtures/fake-data`.
