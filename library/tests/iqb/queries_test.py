"""Tests for the iqb.queries SQL query templates module."""

from importlib.resources import files

import iqb.queries


class TestQueriesPackage:
    """Tests for iqb.queries package structure."""

    def test_queries_package_can_be_imported(self):
        """Test that iqb.queries package can be imported."""
        assert iqb.queries is not None

    def test_queries_package_has_files(self):
        """Test that queries package provides access to files."""
        query_files = files(iqb.queries)
        assert query_files is not None


class TestDownloadsByCountryQuery:
    """Tests for downloads_by_country.sql query template."""

    def test_downloads_by_country_exists(self):
        """Test that downloads_by_country.sql query file exists."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        assert query_file.is_file()

    def test_downloads_by_country_can_be_read(self):
        """Test that downloads_by_country.sql can be read."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert content is not None
        assert len(content) > 0

    def test_downloads_by_country_has_date_placeholders(self):
        """Test that downloads_by_country.sql contains date placeholders."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "{START_DATE}" in content
        assert "{END_DATE}" in content

    def test_downloads_by_country_queries_unified_downloads_table(self):
        """Test that downloads_by_country.sql queries the correct table."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "measurement-lab.ndt.unified_downloads" in content

    def test_downloads_by_country_groups_by_country_code(self):
        """Test that downloads_by_country.sql groups by country code."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "GROUP BY country_code" in content

    def test_downloads_by_country_calculates_percentiles(self):
        """Test that downloads_by_country.sql calculates percentiles."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        # Should calculate p1, p5, p10, p25, p50, p75, p90, p95, p99
        assert "APPROX_QUANTILES" in content
        assert "download_p95" in content or "download_p99" in content


class TestUploadsByCountryQuery:
    """Tests for uploads_by_country.sql query template."""

    def test_uploads_by_country_exists(self):
        """Test that uploads_by_country.sql query file exists."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        assert query_file.is_file()

    def test_uploads_by_country_can_be_read(self):
        """Test that uploads_by_country.sql can be read."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert content is not None
        assert len(content) > 0

    def test_uploads_by_country_has_date_placeholders(self):
        """Test that uploads_by_country.sql contains date placeholders."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "{START_DATE}" in content
        assert "{END_DATE}" in content

    def test_uploads_by_country_queries_unified_uploads_table(self):
        """Test that uploads_by_country.sql queries the correct table."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "measurement-lab.ndt.unified_uploads" in content

    def test_uploads_by_country_groups_by_country_code(self):
        """Test that uploads_by_country.sql groups by country code."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        assert "GROUP BY country_code" in content

    def test_uploads_by_country_calculates_percentiles(self):
        """Test that uploads_by_country.sql calculates percentiles."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        content = query_file.read_text(encoding="utf-8")
        # Should calculate p1, p5, p10, p25, p50, p75, p90, p95, p99
        assert "APPROX_QUANTILES" in content
        assert "upload_p95" in content or "upload_p99" in content


class TestQueryTemplateSubstitution:
    """Tests for query template placeholder substitution."""

    def test_downloads_query_date_substitution(self):
        """Test that date placeholders can be substituted in downloads query."""
        query_file = files(iqb.queries).joinpath("downloads_by_country.sql")
        template = query_file.read_text(encoding="utf-8")

        # Substitute placeholders
        query = template.replace("{START_DATE}", "2024-10-01")
        query = query.replace("{END_DATE}", "2024-11-01")

        # Verify substitution worked
        assert "{START_DATE}" not in query
        assert "{END_DATE}" not in query
        assert "2024-10-01" in query
        assert "2024-11-01" in query

    def test_uploads_query_date_substitution(self):
        """Test that date placeholders can be substituted in uploads query."""
        query_file = files(iqb.queries).joinpath("uploads_by_country.sql")
        template = query_file.read_text(encoding="utf-8")

        # Substitute placeholders
        query = template.replace("{START_DATE}", "2024-10-01")
        query = query.replace("{END_DATE}", "2024-11-01")

        # Verify substitution worked
        assert "{START_DATE}" not in query
        assert "{END_DATE}" not in query
        assert "2024-10-01" in query
        assert "2024-11-01" in query
