"""Module for fetching IQB measurement data from cache.

The IQBCache component manages local caching of IQB measurement data, following
a Git-like convention for storing local state.

Cache Directory Convention
---------------------------
By default, IQBCache looks for a `.iqb/` directory in the current working
directory, similar to how Git uses `.git/` for repository state. This provides:

- Clear ownership (`.iqb/` contains IQB-specific data)
- Per-project isolation (each project has its own cache directory)
- Conventional pattern (like `.cache/`, `.config/`, etc.)

Example usage:

    # Uses ./.iqb/ in current directory
    cache = IQBCache()

    # Or specify custom location
    cache = IQBCache(cache_dir="/shared/iqb-cache")
"""

import json
from datetime import datetime
from pathlib import Path


class IQBCache:
    """Component for fetching IQB measurement data from cache."""

    def __init__(self, cache_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            cache_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        if cache_dir is None:
            self.cache_dir = Path.cwd() / ".iqb"
        else:
            self.cache_dir = Path(cache_dir)

    def get_data(
        self,
        country: str,
        start_date: datetime,
        end_date: datetime | None = None,
        percentile: int = 95,
    ) -> dict:
        """
        Fetch measurement data for IQB calculation.

        Args:
            country: ISO 2-letter country code ("US", "DE", "BR").
            start_date: Start of date range (inclusive).
            end_date: End of date range (exclusive). If None, defaults to start_date + 1 month.
            percentile: Which percentile to extract (1-99).

        Returns:
            dict with keys for IQBCalculator:
            {
                "download_throughput_mbps": float,
                "upload_throughput_mbps": float,
                "latency_ms": float,
                "packet_loss": float,
            }

        Raises:
            FileNotFoundError: If requested data is not available in cache.
            ValueError: If requested percentile is not available in cached data.
        """
        # Normalize country to be lowercase
        country_lower = country.lower()

        # Hard-coded data we have: October 2024 for US, DE, BR
        # Check if we have this exact data
        if country_lower == "us" and start_date == datetime(2024, 10, 1) and end_date is None:
            filename = "us_2024_10.json"

        elif country_lower == "de" and start_date == datetime(2024, 10, 1) and end_date is None:
            filename = "de_2024_10.json"

        elif country_lower == "br" and start_date == datetime(2024, 10, 1) and end_date is None:
            filename = "br_2024_10.json"

        else:
            raise FileNotFoundError(
                f"No cached data for country={country}, "
                f"start_date={start_date}, end_date={end_date}. "
                f"Currently only have: US/DE/BR for October 2024."
            )

        # Load from file
        filepath = self.cache_dir / filename
        with open(filepath) as filep:
            data = json.load(filep)

        # Extract the requested percentile
        return self._extract_percentile(data, percentile)

    def _extract_percentile(self, data: dict, percentile: int) -> dict:
        """
        Extract specific percentile from JSON data.

        Converts from JSON format to IQBCalculator format:
        - Input: {"metrics": {"download_throughput_mbps": {"p95": 123, ...}, ...}}
        - Output: {"download_throughput_mbps": 123, ...}

        Args:
            data: Full JSON data structure from cache file.
            percentile: Which percentile to extract.

        Returns:
            dict with metric values for the specified percentile.

        Raises:
            ValueError: If requested percentile is not available in the cached data.
        """
        metrics = data["metrics"]
        p_key = f"p{percentile}"

        try:
            return {
                "download_throughput_mbps": metrics["download_throughput_mbps"][p_key],
                "upload_throughput_mbps": metrics["upload_throughput_mbps"][p_key],
                "latency_ms": metrics["latency_ms"][p_key],
                "packet_loss": metrics["packet_loss"][p_key],
            }
        except KeyError as err:
            # Determine which percentiles ARE available
            available = sorted(
                [int(k[1:]) for k in metrics["download_throughput_mbps"] if k.startswith("p")]
            )
            raise ValueError(
                f"Percentile {percentile} not available in cached data. "
                f"Available percentiles: {available}"
            ) from err
