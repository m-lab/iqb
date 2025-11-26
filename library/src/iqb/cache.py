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
    cache = IQBCache(data_dir="/shared/iqb-cache")
"""

import json
from datetime import datetime
from pathlib import Path


class IQBCache:
    """Component for fetching IQB measurement data from cache."""

    def __init__(self, data_dir: str | Path | None = None):
        """
        Initialize cache with data directory path.

        Parameters:
            data_dir: Path to directory containing cached data files.
                If None, defaults to .iqb/ in current working directory.
        """
        if data_dir is None:
            self.data_dir = Path.cwd() / ".iqb"
        else:
            self.data_dir = Path(data_dir)

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
            country: ISO 2-letter country code (e.g., "US", "DE", "BR", "FR", etc.).
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

        ⚠️  PERCENTILE INTERPRETATION (CRITICAL!)
        =========================================

        For "higher is better" metrics (throughput):
          - Raw p95 = "95% of users have ≤ 625 Mbit/s speed"
          - Directly usable: download_p95 ≥ threshold?
          - No inversion needed (standard statistical definition)

        For "lower is better" metrics (latency, packet loss):
          - Raw p95 = "95% of users have ≤ 80ms latency" (worst-case typical)
          - We want p95 to represent best-case typical (to match throughput)
          - Solution: Use p5 raw labeled as p95
          - Mathematical inversion: p(X)_labeled = p(100-X)_raw
          - Example: OFFSET(5) raw → labeled as "latency_p95" in JSON

        This inversion happens in BigQuery (see data/query_*.sql),
        so this cache code treats all percentiles uniformly.

        When you request percentile=95, you get the 95th percentile value
        that can be compared uniformly against thresholds.

        NOTE: This creates semantics where p95 represents "typical best
        performance" - empirical validation will determine if appropriate.
        """
        # Design Note
        # -----------
        #
        # For now, we are going with separate pipelines producing data for
        # separate data sources, since this scales well incrementally.
        #
        # Additionally, note how the data is relatively small regardless
        # of the time window that we're choosing (it's always four metrics
        # each of which contains between 25 and 100 percentiles). So,
        # computationally, gluing together N datasets in here will never
        # become an expensive operation.
        #
        # We may revisit this choice when we approach production readiness.

        # 1. Normalize country to be lowercase
        country_lower = country.lower()

        # 2. Create the dictionary with the results
        results = {}

        # TODO(bassosimone): a design choice here is whether we want to
        # allow for partial data (e.g., we have m-lab data but we do not
        # have cloudflare data), which happens right now with a static
        # cache and may also happen when fetching from remote. It is not
        # an issue right now, since there's just the m-lab data source.

        # 3. Attempt to fill the results with m-lab data
        results["m-lab"] = self._get_mlab_data(
            country_lower=country_lower,
            start_date=start_date,
            end_date=end_date,
            percentile=percentile,
        )

        # 4. Attempt to fill the results with cloudflare data
        # TODO(bassosimone): implement

        # 5. Attempt to fill the results with ookla data
        # TODO(bassosimone): implement

        # 6. Return assembled result
        return results

    def _get_mlab_data(
        self,
        country_lower: str,
        start_date: datetime,
        end_date: datetime | None = None,
        percentile: int = 95,
    ) -> dict:
        """Return m-lab data with the given country code, dates, etc."""

        # Map datetime to period string
        # We only support single-month periods with end_date=None
        if end_date is not None:
            raise FileNotFoundError(
                f"No cached data for country={country_lower}, "
                f"start_date={start_date}, end_date={end_date}. "
                f"Multi-month periods not yet supported."
            )

        # Map known periods to their string representation
        known_periods = {
            datetime(2024, 10, 1): "2024_10",
            datetime(2025, 10, 1): "2025_10",
        }

        if start_date not in known_periods:
            raise FileNotFoundError(
                f"No cached data for country={country_lower}, "
                f"start_date={start_date}, end_date={end_date}. "
                f"Available periods: {list(known_periods.keys())}"
            )

        period_str = known_periods[start_date]
        filename = f"{country_lower}_{period_str}.json"
        filepath = self.data_dir / "cache" / "v0" / filename

        # Check if file exists
        if not filepath.exists():
            raise FileNotFoundError(
                f"No cached data file found: {filepath}. "
                f"File does not exist for country={country_lower}, period={period_str}"
            )

        # Load from file
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
