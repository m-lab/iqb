"""
Script to extract country-specific GeoJSON files from Natural Earth data.
Scans cache dir for files like zm_2024_10.json, then extracts matching
country boundaries from ne_10m_admin_0_countries shapefile.
"""

import re
from pathlib import Path

import geopandas as gpd
import pycountry


def extract_geojsons(
    cache_dir: str = "/data/cache/v0",
    ne_file: str = "ne_10m_admin_0_countries.shp",
    output_dir: str = "geojson_countries",
) -> None:
    """
    Extract country GeoJSONs from Natural Earth shapefile based on countries found in cache.
    """
    cache_path = Path(cache_dir)
    ne_path = Path(ne_file)
    output_path = Path(output_dir)

    if not cache_path.exists():
        print(f"❌ Cache directory not found: {cache_path}")
        return

    if not ne_path.exists():
        print(f"❌ Natural Earth file not found: {ne_path}")
        return

    output_path.mkdir(exist_ok=True, parents=True)

    # Load Natural Earth shapefile
    print(f"Loading {ne_path}...")
    gdf = gpd.read_file(ne_path)
    print(f"Loaded {len(gdf)} features")
    print(f"Columns: {list(gdf.columns)}\n")

    # Pattern: xx_yyyy_mm.json
    pattern = re.compile(r"^([a-z]{2})_(\d{4})_(\d{1,2})\.json$", re.IGNORECASE)

    # Find unique country codes in cache
    country_codes = set()
    for file_path in cache_path.glob("*.json"):
        match = pattern.match(file_path.name)
        if match:
            country_codes.add(match.group(1).upper())

    print(f"Found {len(country_codes)} countries in cache\n")

    # Extract each country from Natural Earth
    for code in sorted(country_codes):
        country = pycountry.countries.get(alpha_2=code)
        if not country:
            print(f"⚠ Unknown country code: {code}")
            continue

        # Filter by ISO_A2
        country_gdf = gdf[gdf["ISO_A2"] == code]

        if len(country_gdf) > 0:
            output_file = output_path / f"{country.alpha_3}.geojson"
            country_gdf.to_file(output_file, driver="GeoJSON")

            size_kb = output_file.stat().st_size / 1024
            print(f"✓ {country.name:30} → {output_file.name} ({size_kb:.1f} KB)")
        else:
            print(f"⚠ {country.name} ({code}): No features found in NE data")

    print(f"\n✓ Done! Files saved to: {output_path.absolute()}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract country GeoJSONs from Natural Earth data"
    )
    parser.add_argument(
        "--cache-dir",
        default="/data/cache/v0",
        help="Cache directory with country files",
    )
    parser.add_argument(
        "--ne-file",
        default="ne_10m_admin_0_countries.shp",
        help="Natural Earth shapefile (.shp)",
    )
    parser.add_argument(
        "--output-dir", default="geojson_countries", help="Output directory"
    )

    args = parser.parse_args()
    extract_geojsons(
        cache_dir=args.cache_dir, ne_file=args.ne_file, output_dir=args.output_dir
    )
