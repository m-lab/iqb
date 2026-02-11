"""
Script to extract subdivision-specific GeoJSON files from Natural Earth admin1 data.
Uses ne_10m_admin_1_states_provinces shapefile to get state/province boundaries.
"""

from pathlib import Path

import geopandas as gpd
import pycountry


def extract_admin1_geojsons(
    ne_file: str = "ne_10m_admin_1_states_provinces.shp",
    output_dir: str = "geojson_admin1",
    countries: list[str] | None = None,
) -> None:
    """
    Extract admin1 (state/province) GeoJSONs from Natural Earth shapefile.

    Args:
        ne_file: Path to Natural Earth admin1 shapefile
        output_dir: Output directory for GeoJSON files
        countries: Optional list of 2-letter country codes to filter (e.g., ["US", "DE"])
                   If None, extracts all countries
    """
    ne_path = Path(ne_file)
    output_path = Path(output_dir)

    if not ne_path.exists():
        print(f"❌ Natural Earth file not found: {ne_path}")
        print(
            "Download from: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/"
        )
        return

    output_path.mkdir(exist_ok=True, parents=True)

    # Load Natural Earth admin1 shapefile
    print(f"Loading {ne_path}...")
    gdf = gpd.read_file(ne_path)
    print(f"Loaded {len(gdf)} features")
    print(f"Columns: {list(gdf.columns)}\n")

    # Key columns in admin1 shapefile:
    # - iso_a2: 2-letter country code
    # - iso_3166_2: Full ISO 3166-2 code (e.g., "US-CA")
    # - name: Subdivision name
    # - admin: Country name

    # Get unique country codes from the data
    available_countries = gdf["iso_a2"].dropna().unique()
    print(f"Available countries in dataset: {len(available_countries)}")

    # Filter countries if specified
    if countries:
        country_codes = [c.upper() for c in countries]
    else:
        country_codes = sorted(available_countries)

    print(f"Processing {len(country_codes)} countries\n")

    # Extract subdivisions for each country
    for code in sorted(country_codes):
        if code == "-1" or code == "-99":  # Skip invalid codes
            continue

        country = pycountry.countries.get(alpha_2=code)
        if not country:
            print(f"⚠ Unknown country code: {code}")
            continue

        # Filter by iso_a2
        country_gdf = gdf[gdf["iso_a2"] == code].copy()

        if len(country_gdf) > 0:
            # Save all subdivisions for this country in one file
            output_file = output_path / f"{country.alpha_3}_admin1.geojson"
            country_gdf.to_file(output_file, driver="GeoJSON")

            size_kb = output_file.stat().st_size / 1024
            subdivision_count = len(country_gdf)
            print(
                f"✓ {country.name:30} → {output_file.name} ({subdivision_count} subdivisions, {size_kb:.1f} KB)"
            )
        else:
            print(f"⚠ {country.name} ({code}): No subdivisions found in NE data")

    print(f"\n✓ Done! Files saved to: {output_path.absolute()}")


def extract_admin1_by_subdivision(
    ne_file: str = "ne_10m_admin_1_states_provinces.shp",
    output_dir: str = "geojson_subdivisions",
    countries: list[str] | None = None,
) -> None:
    """
    Extract individual GeoJSON files for each subdivision.
    Creates one file per subdivision (e.g., US-CA.geojson, US-NY.geojson).

    Args:
        ne_file: Path to Natural Earth admin1 shapefile
        output_dir: Output directory for GeoJSON files
        countries: Optional list of 2-letter country codes to filter
    """
    ne_path = Path(ne_file)
    output_path = Path(output_dir)

    if not ne_path.exists():
        print(f"❌ Natural Earth file not found: {ne_path}")
        return

    output_path.mkdir(exist_ok=True, parents=True)

    print(f"Loading {ne_path}...")
    gdf = gpd.read_file(ne_path)
    print(f"Loaded {len(gdf)} features\n")

    # Filter by countries if specified
    if countries:
        country_codes = [c.upper() for c in countries]
        gdf = gdf[gdf["iso_a2"].isin(country_codes)]
        print(f"Filtered to {len(gdf)} features for countries: {country_codes}\n")

    # Extract each subdivision
    count = 0
    for _, row in gdf.iterrows():
        iso_code = row.get("iso_3166_2")
        if not iso_code or iso_code in ["-1", "-99"]:
            continue

        # Create a single-row GeoDataFrame
        subdivision_gdf = gpd.GeoDataFrame([row], crs=gdf.crs)

        # Clean filename (replace invalid chars)
        safe_code = iso_code.replace("/", "-").replace("\\", "-")
        output_file = output_path / f"{safe_code}.geojson"

        subdivision_gdf.to_file(output_file, driver="GeoJSON")
        count += 1

    print(f"✓ Extracted {count} subdivision files to: {output_path.absolute()}")


def extract_cities_geojson(
    ne_file: str = "ne_10m_populated_places_simple.shp",
    output_file: str = "ne_cities.geojson",
) -> None:
    """
    Extract cities from Natural Earth populated places shapefile to GeoJSON.

    Args:
        ne_file: Path to Natural Earth populated places shapefile
        output_file: Output GeoJSON file path
    """
    ne_path = Path(ne_file)
    output_path = Path(output_file)

    if not ne_path.exists():
        print(f"❌ Natural Earth file not found: {ne_path}")
        print(
            "Download from: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/"
        )
        return

    output_path.parent.mkdir(exist_ok=True, parents=True)

    print(f"Loading {ne_path}...")
    gdf = gpd.read_file(ne_path)
    print(f"Loaded {len(gdf)} cities")
    print(f"Columns: {list(gdf.columns)}\n")

    # Keep only essential columns to reduce file size
    # Common columns: name, nameascii, latitude, longitude, iso_a2, adm1name, pop_max
    keep_cols = [
        "name",
        "nameascii",
        "latitude",
        "longitude",
        "iso_a2",
        "adm1name",
        "adm0name",
        "pop_max",
        "geometry",
    ]
    available_cols = [c for c in keep_cols if c in gdf.columns]
    gdf_slim = gdf[available_cols].copy()

    # Save to GeoJSON
    gdf_slim.to_file(output_path, driver="GeoJSON")

    size_kb = output_path.stat().st_size / 1024
    print(f"✓ Extracted {len(gdf_slim)} cities to: {output_path}")
    print(f"  Size: {size_kb:.1f} KB")
    print(f"  Columns: {available_cols}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract admin1 GeoJSONs from Natural Earth data"
    )
    parser.add_argument(
        "--ne-file",
        default="ne_10m_admin_1_states_provinces.shp",
        help="Natural Earth admin1 shapefile (.shp)",
    )
    parser.add_argument(
        "--output-dir", default="geojson_admin1", help="Output directory"
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Optional: specific country codes to extract (e.g., US DE FR)",
    )
    parser.add_argument(
        "--by-subdivision",
        action="store_true",
        help="Create individual files per subdivision instead of per country",
    )
    parser.add_argument(
        "--cities",
        action="store_true",
        help="Extract cities from populated places file instead of admin boundaries",
    )
    parser.add_argument(
        "--cities-file",
        default="ne_10m_populated_places_simple.shp",
        help="Natural Earth populated places shapefile (used with --cities)",
    )

    args = parser.parse_args()

    if args.cities:
        output_file = (
            Path(args.output_dir).parent / "geojson_cities" / "ne_cities.geojson"
        )
        extract_cities_geojson(
            ne_file=args.cities_file,
            output_file=str(output_file),
        )
    elif args.by_subdivision:
        extract_admin1_by_subdivision(
            ne_file=args.ne_file,
            output_dir=args.output_dir,
            countries=args.countries,
        )
    else:
        extract_admin1_geojsons(
            ne_file=args.ne_file,
            output_dir=args.output_dir,
            countries=args.countries,
        )
