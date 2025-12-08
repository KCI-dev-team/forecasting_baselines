# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
# ]
# ///
"""
Clean and combine US city population data from Census Bureau files.

Combines data from:
- sub-est00int.csv (2000-2010)
- sub-est2020int.csv (2010-2020)
- sub-est2024.csv (2020-2024)

Filters for rows where NAME ends with " city" (case sensitive).
Outputs format similar to zillow_homeval_city.csv with City, State, yearly values, and YoY changes.
"""

import pandas as pd
from pathlib import Path

# State abbreviation mapping
STATE_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY", "District of Columbia": "DC",
    "Puerto Rico": "PR"
}


def load_and_filter_cities(filepath: Path) -> pd.DataFrame:
    """Load CSV and filter for rows where NAME ends with ' city'.

    Filters for SUMLEV 162 (incorporated places) to avoid duplicate entries
    from county subdivisions within places (SUMLEV 157).
    """
    # Try different encodings
    for encoding in ["utf-8", "latin-1", "cp1252"]:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    # Filter for:
    # - SUMLEV 162 (incorporated places) to get city-level totals only
    # - Names ending with " city" (case sensitive)
    mask = (df["SUMLEV"] == 162) & (df["NAME"].str.endswith(" city"))
    return df[mask].copy()


def extract_city_name(name: str) -> str:
    """Extract city name by removing ' city' suffix."""
    if name.endswith(" city"):
        return name[:-5]  # Remove " city"
    return name


def main():
    data_dir = Path(__file__).parent.parent / "regression_data"

    # Load all three files
    print("Loading data files...")
    df_00 = load_and_filter_cities(data_dir / "sub-est00int.csv")
    df_10 = load_and_filter_cities(data_dir / "sub-est2020int.csv")
    df_20 = load_and_filter_cities(data_dir / "sub-est2024.csv")

    print(f"  sub-est00int.csv: {len(df_00)} cities")
    print(f"  sub-est2020int.csv: {len(df_10)} cities")
    print(f"  sub-est2024.csv: {len(df_20)} cities")

    # Extract population estimate columns for each dataset
    # 2000-2010 data (sub-est00int.csv)
    pop_cols_00 = [c for c in df_00.columns if c.startswith("POPESTIMATE")]
    df_00_pop = df_00[["NAME", "STNAME"] + pop_cols_00].copy()

    # 2010-2020 data (sub-est2020int.csv)
    pop_cols_10 = [c for c in df_10.columns if c.startswith("POPESTIMATE")]
    df_10_pop = df_10[["NAME", "STNAME"] + pop_cols_10].copy()

    # 2020-2024 data (sub-est2024.csv)
    pop_cols_20 = [c for c in df_20.columns if c.startswith("POPESTIMATE")]
    df_20_pop = df_20[["NAME", "STNAME"] + pop_cols_20].copy()

    # Extract city name (without " city" suffix)
    df_00_pop["City"] = df_00_pop["NAME"].apply(extract_city_name)
    df_10_pop["City"] = df_10_pop["NAME"].apply(extract_city_name)
    df_20_pop["City"] = df_20_pop["NAME"].apply(extract_city_name)

    # Add state abbreviation
    df_00_pop["State"] = df_00_pop["STNAME"].map(STATE_ABBR)
    df_10_pop["State"] = df_10_pop["STNAME"].map(STATE_ABBR)
    df_20_pop["State"] = df_20_pop["STNAME"].map(STATE_ABBR)

    # Create merge key
    df_00_pop["key"] = df_00_pop["City"] + "_" + df_00_pop["State"]
    df_10_pop["key"] = df_10_pop["City"] + "_" + df_10_pop["State"]
    df_20_pop["key"] = df_20_pop["City"] + "_" + df_20_pop["State"]

    # Rename columns to just years
    for col in pop_cols_00:
        year = col.replace("POPESTIMATE", "")
        df_00_pop.rename(columns={col: year}, inplace=True)

    for col in pop_cols_10:
        year = col.replace("POPESTIMATE", "")
        df_10_pop.rename(columns={col: year}, inplace=True)

    for col in pop_cols_20:
        year = col.replace("POPESTIMATE", "")
        df_20_pop.rename(columns={col: year}, inplace=True)

    # Drop non-year columns except key, City, State
    years_00 = [str(y) for y in range(2000, 2011)]
    years_10 = [str(y) for y in range(2010, 2020)]  # Skip 2010 from 00 file, use from 10 file
    years_20 = [str(y) for y in range(2020, 2025)]

    df_00_final = df_00_pop[["key", "City", "State"] + [y for y in years_00 if y in df_00_pop.columns]]
    df_10_final = df_10_pop[["key"] + [y for y in years_10 if y in df_10_pop.columns]]
    df_20_final = df_20_pop[["key"] + [y for y in years_20 if y in df_20_pop.columns]]

    # Merge datasets
    print("\nMerging datasets...")
    # Start with 2000s data
    merged = df_00_final.copy()

    # Merge with 2010s data (use 2010 from this file, skip from 00)
    # Remove 2010 from merged if present to avoid duplication
    if "2010" in merged.columns:
        merged = merged.drop(columns=["2010"])
    merged = merged.merge(df_10_final, on="key", how="outer")

    # Merge with 2020s data (skip 2020 from 10s file)
    if "2020" in merged.columns:
        merged = merged.drop(columns=["2020"])
    merged = merged.merge(df_20_final, on="key", how="outer")

    # Fill City and State from key for rows that only exist in later files
    merged["City"] = merged.apply(
        lambda row: row["key"].rsplit("_", 1)[0] if pd.isna(row["City"]) else row["City"],
        axis=1
    )
    merged["State"] = merged.apply(
        lambda row: row["key"].rsplit("_", 1)[1] if pd.isna(row["State"]) else row["State"],
        axis=1
    )

    # Get all year columns and sort them
    year_cols = [c for c in merged.columns if c.isdigit()]
    year_cols = sorted(year_cols, key=int)

    print(f"Years covered: {min(year_cols)} - {max(year_cols)}")

    # Calculate YoY changes
    print("Calculating YoY changes...")
    yoy_cols = []
    for i in range(1, len(year_cols)):
        prev_year = year_cols[i - 1]
        curr_year = year_cols[i]
        yoy_col = f"{curr_year}_yoy"
        merged[yoy_col] = (merged[curr_year] - merged[prev_year]) / merged[prev_year]
        yoy_cols.append(yoy_col)

    # Reorder columns: City, State, years, yoy changes
    final_cols = ["City", "State"] + year_cols + yoy_cols
    result = merged[final_cols].copy()

    # Sort by City, State
    result = result.sort_values(["City", "State"]).reset_index(drop=True)

    # Save output
    output_path = data_dir / "city_population.csv"
    result.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    print(f"Total cities: {len(result)}")

    # Show sample
    print("\nSample data (first 5 rows):")
    print(result.head())


if __name__ == "__main__":
    main()
