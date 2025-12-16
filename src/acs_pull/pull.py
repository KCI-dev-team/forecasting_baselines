"""
ACS Data Pull v2 - Census Bureau API Data Pipeline
Pulls American Community Survey (ACS) 1-year estimates at the place (city) level.
"""

import os
import re
import time
import json
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

API_KEY = os.getenv("CENSUS_API_KEY")
if not API_KEY:
    raise ValueError("CENSUS_API_KEY not found in environment variables")

BASE_URL = "https://api.census.gov/data/{year}/acs/acs1"

# 52 State/Territory FIPS codes (50 states + DC + Puerto Rico)
STATE_FIPS = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
}

# ACS Groups - simplified list for faster testing
ACS_GROUPS = [
    "B01001", "B01003", "B02003", "B00001", "B08101", "B07409", "B08303", "B14007",
    "B15012", "B17026", "B19081", "B19083", "B23020", "B25070", "B25104"
]

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
TMP_DIR = PROJECT_ROOT / "tmp"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "acs_raw"


def fetch_group_descriptions(year: int) -> dict[str, str]:
    """Fetch group code -> description mapping from Census API."""
    url = f"https://api.census.gov/data/{year}/acs/acs1/groups.json"
    try:
        response = httpx.get(url, timeout=60)
        response.raise_for_status()
        groups = response.json()["groups"]
        return {g["name"]: g["description"] for g in groups}
    except Exception:
        return {}


def clean_col_name(col: str) -> str:
    """Clean column name: lowercase, no spaces or special chars."""
    if col in ("place_fips", "place_name", "state_fips", "year"):
        return col
    # Lowercase
    col = col.lower()
    # Replace spaces, !!, :, and other special chars with underscores
    col = re.sub(r"[!\s:;,\-\(\)\'\"]+", "_", col)
    # Replace multiple underscores with single
    col = re.sub(r"_+", "_", col)
    # Remove trailing underscores
    col = col.strip("_")
    return col


def collect_year(year: int) -> pd.DataFrame:
    """
    Collect all ACS data for one year across all states.
    Single function with tqdm progress bar for accurate tracking.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Fetch group descriptions for readable column names
    print("Fetching group descriptions...")
    group_descriptions = fetch_group_descriptions(year)

    # Build list of all (state, group) combinations
    tasks = [(state_fips, group) for state_fips in STATE_FIPS for group in ACS_GROUPS]
    total_tasks = len(tasks)

    # Store results by state and code->label mapping
    state_data: dict[str, pd.DataFrame | None] = {s: None for s in STATE_FIPS}
    code_to_label: dict[str, str] = {}

    with tqdm(total=total_tasks, desc=f"Year {year}", unit="req") as pbar:
        for state_fips, group in tasks:
            pbar.set_postfix(state=STATE_FIPS[state_fips][:8], group=group)

            # Fetch data with descriptive=true to get both codes and labels
            url = f"{BASE_URL.format(year=year)}?get=group({group})&for=place:*&in=state:{state_fips}&key={API_KEY}&descriptive=true"
            df = None

            for attempt in range(3):
                try:
                    response = httpx.get(url, timeout=60)
                    response.raise_for_status()
                    data = response.json()

                    # With descriptive=true: row 0 = codes, row 1 = labels, row 2+ = data
                    if data and len(data) >= 3:
                        codes = data[0]
                        labels = data[1]
                        # Build code->label mapping with group description prefix
                        group_desc = group_descriptions.get(group, group)
                        for code, label in zip(codes, labels):
                            if code not in code_to_label:
                                # Prefix with group description for non-identifier columns
                                if code not in ("GEO_ID", "NAME", "state", "place"):
                                    code_to_label[code] = f"{group_desc}__{label}"
                                else:
                                    code_to_label[code] = label
                        # Create DataFrame with codes as columns
                        df = pd.DataFrame(data[2:], columns=codes)
                    break

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        break  # Group doesn't exist for this year/state
                except (httpx.RequestError, json.JSONDecodeError):
                    if attempt < 2:
                        time.sleep(2 ** attempt)

            # Merge into state data
            if df is not None and not df.empty:
                existing_df = state_data[state_fips]
                if existing_df is None:
                    state_data[state_fips] = df
                else:
                    # Merge on place identifiers (codes)
                    merge_cols = ["state", "place"]
                    if "NAME" in df.columns:
                        merge_cols.append("NAME")
                    existing_cols = set(existing_df.columns)
                    new_cols = [c for c in df.columns if c not in existing_cols or c in merge_cols]
                    state_data[state_fips] = existing_df.merge(
                        df[new_cols],
                        on=[c for c in merge_cols if c in df.columns],
                        how="outer"
                    )

            time.sleep(0.3)  # Rate limiting
            pbar.update(1)

    # Combine all states
    all_dfs = [df for df in state_data.values() if df is not None and not df.empty]
    if not all_dfs:
        return pd.DataFrame()

    result = pd.concat(all_dfs, ignore_index=True)

    # Clean the data
    result["year"] = year

    # Rename identifier columns
    if "NAME" in result.columns:
        result = result.rename(columns={"NAME": "place_name"})
    if "state" in result.columns:
        result = result.rename(columns={"state": "state_fips"})
    if "place" in result.columns:
        result["place_fips"] = result["state_fips"] + result["place"]
        result = result.drop(columns=["place"])
    if "GEO_ID" in result.columns:
        result = result.drop(columns=["GEO_ID"])

    # Replace missing markers and convert to numeric
    missing_markers = [-666666666, -999999999, -888888888, "-666666666", "-999999999", "-888888888"]
    result = result.replace(missing_markers, None)

    id_cols = {"place_fips", "place_name", "state_fips", "year"}
    for col in result.columns:
        if col not in id_cols:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    # Rename columns from codes to descriptive labels
    result = result.rename(columns=code_to_label)

    # Clean column names: lowercase, no spaces or special chars
    result = result.rename(columns=clean_col_name)

    # Reorder columns
    first_cols = ["place_fips", "place_name", "state_fips", "year"]
    other_cols = [c for c in result.columns if c not in first_cols]
    result = result[first_cols + other_cols]

    return result


def run_single_year(year: int = 2023) -> None:
    """Run data pull for a single year and save outputs."""
    print(f"\nCollecting ACS data for year {year}")
    print(f"States: {len(STATE_FIPS)}, Groups: {len(ACS_GROUPS)}")
    print(f"Total API calls: {len(STATE_FIPS) * len(ACS_GROUPS)}\n")

    df = collect_year(year)

    if df.empty:
        print("No data collected.")
        return

    # Save outputs
    output_path = DATA_RAW_DIR / f"acs_{year}.parquet"
    df.to_parquet(output_path, index=False)

    csv_path = DATA_RAW_DIR / f"acs_{year}.csv"
    df.to_csv(csv_path, index=False)

    # Summary
    print(f"\n{'='*50}")
    print(f"SUMMARY - Year {year}")
    print(f"{'='*50}")
    print(f"Total places: {len(df)}")
    print(f"Total columns: {len(df.columns)}")
    print(f"States: {df['state_fips'].nunique()}")
    print(f"\nSaved to: {output_path}")
    print(f"CSV: {csv_path}")

    # Major city check
    print("\nMajor cities:")
    for city in ["Los Angeles", "New York", "Chicago", "Houston", "Phoenix"]:
        found = df["place_name"].str.contains(city, case=False, na=False).any()
        print(f"  {'Y' if found else 'N'} {city}")


def run_all_years(start: int = 2009, end: int = 2024) -> None:
    """Run data pull for multiple years."""
    print(f"\nCollecting ACS data for years {start}-{end}")
    print(f"States: {len(STATE_FIPS)}, Groups: {len(ACS_GROUPS)}")

    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    for year in range(start, end + 1):
        print(f"\n{'#'*60}")
        print(f"YEAR {year}")
        print(f"{'#'*60}")

        try:
            df = collect_year(year)
            if not df.empty:
                output_path = DATA_RAW_DIR / f"acs_{year}.parquet"
                df.to_parquet(output_path, index=False)
                print(f"Saved {len(df)} rows to {output_path}")
            else:
                print(f"No data for year {year}")
        except Exception as e:
            print(f"Error for year {year}: {e}")

    print(f"\n{'='*60}")
    print("DONE - All years complete")
    print(f"{'='*60}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            start = int(sys.argv[2]) if len(sys.argv) > 2 else 2009
            end = int(sys.argv[3]) if len(sys.argv) > 3 else 2024
            run_all_years(start, end)
        else:
            year = int(sys.argv[1])
            run_single_year(year)
    else:
        print("Usage:")
        print("  uv run python -m src.acs_pull.pull <year>       # Single year")
        print("  uv run python -m src.acs_pull.pull --all        # All years (2009-2024)")
        print("  uv run python -m src.acs_pull.pull --all 2015 2020  # Custom range")
