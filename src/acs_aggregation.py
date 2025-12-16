"""
ACS Data Aggregation Script

Aggregates ACS raw data columns to reduce dimensionality while preserving meaningful signals.
- Input: data/acs_raw/acs_{year}.parquet (2009-2024, excluding 2020)
- Output: data/acs_agg/acs_{year}.parquet
"""

from pathlib import Path

import pandas as pd

# Constants
RAW_DATA_DIR = Path("data/acs_raw")
AGG_DATA_DIR = Path("data/acs_agg")
YEARS = [y for y in range(2009, 2025) if y != 2020]
ID_COLS = ["place_fips", "place_name", "state_fips", "year"]


def load_raw_data(year: int) -> pd.DataFrame:
    """Load a year's parquet file."""
    path = RAW_DATA_DIR / f"acs_{year}.parquet"
    return pd.read_parquet(path)


def filter_estimate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only columns containing '_estimate_' but NOT containing 'annotation' or 'margin'."""
    estimate_cols = [
        c
        for c in df.columns
        if "_estimate_" in c
        and "margin" not in c.lower()
        and "annotation" not in c.lower()
    ]
    return df[ID_COLS + estimate_cols]


def get_col(df: pd.DataFrame, pattern: str) -> pd.Series:
    """Get a column matching pattern, returning zeros if not found."""
    matches = [c for c in df.columns if pattern in c]
    if matches:
        return df[matches[0]].fillna(0)
    return pd.Series(0, index=df.index)


def safe_sum(df: pd.DataFrame, patterns: list[str]) -> pd.Series:
    """Sum columns matching patterns, handling missing columns gracefully."""
    result = pd.Series(0, index=df.index, dtype=float)
    for pattern in patterns:
        result = result + get_col(df, pattern)
    return result


# =============================================================================
# AGGREGATION FUNCTIONS
# =============================================================================


def aggregate_sex_by_age(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate sex_by_age columns into age buckets.
    Output: 21 columns (total + 10 buckets for male + 10 buckets for female)
    """
    result = pd.DataFrame(index=df.index)

    # Total
    result["sex_by_age_total"] = get_col(df, "sex_by_age_estimate_total")

    # Age bucket mappings
    buckets = {
        "age_18_and_under": [
            "under_5_years",
            "5_to_9_years",
            "10_to_14_years",
            "15_to_17_years",
        ],
        "age_19_to_21": ["18_and_19_years", "20_years", "21_years"],
        "age_22_to_24": ["22_to_24_years"],
        "age_25_to_29": ["25_to_29_years"],
        "age_30_to_39": ["30_to_34_years", "35_to_39_years"],
        "age_40_to_49": ["40_to_44_years", "45_to_49_years"],
        "age_50_to_59": ["50_to_54_years", "55_to_59_years"],
        "age_60_to_69": [
            "60_and_61_years",
            "62_to_64_years",
            "65_and_66_years",
            "67_to_69_years",
        ],
        "age_70_to_79": ["70_to_74_years", "75_to_79_years"],
        "age_80_plus": ["80_to_84_years", "85_years_and_over"],
    }

    for sex in ["male", "female"]:
        for bucket_name, age_groups in buckets.items():
            patterns = [
                f"sex_by_age_estimate_total_{sex}_{age}" for age in age_groups
            ]
            result[f"sex_by_age_{sex}_{bucket_name}"] = safe_sum(df, patterns)

    return result


def aggregate_school_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate school_enrollment columns into education levels.
    Output: 7 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "school_enrollment_by_detailed_level_of_school_for_the_population_3_years_and_over_estimate_total"

    # Keep totals
    result["school_enrollment_total"] = get_col(df, f"{prefix}")
    result["school_enrollment_enrolled"] = get_col(df, f"{prefix}_enrolled_in_school")
    result["school_enrollment_not_enrolled"] = get_col(df, f"{prefix}_not_enrolled_in_school")

    # Aggregate below high school: nursery through grade 8
    below_hs_patterns = [
        f"{prefix}_enrolled_in_school_enrolled_in_nursery_school_preschool",
        f"{prefix}_enrolled_in_school_enrolled_in_kindergarten",
    ] + [f"{prefix}_enrolled_in_school_enrolled_in_grade_{i}" for i in range(1, 9)]
    result["school_enrollment_below_high_school"] = safe_sum(df, below_hs_patterns)

    # High school: grades 9-12
    hs_patterns = [f"{prefix}_enrolled_in_school_enrolled_in_grade_{i}" for i in range(9, 13)]
    result["school_enrollment_high_school"] = safe_sum(df, hs_patterns)

    # Undergraduate
    result["school_enrollment_undergraduate"] = get_col(
        df, f"{prefix}_enrolled_in_school_enrolled_in_college_undergraduate_years"
    )

    # Graduate
    result["school_enrollment_graduate"] = get_col(
        df, f"{prefix}_enrolled_in_school_graduate_or_professional_school"
    )

    return result


def aggregate_monthly_housing_costs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate monthly_housing_costs into $500 increments.
    Output: 9 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "monthly_housing_costs_estimate_total"

    # Total and no cash rent
    result["monthly_housing_costs_total"] = get_col(df, prefix)
    result["monthly_housing_costs_no_cash_rent"] = get_col(df, f"{prefix}_no_cash_rent")

    # Under $500
    under_500_patterns = [
        f"{prefix}_less_than_$100",
        f"{prefix}_$100_to_$199",
        f"{prefix}_$200_to_$299",
        f"{prefix}_$300_to_$399",
        f"{prefix}_$400_to_$499",
    ]
    result["monthly_housing_costs_under_500"] = safe_sum(df, under_500_patterns)

    # $500-$999
    mid_patterns = [
        f"{prefix}_$500_to_$599",
        f"{prefix}_$600_to_$699",
        f"{prefix}_$700_to_$799",
        f"{prefix}_$800_to_$899",
        f"{prefix}_$900_to_$999",
    ]
    result["monthly_housing_costs_500_to_999"] = safe_sum(df, mid_patterns)

    # Higher ranges (already aggregated in source)
    result["monthly_housing_costs_1000_to_1499"] = get_col(df, f"{prefix}_$1_000_to_$1_499")
    result["monthly_housing_costs_1500_to_1999"] = get_col(df, f"{prefix}_$1_500_to_$1_999")
    result["monthly_housing_costs_2000_to_2499"] = get_col(df, f"{prefix}_$2_000_to_$2_499")
    result["monthly_housing_costs_2500_to_2999"] = get_col(df, f"{prefix}_$2_500_to_$2_999")
    result["monthly_housing_costs_3000_plus"] = get_col(df, f"{prefix}_$3_000_or_more")

    return result


def aggregate_gross_rent_pct_income(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate gross_rent_as_percentage_of_income into ~10% increments.
    Output: 6 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "gross_rent_as_a_percentage_of_household_income_in_the_past_12_months_estimate_total"

    # Total and not computed
    result["gross_rent_pct_income_total"] = get_col(df, prefix)
    result["gross_rent_pct_income_not_computed"] = get_col(df, f"{prefix}_not_computed")

    # Under 20%
    under_20_patterns = [
        f"{prefix}_less_than_10.0_percent",
        f"{prefix}_10.0_to_14.9_percent",
        f"{prefix}_15.0_to_19.9_percent",
    ]
    result["gross_rent_pct_income_under_20"] = safe_sum(df, under_20_patterns)

    # 20-29%
    pct_20_29_patterns = [
        f"{prefix}_20.0_to_24.9_percent",
        f"{prefix}_25.0_to_29.9_percent",
    ]
    result["gross_rent_pct_income_20_to_29"] = safe_sum(df, pct_20_29_patterns)

    # 30-39%
    pct_30_39_patterns = [
        f"{prefix}_30.0_to_34.9_percent",
        f"{prefix}_35.0_to_39.9_percent",
    ]
    result["gross_rent_pct_income_30_to_39"] = safe_sum(df, pct_30_39_patterns)

    # 40%+
    pct_40_plus_patterns = [
        f"{prefix}_40.0_to_49.9_percent",
        f"{prefix}_50.0_percent_or_more",
    ]
    result["gross_rent_pct_income_40_plus"] = safe_sum(df, pct_40_plus_patterns)

    return result


def aggregate_poverty_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate ratio_of_income_to_poverty into binary split at poverty line (1.0).
    Output: 3 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "ratio_of_income_to_poverty_level_of_families_in_the_past_12_months_estimate_total"

    # Total
    result["poverty_ratio_total"] = get_col(df, prefix)

    # At or below poverty (under 1.0)
    below_patterns = [
        f"{prefix}_under_.50",
        f"{prefix}_.50_to_.74",
        f"{prefix}_.75_to_.99",
    ]
    result["poverty_ratio_at_or_below"] = safe_sum(df, below_patterns)

    # Above poverty (1.0 and over)
    above_patterns = [
        f"{prefix}_1.00_to_1.24",
        f"{prefix}_1.25_to_1.49",
        f"{prefix}_1.50_to_1.74",
        f"{prefix}_1.75_to_1.84",
        f"{prefix}_1.85_to_1.99",
        f"{prefix}_2.00_to_2.99",
        f"{prefix}_3.00_to_3.99",
        f"{prefix}_4.00_to_4.99",
        f"{prefix}_5.00_and_over",
    ]
    result["poverty_ratio_above"] = safe_sum(df, above_patterns)

    return result


def aggregate_travel_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate travel_time_to_work into 10-minute increments.
    Output: 7 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "travel_time_to_work_estimate_total"

    # Total
    result["travel_time_total"] = get_col(df, prefix)

    # Under 10 min
    result["travel_time_under_10"] = safe_sum(
        df, [f"{prefix}_less_than_5_minutes", f"{prefix}_5_to_9_minutes"]
    )

    # 10-19 min
    result["travel_time_10_to_19"] = safe_sum(
        df, [f"{prefix}_10_to_14_minutes", f"{prefix}_15_to_19_minutes"]
    )

    # 20-29 min
    result["travel_time_20_to_29"] = safe_sum(
        df, [f"{prefix}_20_to_24_minutes", f"{prefix}_25_to_29_minutes"]
    )

    # 30-39 min
    result["travel_time_30_to_39"] = safe_sum(
        df, [f"{prefix}_30_to_34_minutes", f"{prefix}_35_to_39_minutes"]
    )

    # 40-59 min
    result["travel_time_40_to_59"] = safe_sum(
        df, [f"{prefix}_40_to_44_minutes", f"{prefix}_45_to_59_minutes"]
    )

    # 60+ min
    result["travel_time_60_plus"] = safe_sum(
        df, [f"{prefix}_60_to_89_minutes", f"{prefix}_90_or_more_minutes"]
    )

    return result


def filter_transportation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only transportation mode totals, drop all age breakdowns.
    Output: 7 columns
    """
    result = pd.DataFrame(index=df.index)
    prefix = "means_of_transportation_to_work_by_age_estimate_total"

    # Total
    result["transportation_total"] = get_col(df, prefix)

    # Mode totals (not ending in age groups)
    result["transportation_drove_alone"] = get_col(
        df, f"{prefix}_car_truck_or_van_drove_alone"
    )
    result["transportation_carpooled"] = get_col(
        df, f"{prefix}_car_truck_or_van_carpooled"
    )
    result["transportation_public_transit"] = get_col(
        df, f"{prefix}_public_transportation_excluding_taxicab"
    )
    result["transportation_walked"] = get_col(df, f"{prefix}_walked")
    result["transportation_taxi_bike_other"] = get_col(
        df, f"{prefix}_taxicab_motorcycle_bicycle_or_other_means"
    )
    result["transportation_worked_from_home"] = get_col(
        df, f"{prefix}_worked_from_home"
    )

    return result


def rename_geo_mobility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep geographical_mobility columns, rename for brevity.
    Output: 30 columns
    """
    result = pd.DataFrame(index=df.index)
    long_prefix = "geographical_mobility_in_the_past_year_by_educational_attainment_for_residence_1_year_ago_in_the_united_states_estimate_total_living_in_area_1_year_ago"
    short_prefix = "geo_mobility"

    geo_cols = [c for c in df.columns if long_prefix in c]

    for col in geo_cols:
        # Create shorter name
        suffix = col.replace(long_prefix, "").strip("_")
        if suffix == "":
            new_name = f"{short_prefix}_total"
        else:
            new_name = f"{short_prefix}_{suffix}"
        result[new_name] = df[col].fillna(0)

    return result


def extract_other_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep other variable groups as-is with simplified names.
    Output: ~26 columns
    """
    result = pd.DataFrame(index=df.index)

    # Total population
    result["total_population"] = get_col(df, "total_population_estimate_total")

    # Gini index
    result["gini_index"] = get_col(df, "gini_index_of_income_inequality_estimate_gini_index")

    # Income quintiles
    quintile_map = {
        "income_quintile_lowest": "mean_household_income_of_quintiles_estimate_quintile_means_lowest_quintile",
        "income_quintile_second": "mean_household_income_of_quintiles_estimate_quintile_means_second_quintile",
        "income_quintile_third": "mean_household_income_of_quintiles_estimate_quintile_means_third_quintile",
        "income_quintile_fourth": "mean_household_income_of_quintiles_estimate_quintile_means_fourth_quintile",
        "income_quintile_highest": "mean_household_income_of_quintiles_estimate_quintile_means_highest_quintile",
        "income_quintile_top_5_pct": "mean_household_income_of_quintiles_estimate_top_5_percent",
    }
    for new_name, old_pattern in quintile_map.items():
        result[new_name] = get_col(df, old_pattern)

    # Hours worked
    hours_map = {
        "hours_worked_total": "mean_usual_hours_worked_in_the_past_12_months_for_workers_16_to_64_years_estimate_mean_usual_hours_total",
        "hours_worked_male": "mean_usual_hours_worked_in_the_past_12_months_for_workers_16_to_64_years_estimate_mean_usual_hours_total_male",
        "hours_worked_female": "mean_usual_hours_worked_in_the_past_12_months_for_workers_16_to_64_years_estimate_mean_usual_hours_total_female",
    }
    for new_name, old_pattern in hours_map.items():
        result[new_name] = get_col(df, old_pattern)

    # Bachelor's degrees
    bachelors_prefix = "total_fields_of_bachelor_s_degrees_reported_estimate_total"
    bachelors_map = {
        "bachelors_degree_total": bachelors_prefix,
        "bachelors_degree_computers_math_stats": f"{bachelors_prefix}_science_and_engineering_computers_mathematics_and_statistics",
        "bachelors_degree_bio_ag_env": f"{bachelors_prefix}_science_and_engineering_biological_agricultural_and_environmental_sciences",
        "bachelors_degree_physical_sciences": f"{bachelors_prefix}_science_and_engineering_physical_and_related_sciences",
        "bachelors_degree_psychology": f"{bachelors_prefix}_science_and_engineering_psychology",
        "bachelors_degree_social_sciences": f"{bachelors_prefix}_science_and_engineering_social_sciences",
        "bachelors_degree_engineering": f"{bachelors_prefix}_science_and_engineering_engineering",
        "bachelors_degree_multidisciplinary": f"{bachelors_prefix}_science_and_engineering_multidisciplinary_studies",
        "bachelors_degree_stem_related": f"{bachelors_prefix}_science_and_engineering_related_fields",
        "bachelors_degree_business": f"{bachelors_prefix}_business",
        "bachelors_degree_education": f"{bachelors_prefix}_education",
        "bachelors_degree_literature_languages": f"{bachelors_prefix}_arts_humanities_and_other_literature_and_languages",
        "bachelors_degree_liberal_arts_history": f"{bachelors_prefix}_arts_humanities_and_other_liberal_arts_and_history",
        "bachelors_degree_visual_performing_arts": f"{bachelors_prefix}_arts_humanities_and_other_visual_and_performing_arts",
        "bachelors_degree_communications": f"{bachelors_prefix}_arts_humanities_and_other_communications",
        "bachelors_degree_other": f"{bachelors_prefix}_arts_humanities_and_other_other",
    }
    for new_name, old_pattern in bachelors_map.items():
        result[new_name] = get_col(df, old_pattern)

    return result


def exclude_race_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop all columns containing 'race' in the name."""
    race_cols = [c for c in df.columns if "race" in c.lower()]
    return df.drop(columns=race_cols, errors="ignore")


def save_aggregated(df: pd.DataFrame, year: int) -> None:
    """Save aggregated data to parquet."""
    AGG_DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = AGG_DATA_DIR / f"acs_{year}.parquet"
    df.to_parquet(path, index=False)
    print(f"Saved: {path}")


def aggregate_year(year: int, verbose: bool = True) -> pd.DataFrame:
    """
    Run full aggregation pipeline for a single year.
    """
    if verbose:
        print(f"\nProcessing year {year}...")

    # Load and filter
    df = load_raw_data(year)
    original_cols = len(df.columns)

    df = filter_estimate_columns(df)
    df = exclude_race_columns(df)

    # Apply all aggregations
    aggregated_dfs = [
        df[ID_COLS],  # Keep identifiers
        aggregate_sex_by_age(df),
        aggregate_school_enrollment(df),
        aggregate_monthly_housing_costs(df),
        aggregate_gross_rent_pct_income(df),
        aggregate_poverty_ratio(df),
        aggregate_travel_time(df),
        filter_transportation(df),
        rename_geo_mobility(df),
        extract_other_variables(df),
    ]

    # Combine all
    result = pd.concat(aggregated_dfs, axis=1)

    if verbose:
        print(f"  Original columns: {original_cols}")
        print(f"  Aggregated columns: {len(result.columns)}")
        print(f"  Rows: {len(result)}")

    return result


def process_all_years(verbose: bool = True) -> dict:
    """
    Process all years and generate column availability report.
    """
    AGG_DATA_DIR.mkdir(parents=True, exist_ok=True)

    results = {}
    all_columns = set()

    for year in YEARS:
        df = aggregate_year(year, verbose=verbose)
        save_aggregated(df, year)
        results[year] = {
            "rows": len(df),
            "columns": list(df.columns),
        }
        all_columns.update(df.columns)

    # Generate column availability report
    if verbose:
        print("\n" + "=" * 60)
        print("COLUMN AVAILABILITY REPORT")
        print("=" * 60)

        # Check which columns are missing in which years
        for col in sorted(all_columns):
            missing_years = []
            for year in YEARS:
                if col not in results[year]["columns"]:
                    missing_years.append(year)
            if missing_years:
                print(f"  {col}: missing in {missing_years}")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total years processed: {len(YEARS)}")
        print(f"Total unique columns: {len(all_columns)}")
        print(f"Years: {YEARS}")

    return results


def validate_output(verbose: bool = True) -> bool:
    """
    Validate the aggregated output files.
    """
    if verbose:
        print("\n" + "=" * 60)
        print("VALIDATION")
        print("=" * 60)

    all_valid = True

    for year in YEARS:
        raw_path = RAW_DATA_DIR / f"acs_{year}.parquet"
        agg_path = AGG_DATA_DIR / f"acs_{year}.parquet"

        if not agg_path.exists():
            if verbose:
                print(f"  {year}: MISSING aggregated file")
            all_valid = False
            continue

        raw_df = pd.read_parquet(raw_path)
        agg_df = pd.read_parquet(agg_path)

        # Check row counts match
        if len(raw_df) != len(agg_df):
            if verbose:
                print(f"  {year}: Row count mismatch (raw={len(raw_df)}, agg={len(agg_df)})")
            all_valid = False
        else:
            if verbose:
                print(f"  {year}: OK (rows={len(agg_df)}, cols={len(agg_df.columns)})")

    return all_valid


if __name__ == "__main__":
    print("ACS Data Aggregation Pipeline")
    print("=" * 60)

    # Process all years
    results = process_all_years(verbose=True)

    # Validate output
    is_valid = validate_output(verbose=True)

    if is_valid:
        print("\nAll validations passed!")
    else:
        print("\nSome validations failed. Please check the output.")