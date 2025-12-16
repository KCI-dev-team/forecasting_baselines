# ACS Data Pull v2 - Sequential Implementation Checklist

> Reference: `./SUMMARY.md` for full context.

---

## Decisions

- **Geography**: 50 states + DC + Puerto Rico (52 total)
- **MOE columns**: Keep all Margin of Error columns
- **Output format**: Long-format panel (place_fips, year, var1, var2, ...)

---

## Sequential Checklist

Work through these in order. Check off each item as completed.

### 1. Environment Setup

- [ ] 1.1 Verify `.env` file contains `CENSUS_API_KEY`
- [ ] 1.2 Create directory `src/acs_pull/`
- [ ] 1.3 Create `src/acs_pull/__init__.py` (empty file)
- [ ] 1.4 Add dependencies to `pyproject.toml`: `httpx`, `pandas`, `python-dotenv`, `pyarrow`
- [ ] 1.5 Run `uv sync` to install dependencies

### 2. Create Single Pull Script

- [ ] 2.1 Create `src/acs_pull/pull.py` with all logic in one file
- [ ] 2.2 Add imports: `httpx`, `pandas`, `os`, `time`, `json`, `dotenv`
- [ ] 2.3 Add `load_dotenv()` and read `CENSUS_API_KEY` from env
- [ ] 2.4 Define `STATE_FIPS` dict with all 52 state/territory codes
- [ ] 2.5 Define `ACS_GROUPS` list with all 60+ group codes from PRD
- [ ] 2.6 Define `BASE_URL = "https://api.census.gov/data/{year}/acs/acs1"`

### 3. Implement API Fetch Function

- [ ] 3.1 Write `fetch_group(year: int, group: str, state_fips: str) -> pd.DataFrame`
- [ ] 3.2 Build URL: `{BASE_URL}?get=group({group})&for=place:*&in=state:{state_fips}&key={api_key}`
- [ ] 3.3 Make GET request with `httpx.get(url, timeout=60)`
- [ ] 3.4 Parse JSON response: first row = headers, remaining rows = data
- [ ] 3.5 Return DataFrame with headers as columns
- [ ] 3.6 Add `time.sleep(0.5)` after each request for rate limiting
- [ ] 3.7 Add retry logic: 3 attempts with exponential backoff on failure

### 4. Test Single API Call

- [ ] 4.1 Test fetch for 1 group, 1 state, 1 year: `fetch_group(2023, "B01001", "06")`
- [ ] 4.2 Verify DataFrame is returned with expected columns
- [ ] 4.3 Print sample rows to confirm data looks correct
- [ ] 4.4 Save test output to `tmp/test_fetch.csv` for inspection

### 5. Implement Single Year Collection

- [ ] 5.1 Write `collect_year(year: int) -> pd.DataFrame`
- [ ] 5.2 Loop through all 52 states
- [ ] 5.3 For each state, loop through all groups
- [ ] 5.4 Concatenate all DataFrames by place (merge on place FIPS)
- [ ] 5.5 Add `year` column to the result
- [ ] 5.6 Add progress logging: `print(f"State {i}/52: {state_name}, Group {j}/N: {group}")`
- [ ] 5.7 Implement checkpoint: save partial results after each state to `tmp/checkpoint_{year}_{state}.parquet`

### 6. Handle Data Cleaning

- [ ] 6.1 Replace Census missing value marker `-666666666` with `None`
- [ ] 6.2 Convert numeric columns from string to float
- [ ] 6.3 Rename `NAME` column to `place_name`
- [ ] 6.4 Rename `state` column to `state_fips`
- [ ] 6.5 Create `place_fips` from the place identifier column
- [ ] 6.6 Reorder columns: `place_fips, place_name, state_fips, year, ...variables...`

### 7. Run Single Year Pull (Phase 1 Deliverable)

- [ ] 7.1 Create `data/acs_raw/` directory
- [ ] 7.2 Run `collect_year(2023)`
- [ ] 7.3 Save result to `data/acs_raw/acs_2023.parquet`
- [ ] 7.4 Generate data dictionary: variable code -> description mapping
- [ ] 7.5 Save data dictionary to `data/acs_raw/data_dictionary_2023.csv`
- [ ] 7.6 Print summary: row count, column count, sample of place names

### 8. Validate Single Year Output

- [ ] 8.1 Verify ~800-900 places returned (population 65k+ threshold)
- [ ] 8.2 Spot check: Los Angeles, New York, Chicago present
- [ ] 8.3 Verify no duplicate place entries
- [ ] 8.4 Check missing value percentage per column
- [ ] 8.5 **STOP**: Get user feedback on single year output before proceeding

### 9. Implement Multi-Year Collection

- [ ] 9.1 Write `collect_all_years(start: int, end: int) -> pd.DataFrame`
- [ ] 9.2 Loop through years from start to end
- [ ] 9.3 Call `collect_year(year)` for each
- [ ] 9.4 Concatenate all year DataFrames
- [ ] 9.5 Add per-year checkpoint: save after each year completes

### 10. Run Full Historical Pull (2009-2024)

- [ ] 10.1 Run `collect_all_years(2009, 2024)`
- [ ] 10.2 Create `data/acs_processed/` directory
- [ ] 10.3 Save to `data/acs_processed/acs_places_2009_2024.parquet`
- [ ] 10.4 Log any years/groups that failed or had issues

### 11. Final Quality Checks

- [ ] 11.1 Count unique places per year (check for balanced panel)
- [ ] 11.2 List places with gaps (unbalanced panel entries)
- [ ] 11.3 Document any variables missing in certain years
- [ ] 11.4 Generate final summary statistics

### 12. Cleanup

- [ ] 12.1 Remove checkpoint files from `tmp/`
- [ ] 12.2 Update `./SUMMARY.md` with actual results
- [ ] 12.3 Mark PRD status as complete in Notion

---

## Quick Reference

**Test command (single fetch)**:
```bash
uv run python -c "from src.acs_pull.pull import fetch_group; print(fetch_group(2023, 'B01001', '06').head())"
```

**Run single year**:
```bash
uv run python -c "from src.acs_pull.pull import collect_year; collect_year(2023)"
```

**Run full pull**:
```bash
uv run python -c "from src.acs_pull.pull import collect_all_years; collect_all_years(2009, 2024)"
```

---

## State FIPS Codes (52 total)

```
01=AL, 02=AK, 04=AZ, 05=AR, 06=CA, 08=CO, 09=CT, 10=DE, 11=DC, 12=FL,
13=GA, 15=HI, 16=ID, 17=IL, 18=IN, 19=IA, 20=KS, 21=KY, 22=LA, 23=ME,
24=MD, 25=MA, 26=MI, 27=MN, 28=MS, 29=MO, 30=MT, 31=NE, 32=NV, 33=NH,
34=NJ, 35=NM, 36=NY, 37=NC, 38=ND, 39=OH, 40=OK, 41=OR, 42=PA, 44=RI,
45=SC, 46=SD, 47=TN, 48=TX, 49=UT, 50=VT, 51=VA, 53=WA, 54=WV, 55=WI,
56=WY, 72=PR
```

---

## ACS Groups (from PRD)

```
B00001, B00002, B01001, B01003, B02003, B05001, B07001, B07409, B08101,
B08135, B08303, B09001, B09010, B14007, B15012, B17026, B19051, B19052,
B19053, B19054, B19055, B19061, B19062, B19063, B19064, B19065, B19081,
B19083, B19101, B19301, B23018, B23020, B24031, B25001, B25017, B25018,
B25019, B25034, B25035, B25056, B25057, B25058, B25059, B25060, B25061,
B25062, B25063, B25064, B25065, B25066, B25067, B25068, B25070, B25071,
B25074, B25075, B25076, B25077, B25078, B25079, B25080, B25081, B25082,
B25083, B25085, B25086, B25104, B25105, B27001, B27004
```
