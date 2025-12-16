# ACS Data Pull v2 - Task Summary

## Overview
Build a data pipeline to pull American Community Survey (ACS) data from the Census Bureau API at the **place (city) level** for **years 2009-2024** to create a forecasting-ready dataset.

---

## Key Changes from v1

| Aspect | v1 (Previous) | v2 (New) |
|--------|---------------|----------|
| Geography | 9 specific CBSAs | All places (cities) |
| Years | 2020-2023 | 2009-2024 |
| Output | Raw grouped data | Long-format panel for forecasting |

---

## Data Scope

**Years**: 2009-2024 (16 years)

**Geography**: "Place" level (Census terminology for cities/towns)
- ACS 1-year estimates: Only places with population >=65,000 (~800-900 places)
- Smaller places require ACS 5-year estimates (different API endpoint)

**Variables**: 60+ ACS groups from the collected list, including:

| Category | Groups | Description |
|----------|--------|-------------|
| **Demographics** | B01001, B01003, B02003 | Sex by age, total population, race |
| **Mobility** | B07001, B07409 | Geographic mobility by age/education |
| **Commuting** | B08101, B08135, B08303 | Transportation to work, travel time |
| **Family** | B09001, B09010 | Population under 18, public assistance |
| **Education** | B14007, B15012 | School enrollment, bachelor's degrees |
| **Poverty** | B17026 | Income to poverty ratio |
| **Income** | B19051-B19065, B19081, B19083, B19101, B19301 | Earnings, Gini index, income quintiles |
| **Employment** | B23018, B23020, B24031 | Hours worked, industry earnings |
| **Housing** | B25001, B25017-B25019, B25034-B25035 | Units, rooms, year built |
| **Rent** | B25056-B25071, B25074 | Contract/gross rent, rent burden |
| **Home Value** | B25075-B25086, B25104-B25105 | Property values, housing costs |
| **Healthcare** | B27001, B27004 | Health insurance coverage |
| **Citizenship** | B05001 | Citizenship status |

---

## Output Format (Forecasting-Ready)

**Structure**: Long-format panel dataset (standard for time-series/panel models)
- **Rows**: One per place-year combination
- **Columns**: place_fips, place_name, state, year, + all variables

```
place_fips | place_name  | state | year | B19013_001E | B25077_001E | ...
-----------|-------------|-------|------|-------------|-------------|----
0644000    | Los Angeles | CA    | 2009 | 54385       | 425000      | ...
0644000    | Los Angeles | CA    | 2010 | 55476       | 435000      | ...
0644000    | Los Angeles | CA    | 2011 | 56123       | 440000      | ...
```

This format enables:
- Time-series forecasting (ARIMA, Prophet, LSTM)
- Panel regression models (fixed effects, random effects)
- Easy filtering/grouping by place or year
- ML feature engineering with temporal lags

**Note**: Can easily pivot to wide format if needed for specific analyses.

---

## API Details

**Base URL**: `https://api.census.gov/data/{year}/acs/acs1`

**Query Format**:
```
?get=group({group})&for=place:*&in=state:{state_fips}&key={api_key}
```

**Configuration**:
- API key available in `.env` file
- With API key: Higher rate limits than default 500/day

**API Constraints** (per [Census API documentation](https://www.census.gov/data/developers/guidance/api-user-guide.Example_API_Queries.html)):
- **Cannot query all places nationwide in one call** - wildcard `*` not valid for `in` parameter
- Must loop through states: `for=place:*&in=state:XX` for each state
- Single group per call confirmed; multiple groups (`get=group(A),group(B)`) needs testing
- Use `descriptive=true` for human-readable headers

---

## Technical Considerations

### API Call Volume
- 50 states x 60+ groups x 16 years = **~48,000 API calls**
- With API key, rate limits are generous but calls should be throttled

### Data Consistency Notes
Per [IPUMS ACS documentation](https://usa.ipums.org/usa/acs.shtml):
- **2009 and earlier**: Population controls based on Census 2000
- **2010 and later**: Population controls based on Census 2010
- This creates a methodological break in the series
- Core variables (demographics, income, housing) remain consistent in definition
- Some variables added over time:
  - Health insurance questions: Added 2008
  - Bachelor's degree field: Added 2009

### Error Handling Needs
- API timeouts and retries
- Missing data for some place-year-variable combinations
- Checkpointing for restartability on large pulls

---

## Initial Deliverable (Per PRD)

> "To start, implement the logic to make the pull and save out 1 year's worth of data, so that I can inspect and provide feedback."

Phase 1 = working prototype for a single year before scaling to full 16-year range.

---

## Source References

**Notion PRD Documents**:
- PRD: https://www.notion.so/2ca7a36db9e880d2a5ecf873eb37e6ff
- Previous Implementation: https://www.notion.so/25d7a36db9e880b6aa3ae077b17526cd
- Variable Groups List: https://www.notion.so/2c57a36db9e880e4ae11f155e22e6049
- Implementation Strategy (v1): https://www.notion.so/25d7a36db9e881718f78c08388f06a62

**Census Bureau Documentation**:
- [Census API User Guide](https://www.census.gov/data/developers/guidance/api-user-guide.Example_API_Queries.html)
- [Groups Functionality](https://www.census.gov/data/developers/updates/groups-functionality.html)
- [ACS 1-Year Data API](https://www.census.gov/data/developers/data-sets/acs-1year.html)
- [ACS API Handbook (PDF)](https://www.census.gov/content/dam/Census/library/publications/2020/acs/acs_api_handbook_2020_ch02.pdf)

**Data Quality References**:
- [IPUMS ACS Documentation](https://usa.ipums.org/usa/acs.shtml)
