# Weekly Returns Analysis

This folder contains a Jupyter notebook (`sql_test.ipynb`) that performs weekly returns analysis using DuckDB.

## Required Files

For the `sql_test.ipynb` notebook to work properly, you need to have the following CSV files in this directory:

### 1. `data.csv`
Contains fund performance data with the following columns:
- `FUND_CODE`: Identifier for the fund (e.g., FUND_01)
- `MARKET_DATE`: Date in YYYY-MM-DD format
- `RETUNR_TYPE`: Type of return (e.g., TYPE_A)
- `DAILY_RETURN`: Daily return percentage (can be empty for non-trading days)

### 2. `calendar.csv`
Contains calendar information with, at least, the following columns:
- `DATE`: Date in YYYY-MM-DD format (spans from 1900 to 2299)
- `DAY_OF_WEEK`: Numeric day of week (1-7)
- `IS_HOLIDAY`: Boolean indicating if the date is a holiday
- `IS_WORKING_DAY`: Boolean indicating if the date is a working day

## Setup

Make sure you have the required dependencies installed:
```bash
pip install duckdb pandas
```

Then place both `data.csv` and `calendar.csv` files in this directory before running the notebook.
