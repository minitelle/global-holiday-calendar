# global-holiday-calendar

Worldwide public holidays and marketplace-relevant commercial dates as Parquet files.
Built for BigQuery and Snowflake. Includes AU fiscal year labelling.

**[Live demo →](https://claude.ai/artifact/6hzCG8Z5FdLpB9296dFRU3)**

---

## What is in the dataset

- Public holidays for 25 core markets via the `holidays` Python library
- Commercial dates with country-level variation: Mother's Day, Father's Day,
  Black Friday, Easter, Halloween, Valentine's Day, Pride, and more
- AU fiscal year labels on every row (FY27, Q1-Q4, fiscal week)

## Quick start

```bash
# 1. Install
pip3 install -r scripts/requirements.txt

# 2. Generate Parquet files (2019–2031, core markets)
python3 scripts/generate_holidays.py \
  --start 2019 --end 2031 \
  --core-only \
  --output data/holidays

# 3. Run the DuckDB demo
python3 duckdb_demo.py

# 4. Open the dashboard
open demo/dashboard.html
```

## Load into BigQuery

```bash
export PROJECT=your-gcp-project
export BUCKET=your-gcs-bucket
bash bq/setup.sh
```

## Load into Snowflake

```sql
-- See snowflake/setup.sql
```

## Use in dbt

```yaml
# packages.yml
packages:
  - git: "https://github.com/YOUR-USERNAME/global-holiday-calendar.git"
    subdirectory: "dbt"
    revision: "main"
```

## Column reference

| Column | Description |
|---|---|
| `calendar_date` | The date |
| `country_code` | ISO 3166-1 alpha-2 |
| `holiday_name` | Name of the holiday or commercial event |
| `holiday_category` | `public_holiday` or `commercial` |
| `commercial_season` | e.g. `christmas`, `mothers_day`, `black_friday_cm` |
| `is_public_holiday` | Boolean |
| `is_commercial_date` | Boolean |
| `fiscal_year_au` | e.g. `FY27` |
| `fiscal_quarter_au` | 1–4, Q1=Jul-Sep |
| `fiscal_month_au` | 1–12, Month 1=July |
| `fiscal_week_au` | Week of AU fiscal year |
