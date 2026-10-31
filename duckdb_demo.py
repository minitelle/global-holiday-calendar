#!/usr/bin/env python3
"""
DuckDB Commercial Calendar Demo
global-holiday-calendar

Shows how DuckDB reads Parquet files directly — no BigQuery, no Snowflake,
no credentials, no loading step. Runs entirely on your laptop.

Usage:
    pip install duckdb pandas
    python duckdb_demo.py
"""

import duckdb

conn = duckdb.connect()  # in-memory, no file needed

PARQUET = "data/holidays/year=*/data.parquet"


# ── 1. Basic query: AU public holidays in FY27 ───────────────────────────────
print("\n── AU Public Holidays · FY27 ────────────────────────────────────────")
df = conn.execute(f"""
    SELECT
        calendar_date,
        holiday_name,
        day_name,
        fiscal_quarter_au  AS fq
    FROM read_parquet('{PARQUET}', hive_partitioning = true)
    WHERE country_code     = 'AU'
      AND fiscal_year_au   = 'FY27'
      AND is_public_holiday
    ORDER BY calendar_date
""").df()
print(df.to_string(index=False))


# ── 2. Commercial season peaks across all core markets ───────────────────────
print("\n── Commercial Season Peaks · FY27 · Core Markets ───────────────────")
df = conn.execute(f"""
    SELECT
        commercial_season,
        country_code,
        calendar_date,
        holiday_name,
        day_name
    FROM read_parquet('{PARQUET}', hive_partitioning = true)
    WHERE fiscal_year_au   = 'FY27'
      AND is_commercial_date
      AND commercial_season IN ('christmas','black_friday_cm','mothers_day','easter')
      AND country_code     IN ('AU','US','GB')
    ORDER BY commercial_season, calendar_date, country_code
""").df()
print(df.to_string(index=False))


# ── 3. How Mother's Day varies by country ────────────────────────────────────
print("\n── Mother's Day Dates · 2025–2027 · All Countries ──────────────────")
df = conn.execute(f"""
    SELECT
        year,
        country_code,
        calendar_date,
        holiday_name,
        day_name
    FROM read_parquet('{PARQUET}', hive_partitioning = true)
    WHERE commercial_season = 'mothers_day'
      AND year BETWEEN 2025 AND 2027
    ORDER BY year, calendar_date, country_code
""").df()
print(df.to_string(index=False))


# ── 4. AU fiscal quarter breakdown ──────────────────────────────────────────
print("\n── AU FY27: Events by Fiscal Quarter ───────────────────────────────")
df = conn.execute(f"""
    SELECT
        fiscal_quarter_au                            AS quarter,
        COUNT(*) FILTER (WHERE is_public_holiday)    AS public_holidays,
        COUNT(*) FILTER (WHERE is_commercial_date)   AS commercial_dates,
        STRING_AGG(DISTINCT commercial_season, ', ')
            FILTER (WHERE is_commercial_date)        AS seasons
    FROM read_parquet('{PARQUET}', hive_partitioning = true)
    WHERE country_code   = 'AU'
      AND fiscal_year_au = 'FY27'
    GROUP BY 1
    ORDER BY 1
""").df()
print(df.to_string(index=False))


# ── 5. Export for another tool (e.g. Sigma, Excel, Tableau) ─────────────────
print("\n── Export: AU FY27 notable dates → CSV ─────────────────────────────")
conn.execute(f"""
    COPY (
        SELECT
            calendar_date,
            country_code,
            holiday_name,
            holiday_category,
            commercial_season,
            fiscal_year_au,
            fiscal_quarter_au,
            fiscal_week_au,
            day_name,
            is_weekend
        FROM read_parquet('{PARQUET}', hive_partitioning = true)
        WHERE country_code   = 'AU'
          AND fiscal_year_au = 'FY27'
          AND (is_public_holiday OR is_commercial_date)
        ORDER BY calendar_date
    ) TO 'au_fy27_notable_dates.csv' (HEADER, DELIMITER ',')
""")
print("Written: au_fy27_notable_dates.csv")
print("\nTo open in Python: import pandas as pd; df = pd.read_csv('au_fy27_notable_dates.csv')")
print("To query in DuckDB: SELECT * FROM 'au_fy27_notable_dates.csv'")
