-- models/ref_holidays.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Cleaned, typed view of the raw holidays table loaded via generate_holidays.py.
-- Source: ref_data_raw.holidays (BigQuery) or REF_DATA_RAW.HOLIDAYS (Snowflake)
--
-- Run this after loading the Parquet files with the setup scripts in:
--   bq/setup.sh  (BigQuery)
--   snowflake/setup.sql  (Snowflake)
--
-- Grain: one row per (calendar_date, country_code, holiday_name, holiday_category)
-- A single date can have multiple rows per country (e.g. Good Friday appears
-- as both public_holiday and commercial/easter on the same day).
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['reference', 'calendar', 'holidays']
    )
}}

with raw as (
    select *
    from {{ source('ref_data_raw', 'holidays') }}
),

cleaned as (
    select
        cast(calendar_date     as date)    as calendar_date,
        upper(trim(country_code))          as country_code,
        trim(holiday_name)                 as holiday_name,
        lower(trim(holiday_category))      as holiday_category,
        lower(trim(commercial_season))     as commercial_season,
        cast(is_public_holiday  as boolean) as is_public_holiday,
        cast(is_commercial_date as boolean) as is_commercial_date,
        cast(is_core_market     as boolean) as is_core_market,

        -- Calendar attributes (redundant with ref_calendar_dates but kept for
        -- standalone use: analysts can query this table without a JOIN)
        cast(year                as integer) as year,
        cast(month               as integer) as month,
        cast(day_of_month        as integer) as day_of_month,
        cast(day_of_week_iso     as integer) as day_of_week_iso,
        trim(day_name)                       as day_name,
        cast(is_weekend          as boolean) as is_weekend,
        cast(week_of_year_iso    as integer) as week_of_year_iso,
        cast(calendar_quarter    as integer) as calendar_quarter,
        trim(fiscal_year_au)                 as fiscal_year_au,
        cast(fiscal_quarter_au   as integer) as fiscal_quarter_au,
        cast(fiscal_month_au     as integer) as fiscal_month_au,
        cast(fiscal_week_au      as integer) as fiscal_week_au

    from raw
    where calendar_date is not null
      and country_code  is not null
      and holiday_name  is not null
)

select * from cleaned
order by calendar_date, country_code, holiday_category
