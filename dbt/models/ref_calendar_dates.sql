-- models/ref_calendar_dates.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Full date spine with calendar and AU fiscal year attributes.
-- No external data dependencies — generated entirely in SQL.
-- Works on BigQuery and Snowflake via dbt-utils date_spine macro.
--
-- AU Fiscal Year convention (July to June):
--   FY27 = July 1 2026 to June 30 2027
--   Q1 = Jul-Sep | Q2 = Oct-Dec | Q3 = Jan-Mar | Q4 = Apr-Jun
--   Fiscal month 1 = July, Fiscal month 12 = June
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['reference', 'calendar', 'date_spine']
    )
}}

with spine as (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('" ~ var('ref_data_start_date', '2019-01-01') ~ "' as date)",
        end_date   = "cast('" ~ var('ref_data_end_date',   '2032-01-01') ~ "' as date)"
    ) }}
),

enriched as (
    select
        cast(date_day as date)                                      as calendar_date,

        -- ── Standard calendar attributes ──────────────────────────────────────
        extract(year  from date_day)                                as year,
        extract(month from date_day)                                as month,
        extract(day   from date_day)                                as day_of_month,

        -- ISO day of week: 1=Monday, 7=Sunday
        {{ iso_day_of_week('date_day') }}                           as day_of_week_iso,
        {{ day_name('date_day') }}                                  as day_name,
        {{ is_weekend('date_day') }}                                as is_weekend,

        -- ISO week number (week containing Thursday belongs to that year)
        -- Fix: BigQuery uses ISOWEEK, Snowflake uses WEEKISO
        {% if target.type == 'bigquery' %}
        extract(isoweek from date_day)
        {% elif target.type == 'snowflake' %}
        extract(weekiso from date_day)
        {% else %}
        extract(week from date_day)
        {% endif %}                                                 as week_of_year_iso,

        -- Calendar quarter: integer division required (BQ / is float)
        {% if target.type == 'bigquery' %}
        div(cast(extract(month from date_day) as int64) - 1, 3) + 1
        {% else %}
        floor((extract(month from date_day) - 1) / 3) + 1
        {% endif %}                                                 as calendar_quarter,

        -- ── AU Fiscal Year attributes ─────────────────────────────────────────
        case
            when extract(month from date_day) >= 7
            then concat(
                'FY',
                right(cast(extract(year from date_day) + 1 as {{ dbt.type_string() }}), 2)
            )
            else concat(
                'FY',
                right(cast(extract(year from date_day) as {{ dbt.type_string() }}), 2)
            )
        end                                                         as fiscal_year_au,

        case
            when extract(month from date_day) >= 7
            then cast(extract(year from date_day) + 1 as integer)
            else cast(extract(year from date_day)     as integer)
        end                                                         as fiscal_year_int_au,

        case
            when extract(month from date_day) in (7, 8, 9)    then 1
            when extract(month from date_day) in (10, 11, 12)  then 2
            when extract(month from date_day) in (1, 2, 3)     then 3
            else 4
        end                                                         as fiscal_quarter_au,

        mod(cast(extract(month from date_day) as integer) - 7 + 12, 12) + 1
                                                                    as fiscal_month_au,

        {{ datediff_weeks(au_fiscal_year_start('date_day'), 'date_day') }} + 1
                                                                    as fiscal_week_au,

        {{ au_fiscal_year_start('date_day') }}                      as fiscal_year_start_au

    from spine
)

select * from enriched
order by calendar_date
