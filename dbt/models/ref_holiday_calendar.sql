-- models/ref_holiday_calendar.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- THE PRIMARY ANALYST-FACING TABLE.
-- One row per (calendar_date, country_code).
-- Flattens all holidays for a date into boolean flags and string columns.
-- Join this to any fact table on calendar_date + country_code to add
-- holiday context to any dashboard or analysis.
--
-- Example usage (Sigma / BigQuery / Snowflake):
--
--   SELECT
--     o.order_date,
--     o.revenue,
--     h.is_public_holiday,
--     h.is_commercial_date,
--     h.commercial_season,
--     h.holiday_names
--   FROM orders o
--   LEFT JOIN ref_holiday_calendar h
--     ON o.order_date    = h.calendar_date
--    AND o.country_code  = h.country_code
--
-- Grain: (calendar_date, country_code) — unique.
-- ─────────────────────────────────────────────────────────────────────────────

{{
    config(
        materialized = 'table',
        tags         = ['reference', 'calendar', 'holidays', 'analyst_facing']
    )
}}

with holiday_agg as (
    select
        calendar_date,
        country_code,

        max(cast(is_public_holiday  as integer)) = 1   as is_public_holiday,
        max(cast(is_commercial_date as integer)) = 1   as is_commercial_date,

        -- Aggregate holiday names — cross-db
        {% if target.type == 'bigquery' %}
        string_agg(
            case when is_public_holiday then holiday_name end,
            '; ' order by holiday_name
        )                                               as public_holiday_names,
        string_agg(
            case when is_commercial_date then holiday_name end,
            '; ' order by holiday_name
        )                                               as commercial_date_names,
        string_agg(
            distinct commercial_season,
            '; ' order by commercial_season
        )                                               as commercial_seasons,
        {% elif target.type == 'snowflake' %}
        listagg(
            case when is_public_holiday then holiday_name end,
            '; '
        ) within group (order by holiday_name)          as public_holiday_names,
        listagg(
            case when is_commercial_date then holiday_name end,
            '; '
        ) within group (order by holiday_name)          as commercial_date_names,
        listagg(
            distinct commercial_season,
            '; '
        ) within group (order by commercial_season)     as commercial_seasons,
        {% else %}
        string_agg(case when is_public_holiday then holiday_name end, '; ')
                                                        as public_holiday_names,
        string_agg(case when is_commercial_date then holiday_name end, '; ')
                                                        as commercial_date_names,
        string_agg(distinct commercial_season, '; ')    as commercial_seasons,
        {% endif %}

        max(cast(is_core_market as integer)) = 1        as is_core_market

    from {{ ref('ref_holidays') }}
    group by calendar_date, country_code
),

country_list as (
    select distinct country_code, is_core_market
    from {{ ref('ref_holidays') }}
),

date_country_spine as (
    select
        d.calendar_date,
        c.country_code,
        c.is_core_market
    from {{ ref('ref_calendar_dates') }} d
    cross join country_list c
),

final as (
    select
        s.calendar_date,
        s.country_code,
        s.is_core_market,

        coalesce(h.is_public_holiday,   false)          as is_public_holiday,
        coalesce(h.is_commercial_date,  false)          as is_commercial_date,

        coalesce(h.is_public_holiday, false)
        or coalesce(h.is_commercial_date, false)
        or d.is_weekend                                 as is_notable_date,

        nullif(trim(h.public_holiday_names),  '')       as public_holiday_names,
        nullif(trim(h.commercial_date_names), '')       as commercial_date_names,
        nullif(trim(h.commercial_seasons),    '')       as commercial_seasons,

        -- First season in sorted list — cross-db safe
        {% if target.type == 'bigquery' %}
        split(coalesce(nullif(trim(h.commercial_seasons), ''), ''), '; ')[safe_ordinal(1)]
        {% else %}
        split_part(coalesce(nullif(trim(h.commercial_seasons), ''), ''), '; ', 1)
        {% endif %}                                     as primary_commercial_season,

        d.year,
        d.month,
        d.day_of_month,
        d.day_of_week_iso,
        d.day_name,
        d.is_weekend,
        d.week_of_year_iso,
        d.calendar_quarter,
        d.fiscal_year_au,
        d.fiscal_year_int_au,
        d.fiscal_quarter_au,
        d.fiscal_month_au,
        d.fiscal_week_au,
        d.fiscal_year_start_au

    from date_country_spine s
    left join {{ ref('ref_calendar_dates') }} d
        on s.calendar_date = d.calendar_date
    left join holiday_agg h
        on s.calendar_date  = h.calendar_date
       and s.country_code   = h.country_code
)

select * from final
order by calendar_date, country_code
