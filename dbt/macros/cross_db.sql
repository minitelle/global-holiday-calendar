-- macros/cross_db.sql
-- Cross-database helpers for BigQuery and Snowflake compatibility.
-- These complement dbt-utils and dbt-core cross-db macros.

{% macro day_name(column) %}
    {%- if target.type == 'bigquery' -%}
        FORMAT_DATE('%A', {{ column }})
    {%- elif target.type == 'snowflake' -%}
        -- Snowflake DAYNAME returns 3-char abbreviation; expand to full name.
        CASE EXTRACT(DAYOFWEEK FROM {{ column }})
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END
    {%- else -%}
        CASE EXTRACT(DOW FROM {{ column }})
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END
    {%- endif %}
{% endmacro %}


{% macro iso_day_of_week(column) %}
    -- Returns 1=Monday, 7=Sunday (ISO 8601)
    {%- if target.type == 'bigquery' -%}
        -- BQ DAYOFWEEK is 1=Sun, 7=Sat; shift to ISO
        MOD(EXTRACT(DAYOFWEEK FROM {{ column }}) + 5, 7) + 1
    {%- elif target.type == 'snowflake' -%}
        EXTRACT(DAYOFWEEKISO FROM {{ column }})
    {%- else -%}
        -- Generic ISO fallback (PostgreSQL-style)
        EXTRACT(ISODOW FROM {{ column }})
    {%- endif %}
{% endmacro %}


{% macro is_weekend(column) %}
    -- True for Saturday and Sunday
    {%- if target.type == 'bigquery' -%}
        EXTRACT(DAYOFWEEK FROM {{ column }}) IN (1, 7)
    {%- elif target.type == 'snowflake' -%}
        DAYOFWEEK({{ column }}) IN (0, 6)
    {%- else -%}
        EXTRACT(ISODOW FROM {{ column }}) IN (6, 7)
    {%- endif %}
{% endmacro %}


{% macro datediff_weeks(start_col, end_col) %}
    -- Number of full weeks between two dates
    {%- if target.type == 'bigquery' -%}
        DATE_DIFF({{ end_col }}, {{ start_col }}, WEEK)
    {%- elif target.type == 'snowflake' -%}
        DATEDIFF('week', {{ start_col }}, {{ end_col }})
    {%- else -%}
        ({{ end_col }} - {{ start_col }}) / 7
    {%- endif %}
{% endmacro %}


{% macro au_fiscal_year_start(date_col) %}
    -- Returns the first day of the AU fiscal year for a given date
    {%- if target.type == 'bigquery' -%}
        CASE
            WHEN EXTRACT(MONTH FROM {{ date_col }}) >= 7
            THEN DATE(EXTRACT(YEAR FROM {{ date_col }}),     7, 1)
            ELSE DATE(EXTRACT(YEAR FROM {{ date_col }}) - 1, 7, 1)
        END
    {%- elif target.type == 'snowflake' -%}
        CASE
            WHEN EXTRACT(MONTH FROM {{ date_col }}) >= 7
            THEN DATE_FROM_PARTS(EXTRACT(YEAR FROM {{ date_col }}),     7, 1)
            ELSE DATE_FROM_PARTS(EXTRACT(YEAR FROM {{ date_col }}) - 1, 7, 1)
        END
    {%- else -%}
        CASE
            WHEN EXTRACT(MONTH FROM {{ date_col }}) >= 7
            THEN MAKE_DATE(EXTRACT(YEAR FROM {{ date_col }})::int,     7, 1)
            ELSE MAKE_DATE(EXTRACT(YEAR FROM {{ date_col }})::int - 1, 7, 1)
        END
    {%- endif %}
{% endmacro %}
