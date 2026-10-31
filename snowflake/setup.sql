-- snowflake/setup.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Snowflake setup for global-holiday-calendar holiday calendar.
-- Run once per environment (dev / prod). Idempotent — uses CREATE OR REPLACE.
--
-- Prerequisites:
--   Parquet files generated: cd scripts && python generate_holidays.py
--   Files uploaded to your internal stage or S3/GCS external stage.
--
-- Adjust: database, schema, stage, and warehouse names to match your env.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 0. Context ────────────────────────────────────────────────────────────────

USE ROLE    SYSADMIN;               -- or your DE role
USE WAREHOUSE TRANSFORMING;         -- adjust to your transform WH
USE DATABASE  ANALYTICS;            -- adjust
USE SCHEMA    REF_DATA_RAW;         -- will be created below


-- ── 1. Create schema ──────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS REF_DATA_RAW
    DATA_RETENTION_TIME_IN_DAYS = 1
    COMMENT = 'Global holiday calendar — holiday + commerce calendar';


-- ── 2. File format ────────────────────────────────────────────────────────────

CREATE OR REPLACE FILE FORMAT REF_DATA_RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE
    BINARY_AS_TEXT = FALSE;


-- ── 3. Stage (internal named stage) ──────────────────────────────────────────
-- If you use an S3 or GCS external stage, replace with your stage definition.

CREATE OR REPLACE STAGE REF_DATA_RAW.HOLIDAYS_STAGE
    FILE_FORMAT = REF_DATA_RAW.PARQUET_FORMAT
    COMMENT = 'Staging for holiday calendar Parquet files';

-- Upload from your local machine:
--   snowsql -q "PUT file:///path/to/data/holidays/year=*/data.parquet @REF_DATA_RAW.HOLIDAYS_STAGE AUTO_COMPRESS=FALSE"
-- Or from CI/CD using SnowCLI / Snowflake connector.


-- ── 4. Target table ──────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE REF_DATA_RAW.HOLIDAYS (
    CALENDAR_DATE        DATE          NOT NULL,
    COUNTRY_CODE         VARCHAR(5)    NOT NULL,
    HOLIDAY_NAME         VARCHAR(200)  NOT NULL,
    HOLIDAY_CATEGORY     VARCHAR(50)   NOT NULL,    -- public_holiday | commercial | observance
    COMMERCIAL_SEASON    VARCHAR(100),              -- nullable
    IS_PUBLIC_HOLIDAY    BOOLEAN       NOT NULL,
    IS_COMMERCIAL_DATE   BOOLEAN       NOT NULL,
    YEAR                 INTEGER       NOT NULL,
    MONTH                INTEGER       NOT NULL,
    DAY_OF_MONTH         INTEGER       NOT NULL,
    DAY_OF_WEEK_ISO      INTEGER       NOT NULL,    -- 1=Monday, 7=Sunday
    DAY_NAME             VARCHAR(20)   NOT NULL,
    IS_WEEKEND           BOOLEAN       NOT NULL,
    WEEK_OF_YEAR_ISO     INTEGER       NOT NULL,
    CALENDAR_QUARTER     INTEGER       NOT NULL,
    FISCAL_YEAR_AU       VARCHAR(10)   NOT NULL,    -- e.g. 'FY27'
    FISCAL_QUARTER_AU    INTEGER       NOT NULL,
    FISCAL_MONTH_AU      INTEGER       NOT NULL,
    FISCAL_WEEK_AU       INTEGER       NOT NULL,
    IS_CORE_MARKET       BOOLEAN       NOT NULL
)
CLUSTER BY (CALENDAR_DATE, COUNTRY_CODE)
COMMENT = 'Holiday + commerce calendar. Source: global-holiday-calendar scripts/generate_holidays.py.';


-- ── 5. Load from stage ────────────────────────────────────────────────────────

COPY INTO REF_DATA_RAW.HOLIDAYS
FROM (
    SELECT
        TO_DATE($1:calendar_date::VARCHAR),
        $1:country_code::VARCHAR,
        $1:holiday_name::VARCHAR,
        $1:holiday_category::VARCHAR,
        $1:commercial_season::VARCHAR,
        $1:is_public_holiday::BOOLEAN,
        $1:is_commercial_date::BOOLEAN,
        $1:year::INTEGER,
        $1:month::INTEGER,
        $1:day_of_month::INTEGER,
        $1:day_of_week_iso::INTEGER,
        $1:day_name::VARCHAR,
        $1:is_weekend::BOOLEAN,
        $1:week_of_year_iso::INTEGER,
        $1:calendar_quarter::INTEGER,
        $1:fiscal_year_au::VARCHAR,
        $1:fiscal_quarter_au::INTEGER,
        $1:fiscal_month_au::INTEGER,
        $1:fiscal_week_au::INTEGER,
        $1:is_core_market::BOOLEAN
    FROM @REF_DATA_RAW.HOLIDAYS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'REF_DATA_RAW.PARQUET_FORMAT')
PATTERN = '.*data.parquet'
ON_ERROR = 'ABORT_STATEMENT'
PURGE = FALSE;


-- ── 6. Verify ────────────────────────────────────────────────────────────────

SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT country_code)    AS countries,
    MIN(calendar_date)              AS min_date,
    MAX(calendar_date)              AS max_date,
    COUNT(DISTINCT fiscal_year_au)  AS fiscal_years
FROM REF_DATA_RAW.HOLIDAYS;


-- ── 7. Grant read access to dbt transform role ────────────────────────────────

GRANT USAGE  ON SCHEMA REF_DATA_RAW                   TO ROLE TRANSFORMER;
GRANT SELECT ON TABLE  REF_DATA_RAW.HOLIDAYS           TO ROLE TRANSFORMER;
GRANT USAGE  ON STAGE  REF_DATA_RAW.HOLIDAYS_STAGE     TO ROLE TRANSFORMER;

-- Done. Set dbt var 'ref_data_raw_schema' to 'REF_DATA_RAW' and run dbt build.
