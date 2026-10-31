# global-holiday-calendar

Worldwide public holidays, commercial and marketing dates, sports and elections, and religious and cultural observances for 2025 to 2028, as Parquet files. Built for DuckDB, BigQuery, Snowflake and dbt, with Australian (July to June) fiscal year labels on every row.

[**Live demo →**](https://minitelle.github.io/global-holiday-calendar/demo/dashboard.html)

---

## What is in the dataset

- **Public holidays** for 204 countries via the [`holidays`](https://github.com/vacanza/holidays) library, including China (English names) and every Australian state and territory: half-day holidays (Christmas Eve and New Year's Eve from 7pm in NT and SA), bank holidays, and Tasmania's regional holidays.
- **Commercial dates** with country-level variation: Mother's Day, Father's Day, Black Friday, Cyber Monday, Easter, Halloween, Valentine's Day, Back to School and more.
- **Pride:** June Pride Month, Trans Day of Visibility, IDAHOBIT, and Australia's own season with the organisers' published dates for Midsumma (Melbourne) and Sydney Gay and Lesbian Mardi Gras, plus Wear It Purple Day.
- **US marketing moments** from Printful's eCommerce holiday calendar (Galentine's Day, National Sticker Day, Small Business Saturday and more).
- **Sports:** Super Bowl Sunday and March Madness (Selection Sunday, Final Four, National Championship).
- **Elections:** US election days (2026 midterms, 2028 presidential) and Australian federal, state and territory elections.
- **Religious and cultural observances** for every country: Christian, Orthodox Christian, Jewish, Islamic, Sikh, Bahá'í, Hindu, Buddhist, Chinese and Persian.

## Quick start

Requires Python 3.10 or later.

```bash
# 1. Create an environment and install
python3 -m venv .venv
source .venv/bin/activate        # fish: source .venv/bin/activate.fish
pip install -r scripts/requirements.txt

# 2. Generate the Parquet files (2025 to 2028, all countries)
cd scripts && python generate_holidays.py && cd ..
#    Faster, core markets only:
#    cd scripts && python generate_holidays.py --core-only && cd ..

# 3. Run the DuckDB demo
python duckdb_demo.py

# 4. Open the dashboard
open demo/dashboard.html
```

Files are written to `data/holidays/year=YYYY/data.parquet` and are not committed to the repo; generate them locally.

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
  - git: "https://github.com/minitelle/global-holiday-calendar.git"
    subdirectory: "dbt"
    revision: "main"
```

## Column reference

| Column | Description |
|---|---|
| `calendar_date` | The date |
| `country_code` | ISO 3166-1 alpha-2 |
| `subdivision_code` | State or territory for regional holidays and elections (e.g. `VIC`), otherwise null |
| `holiday_name` | Name of the holiday, event or observance |
| `holiday_category` | `public_holiday`, `commercial` or `observance` (religious and cultural) |
| `commercial_season` | e.g. `christmas`, `mothers_day`, `black_friday_cm`, `pride`, `sports`, `elections`, `marketing` |
| `tradition` | Observances only: `christian`, `christian_orthodox`, `jewish`, `islamic`, `sikh`, `bahai`, `hindu`, `buddhist`, `chinese`, `persian` |
| `is_public_holiday` | Boolean |
| `is_commercial_date` | Boolean |
| `is_estimated` | True for dates that depend on moon sighting or are not yet announced |
| `is_core_market` | Boolean |
| `year`, `month`, `day_of_month`, `day_of_week_iso`, `day_name`, `is_weekend`, `week_of_year_iso`, `calendar_quarter` | Calendar attributes |
| `fiscal_year_au` | e.g. `FY27` (July 2026 to June 2027) |
| `fiscal_quarter_au` | 1 to 4, Q1 = Jul to Sep |
| `fiscal_month_au` | 1 to 12, month 1 = July |
| `fiscal_week_au` | Week of the AU fiscal year, week 1 starts 1 July |

## Notes on dates

- **Jewish, Islamic and Bahá'í days** begin at sundown the evening before the date shown.
- **Islamic dates** follow the Umm al-Qura calendar and can differ by a day from local moon sighting (for example, some countries mark Eid al-Fitr 2027 on 10 March rather than 9 March). They are flagged `is_estimated`.
- **Bahá'í Naw-Rúz and Twin Holy Days** use the Bahá'í World Centre's published dates, so Naw-Rúz can differ by a day from Iranian Nowruz.
- **2028 Midsumma and Mardi Gras** dates follow the organisers' usual pattern and are flagged `is_estimated` until announced.
- Event dates (Super Bowl, March Madness, elections, pride festivals) were checked against official sources in October 2026.
