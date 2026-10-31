#!/usr/bin/env python3
"""
Global Holiday and Commerce Calendar Generator

Public holidays (incl. every Australian state and territory), China, commercial
and marketing dates, US sports and elections, Australian elections, pride events,
and a religious and cultural observance layer (Christian, Orthodox, Jewish,
Islamic, Sikh, Bahá'í, Hindu, Buddhist, Chinese, Persian).

Generates worldwide public holidays + marketplace-relevant commercial dates
as Parquet files partitioned by year.

Designed to load into BigQuery or Snowflake as a permanent reference table.
Usable standalone or as the data source for the accompanying dbt package.

Usage:
    pip install -r requirements.txt
    python generate_holidays.py --start 2025 --end 2028 --output ../data/holidays

Schema: see README.md > Column Reference
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import holidays as hd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.easter import easter, EASTER_ORTHODOX
from lunardate import LunarDate
from holidays.calendars import (
    _BuddhistLunisolar, _ChineseLunisolar, _HebrewLunisolar, _HinduLunisolar,
    _IslamicLunar, _Persian, _ThaiLunisolar,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Country catalogue ─────────────────────────────────────────────────────────
# Core markets: English-speaking and major ecommerce markets (en-US + en-AU heavy, growing EU + APAC).
# Extended: everything else the `holidays` library supports.
# Remove countries to reduce output size and generation time.

CORE_MARKETS = [
    "AU", "US", "GB", "CA", "DE", "FR", "NZ", "IE", "SG", "IN",
    "JP", "NL", "SE", "CH", "NO", "DK", "AT", "BE", "IT", "ES",
    "PT", "PL", "ZA", "BR", "MX", "CN",
]

# Countries whose holiday names default to a non-Latin script: request English names.
ENGLISH_NAMES = {"CN"}

AU_STATES = ["ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"]

EXTENDED_MARKETS = [
    "AE", "AF", "AG", "AL", "AM", "AO", "AR", "AT", "AW", "AZ",
    "BA", "BB", "BD", "BJ", "BL", "BM", "BN", "BO", "BR", "BS",
    "BT", "BW", "BY", "BZ", "CF", "CG", "CL", "CM", "CN", "CO",
    "CR", "CU", "CV", "CW", "CY", "CZ", "DJ", "DK", "DM", "DO",
    "DZ", "EC", "EE", "EG", "ER", "ES", "ET", "FI", "FJ", "FM",
    "GA", "GE", "GH", "GI", "GM", "GN", "GQ", "GR", "GT", "GU",
    "GW", "GY", "HK", "HN", "HR", "HT", "HU", "ID", "IL", "IM",
    "IQ", "IR", "IS", "JM", "JO", "KE", "KG", "KH", "KI", "KM",
    "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI",
    "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MD", "ME",
    "MG", "MH", "MK", "ML", "MM", "MN", "MO", "MP", "MR", "MS",
    "MT", "MU", "MV", "MW", "MY", "MZ", "NA", "NC", "NE", "NG",
    "NI", "NR", "NU", "OM", "PA", "PE", "PG", "PH", "PK", "PW",
    "PY", "QA", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD",
    "SI", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SV",
    "SX", "SY", "SZ", "TC", "TD", "TG", "TH", "TJ", "TL", "TM",
    "TN", "TO", "TR", "TT", "TV", "TW", "TZ", "UA", "UG", "UY",
    "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU", "WS", "XK",
    "YE", "ZM", "ZW",
]

ALL_COUNTRIES = list(dict.fromkeys(CORE_MARKETS + EXTENDED_MARKETS))


# ── Parquet output schema ─────────────────────────────────────────────────────

SCHEMA = pa.schema([
    pa.field("calendar_date",        pa.date32(),     nullable=False),
    pa.field("country_code",         pa.string(),     nullable=False),
    pa.field("holiday_name",         pa.string(),     nullable=False),
    pa.field("holiday_category",     pa.string(),     nullable=False),  # public_holiday | commercial | observance (religious/cultural)
    pa.field("commercial_season",    pa.string(),     nullable=True),   # nullable for public holidays
    pa.field("is_public_holiday",    pa.bool_(),      nullable=False),
    pa.field("is_commercial_date",   pa.bool_(),      nullable=False),
    pa.field("year",                 pa.int32(),      nullable=False),
    pa.field("month",                pa.int32(),      nullable=False),
    pa.field("day_of_month",         pa.int32(),      nullable=False),
    pa.field("day_of_week_iso",      pa.int32(),      nullable=False),  # 1=Mon, 7=Sun
    pa.field("day_name",             pa.string(),     nullable=False),
    pa.field("is_weekend",           pa.bool_(),      nullable=False),
    pa.field("week_of_year_iso",     pa.int32(),      nullable=False),
    pa.field("calendar_quarter",     pa.int32(),      nullable=False),
    pa.field("fiscal_year_au",       pa.string(),     nullable=False),  # e.g. "FY27"
    pa.field("fiscal_quarter_au",    pa.int32(),      nullable=False),  # 1-4
    pa.field("fiscal_month_au",      pa.int32(),      nullable=False),  # 1=Jul, 12=Jun
    pa.field("fiscal_week_au",       pa.int32(),      nullable=False),
    pa.field("is_core_market",       pa.bool_(),      nullable=False),
    pa.field("subdivision_code",     pa.string(),     nullable=True),
    pa.field("tradition",            pa.string(),     nullable=True),   # observances only: christian, jewish, islamic, ...
    pa.field("is_estimated",         pa.bool_(),      nullable=False),  # lunar/moon-sighting or unannounced dates
])


# ── Date helpers ──────────────────────────────────────────────────────────────

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """nth occurrence (1-based) of ISO weekday (Mon=0, Sun=6) in a given month."""
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta) + timedelta(weeks=n - 1)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Last occurrence of ISO weekday in a given month."""
    next_month_first = date(year + (month // 12), month % 12 + 1, 1)
    last_day = next_month_first - timedelta(days=1)
    delta = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=delta)


def _mothering_sunday(year: int) -> date:
    """UK/IE Mothering Sunday: 3 weeks before Easter (4th Sunday of Lent)."""
    return easter(year) - timedelta(weeks=3)


# ── Verified event tables ─────────────────────────────────────────────────────
# Sources checked Oct 2026: midsumma.org.au, mardigras.org.au announcements, ncaa.com,
# NFL (Super Bowl LXII = 13 Feb 2028), Australian electoral commissions via Hawker Britton.

AU_PRIDE = {  # (date, name, is_estimated)
    2025: [(date(2025, 1, 19), "Midsumma Carnival (festival opens)", False), (date(2025, 2, 2), "Midsumma Pride March", False),
           (date(2025, 2, 9), "Victoria's Pride", False), (date(2025, 2, 14), "Sydney Mardi Gras Festival opens", False),
           (date(2025, 3, 1), "Sydney Mardi Gras Parade", False)],
    2026: [(date(2026, 1, 18), "Midsumma Carnival (festival opens)", False), (date(2026, 2, 1), "Midsumma Pride March", False),
           (date(2026, 2, 8), "Victoria's Pride", False), (date(2026, 2, 13), "Sydney Mardi Gras Festival opens", False),
           (date(2026, 2, 28), "Sydney Mardi Gras Parade", False)],
    2027: [(date(2027, 1, 17), "Midsumma Carnival (festival opens)", False), (date(2027, 1, 31), "Midsumma Pride March", False),
           (date(2027, 2, 7), "Victoria's Pride", False), (date(2027, 2, 12), "Sydney Mardi Gras Festival opens", False),
           (date(2027, 3, 6), "Sydney Mardi Gras Parade", False)],
    2028: [(date(2028, 1, 16), "Midsumma Carnival (festival opens)", True), (date(2028, 1, 30), "Midsumma Pride March", True),
           (date(2028, 2, 6), "Victoria's Pride", True), (date(2028, 2, 11), "Sydney Mardi Gras Festival opens", True),
           (date(2028, 3, 4), "Sydney Mardi Gras Parade", True)],
}

MARCH_MADNESS = {  # NCAA Division I men's tournament
    2025: [(date(2025, 3, 16), "March Madness: Selection Sunday"), (date(2025, 4, 5), "March Madness: Final Four"), (date(2025, 4, 7), "March Madness: National Championship")],
    2026: [(date(2026, 3, 15), "March Madness: Selection Sunday"), (date(2026, 4, 4), "March Madness: Final Four"), (date(2026, 4, 6), "March Madness: National Championship")],
    2027: [(date(2027, 3, 14), "March Madness: Selection Sunday"), (date(2027, 4, 3), "March Madness: Final Four"), (date(2027, 4, 5), "March Madness: National Championship")],
    2028: [(date(2028, 3, 12), "March Madness: Selection Sunday"), (date(2028, 4, 1), "March Madness: Final Four"), (date(2028, 4, 3), "March Madness: National Championship")],
}

AU_ELECTIONS = {  # (date, name, subdivision); the next federal election date is not yet set (due by 20 May 2028)
    2025: [(date(2025, 3, 8), "WA State Election", "WA"), (date(2025, 5, 3), "Australian Federal Election", None),
           (date(2025, 7, 19), "Tasmanian State Election", "TAS")],
    2026: [(date(2026, 3, 21), "SA State Election", "SA"), (date(2026, 11, 28), "Victorian State Election", "VIC")],
    2027: [(date(2027, 3, 13), "NSW State Election", "NSW")],
    2028: [(date(2028, 8, 26), "NT General Election", "NT"), (date(2028, 10, 21), "ACT Election", "ACT"),
           (date(2028, 10, 28), "Queensland State Election", "QLD")],
}


BAHAI = {  # (Naw-Rúz, Birth of the Báb, Birth of Bahá'u'lláh), Bahá'í World Centre table
    2025: (date(2025, 3, 20), date(2025, 10, 22), date(2025, 10, 23)),
    2026: (date(2026, 3, 21), date(2026, 11, 10), date(2026, 11, 11)),
    2027: (date(2027, 3, 21), date(2027, 10, 30), date(2027, 10, 31)),
    2028: (date(2028, 3, 20), date(2028, 10, 19), date(2028, 10, 20)),
}


def _commercial(d: date, country: str, name: str, season: str) -> dict:
    return {
        "calendar_date":      d,
        "country_code":       country,
        "holiday_name":       name,
        "holiday_category":   "commercial",
        "commercial_season":  season,
        "is_public_holiday":  False,
        "is_commercial_date": True,
        "is_estimated":       False,
    }


def printful_moments(year: int) -> list[tuple[date, str]]:
    """US and global marketing moments from Printful's eCommerce holiday calendar.
    Dates already covered elsewhere (Valentine's, Mother's Day, Black Friday...) are omitted."""
    SUN, FRI, THU = 6, 4, 3
    m = [
        (date(year, 1, 13), "National Sticker Day"), (date(year, 1, 15), "National Hat Day"),
        (_nth_weekday(year, 1, THU, 3), "Get to Know Your Customers Day"), (date(year, 1, 21), "National Cheesy Socks Day"),
        (date(year, 2, 1), "Black History Month starts"), (date(year, 2, 2), "Groundhog Day"), (date(year, 2, 13), "Galentine's Day"),
        (date(year, 3, 1), "Women's History Month starts"), (date(year, 3, 3), "World Wildlife Day"), (date(year, 3, 6), "National Dress Day"),
        (date(year, 3, 17), "St. Patrick's Day"),
        (date(year, 4, 1), "April Fools' Day"), (date(year, 4, 10), "National Sibling Day"), (date(year, 4, 11), "National Pet Day"),
        (date(year, 4, 22), "Earth Day"), (date(year, 4, 29), "International Dance Day"),
        (date(year, 5, 1), "Mental Health Awareness Month starts"), (date(year, 5, 5), "Cinco de Mayo"), (_nth_weekday(year, 5, FRI, 1), "Space Day"),
        (date(year, 5, 16), "National Love a Tree Day"), (date(year, 5, 25), "National Wine Day"),
        (_nth_weekday(year, 6, FRI, 1), "National Doughnut Day"), (date(year, 6, 5), "World Environment Day"), (date(year, 6, 14), "Flag Day"),
        (_nth_weekday(year, 6, FRI, 3), "National Flip Flop Day"), (date(year, 6, 21), "International Yoga Day"),
        (date(year, 7, 5), "National Bikini Day"), (date(year, 7, 17), "World Emoji Day"), (_nth_weekday(year, 7, SUN, 3), "National Ice Cream Day"),
        (date(year, 7, 30), "International Day of Friendship"),
        (date(year, 8, 1), "Back-to-school season starts"), (_nth_weekday(year, 8, SUN, 1), "American Family Day"),
        (date(year, 8, 9), "National Book Lovers Day"), (date(year, 8, 19), "World Photography Day"),
        (date(year, 9, 15), "Hispanic Heritage Month starts"), (date(year, 9, 21), "International Day of Peace"),
        (next(date(year, 9, d) for d in range(15, 22) if date(year, 9, d).weekday() == 5), "Oktoberfest starts"),
        (date(year, 10, 5), "World Teachers' Day"), (date(year, 10, 10), "World Mental Health Day"), (date(year, 10, 16), "Boss's Day"),
        (date(year, 11, 1), "Native American Heritage Month starts"), (date(year, 11, 1), "World Vegan Day"), (date(year, 11, 11), "Singles' Day"),
        (_nth_weekday(year, 11, THU, 4) + timedelta(days=2), "Small Business Saturday"),
        (date(year, 12, 4), "National Sock Day"), (date(year, 12, 14), "National Free Shipping Day"), (date(year, 12, 26), "Kwanzaa begins"),
    ]
    return m


# ── Religious and cultural observances ────────────────────────────────────────
# Computed from the `holidays` calendar engines (Hebrew, Islamic, Chinese, Hindu,
# Thai/Buddhist, Persian), dateutil Easter rules, and lunardate for the Chinese
# lunar dates the library does not expose. Jewish and Islamic days begin at sundown
# the evening before the date shown. Islamic dates depend on moon sighting and are
# flagged is_estimated.

def get_observances(year: int) -> list[dict]:
    out = []

    def add(d, name, tradition, est=False):
        if d is None or d.year != year:
            return
        out.append({
            "calendar_date": d, "country_code": "*", "holiday_name": name,
            "holiday_category": "observance", "commercial_season": None,
            "is_public_holiday": False, "is_commercial_date": False,
            "tradition": tradition, "is_estimated": bool(est),
        })

    def first(v):  # library helpers return a date, (date, estimated), or an iterable of either
        if v is None:
            return None, False
        if isinstance(v, date):
            return v, False
        if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], date):
            return v[0], bool(v[1])
        vals = sorted((x, False) if isinstance(x, date) else (x[0], bool(x[1])) for x in v)
        vals = [x for x in vals if x[0].year == year]
        return vals[0] if vals else (None, False)

    # Christian (Western) and Orthodox
    e = easter(year)
    for d, n in [(date(year, 1, 6), "Epiphany"), (e - timedelta(46), "Ash Wednesday"), (e - timedelta(7), "Palm Sunday"),
                 (e - timedelta(2), "Good Friday"), (e, "Easter Sunday"), (e + timedelta(39), "Ascension Day"),
                 (e + timedelta(49), "Pentecost"), (date(year, 11, 1), "All Saints' Day"),
                 (date(year, 12, 25) - timedelta(days=date(year, 12, 25).isoweekday() % 7 + 21), "First Sunday of Advent"),
                 (date(year, 12, 25), "Christmas Day")]:
        add(d, n, "christian")
    add(date(year, 1, 7), "Orthodox Christmas", "christian_orthodox")
    add(easter(year, EASTER_ORTHODOX), "Orthodox Easter", "christian_orthodox")

    # Jewish
    h = _HebrewLunisolar()
    for fn, n in [("purim_date", "Purim"), ("passover_date", "Passover"), ("shavuot_date", "Shavuot"),
                  ("rosh_hashanah_date", "Rosh Hashanah"), ("yom_kippur_date", "Yom Kippur"),
                  ("sukkot_date", "Sukkot"), ("hanukkah_date", "Hanukkah begins")]:
        d, est = first(getattr(h, fn)(year))
        add(d, n, "jewish", est)

    # Islamic (moon sighting: estimated)
    isl = _IslamicLunar()
    for fn, n in [("ramadan_beginning_dates", "Ramadan begins"), ("laylat_al_qadr_dates", "Laylat al-Qadr"),
                  ("eid_al_fitr_dates", "Eid al-Fitr"), ("eid_al_adha_dates", "Eid al-Adha"),
                  ("hijri_new_year_dates", "Islamic New Year"), ("ashura_dates", "Ashura"),
                  ("mawlid_dates", "Mawlid (Prophet's Birthday)"), ("isra_and_miraj_dates", "Isra and Mi'raj")]:
        for d, est in getattr(isl, fn)(year):
            add(d, n, "islamic", True)

    # Hindu and Sikh
    hi = _HinduLunisolar()
    for fn, n, t in [("makar_sankranti_date", "Makar Sankranti", "hindu"), ("pongal_date", "Pongal", "hindu"),
                     ("maha_shivaratri_date", "Maha Shivaratri", "hindu"), ("holi_date", "Holi", "hindu"),
                     ("ram_navami_date", "Ram Navami", "hindu"), ("raksha_bandhan_date", "Raksha Bandhan", "hindu"),
                     ("janmashtami_date", "Janmashtami", "hindu"), ("ganesh_chaturthi_date", "Ganesh Chaturthi", "hindu"),
                     ("sharad_navratri_date", "Navratri begins", "hindu"), ("dussehra_date", "Dussehra", "hindu"),
                     ("diwali_india_date", "Diwali", "hindu"),
                     ("guru_gobind_singh_jayanti_date", "Guru Gobind Singh Jayanti", "sikh"), ("hola_mohalla_date", "Hola Mohalla", "sikh"),
                     ("vaisakhi_date", "Vaisakhi", "sikh"), ("diwali_india_date", "Bandi Chhor Divas", "sikh"),
                     ("guru_nanak_jayanti_date", "Guru Nanak Gurpurab", "sikh")]:
        d, est = first(getattr(hi, fn)(year))
        add(d, n, t, est)

    # Buddhist
    d, est = first(_BuddhistLunisolar().vesak_date(year)); add(d, "Vesak", "buddhist", est)
    th = _ThaiLunisolar()
    add(th.makha_bucha_date(year), "Magha Puja", "buddhist")
    add(th.asarnha_bucha_date(year), "Asalha Puja", "buddhist")
    add(LunarDate(year, 4, 8).to_solar_date(), "Buddha's Birthday (East Asian)", "buddhist")
    add(date(year, 12, 8), "Bodhi Day", "buddhist")

    # Chinese traditional (lunar calendar)
    cn = _ChineseLunisolar()
    lny, _ = first(cn.lunar_new_year_date(year))
    add(lny - timedelta(1), "Lunar New Year's Eve", "chinese"); add(lny, "Lunar New Year", "chinese")
    add(lny + timedelta(14), "Lantern Festival", "chinese")
    add(first(cn.qingming_date(year))[0], "Qingming Festival", "chinese")
    add(first(cn.dragon_boat_date(year))[0], "Dragon Boat Festival", "chinese")
    add(LunarDate(year, 7, 7).to_solar_date(), "Qixi Festival", "chinese")
    add(LunarDate(year, 7, 15).to_solar_date(), "Ghost Festival", "chinese")
    add(first(cn.mid_autumn_date(year))[0], "Mid-Autumn Festival", "chinese")
    add(first(cn.double_ninth_date(year))[0], "Double Ninth Festival", "chinese")
    add(first(cn.winter_solstice_date(year))[0], "Dongzhi (Winter Solstice)", "chinese")
    for ly in (year - 1, year):  # Laba (12th month, 8th day) can fall in January
        try:
            add(LunarDate(ly, 12, 8).to_solar_date(), "Laba Festival", "chinese")
        except ValueError:
            pass

    # Persian (and Bahá'í, which follows the Tehran equinox for Naw-Rúz)
    pe = _Persian()
    nowruz = pe.new_year_date(year)
    last_wed = nowruz - timedelta(days=(nowruz.weekday() - 2) % 7 or 7)
    add(last_wed - timedelta(1), "Chaharshanbe Suri", "persian")
    add(nowruz, "Nowruz (Persian New Year)", "persian")
    add(nowruz + timedelta(12), "Sizdah Be-dar", "persian")
    add(pe.persian_to_gregorian(year - 621, 9, 30), "Shab-e Yalda", "persian")
    # Bahá'í: official Bahá'í World Centre dates (Naw-Rúz uses sunset in Tehran, so it can differ from Iranian Nowruz)
    nr, bab, bah = BAHAI.get(year, (None, None, None))
    add(nr, "Naw-Rúz (Bahá'í New Year)", "bahai")
    add(nr + timedelta(31) if nr else None, "First Day of Ridván", "bahai")
    add(bab, "Birth of the Báb", "bahai")
    add(bah, "Birth of Bahá'u'lláh", "bahai")

    return out


# ── Commercial date catalogue ─────────────────────────────────────────────────

def get_commercial_dates(year: int) -> list[dict]:
    """
    Marketplace-relevant commercial dates with country-level variation.
    Returns a list of dicts ready for the output DataFrame.

    country_code="*" = expand to all countries in ALL_COUNTRIES.

    Notes on design choices:
    - Mother's Day and Father's Day vary by country -- modelled explicitly.
    - Black Friday uses US Thanksgiving as anchor (consistent globally).
    - Back to School is split by hemisphere.
    - Chinese New Year is handled by the `holidays` library for CN/TW.
    """
    rows = []
    SUNDAY = 6  # Python weekday (Mon=0, Sun=6)
    MONDAY = 0
    THURSDAY = 3

    def add(d: date, country: str, name: str, season: str):
        rows.append(_commercial(d, country, name, season))

    # ── Universal / near-universal ────────────────────────────────────────────
    add(date(year, 2, 14),  "*",   "Valentine's Day",          "valentines")
    add(date(year, 3, 8),   "*",   "International Women's Day","womens_day")
    add(date(year, 6, 1),   "*",   "Pride Month Start",        "pride")
    add(date(year, 6, 28),  "*",   "Pride Day",                "pride")
    add(date(year, 3, 31),  "*",   "Trans Day of Visibility",  "pride")
    add(date(year, 5, 17),  "*",   "IDAHOBIT",                 "pride")
    add(date(year, 10, 31), "*",   "Halloween",                "halloween")
    add(date(year, 11, 19), "*",   "International Men's Day",  "mens_day")
    add(date(year, 12, 24), "*",   "Christmas Eve",            "christmas")
    add(date(year, 12, 31), "*",   "New Year's Eve",           "new_year")

    # Easter (varies by year)
    add(easter(year),        "*",   "Easter Sunday",            "easter")
    add(easter(year) - timedelta(days=2), "*", "Good Friday",  "easter")

    # ── Mother's Day ──────────────────────────────────────────────────────────
    # 2nd Sunday May: US, CA, AU, NZ, DE, IN, JP, IT, SG, ZA, and most others
    mothers_2nd_sun_may = _nth_weekday(year, 5, SUNDAY, 2)
    for c in ["US", "CA", "AU", "NZ", "DE", "IN", "JP", "IT", "SG", "ZA",
               "AT", "BE", "CH", "FI", "HK", "MY", "NL", "PE", "PH", "TW"]:
        add(mothers_2nd_sun_may, c, "Mother's Day", "mothers_day")

    # UK/IE: Mothering Sunday (4th Sunday of Lent, 3 weeks before Easter)
    ms_uk = _mothering_sunday(year)
    for c in ["GB", "IE"]:
        add(ms_uk, c, "Mothering Sunday", "mothers_day")

    # France: last Sunday of May
    add(_last_weekday_of_month(year, 5, SUNDAY), "FR", "Fête des Mères", "mothers_day")

    # Spain: 1st Sunday of May
    add(_nth_weekday(year, 5, SUNDAY, 1), "ES", "Día de la Madre", "mothers_day")

    # Nordic: last Sunday of May (same as France)
    for c in ["SE", "NO", "DK"]:
        add(_last_weekday_of_month(year, 5, SUNDAY), c, "Mother's Day", "mothers_day")

    # Brazil: 2nd Sunday of May
    add(_nth_weekday(year, 5, SUNDAY, 2), "BR", "Dia das Mães", "mothers_day")

    # ── Father's Day ──────────────────────────────────────────────────────────
    # 3rd Sunday June: US, CA, GB, IE, FR, DE, NZ, SG, IN, HK, and most
    fathers_3rd_sun_jun = _nth_weekday(year, 6, SUNDAY, 3)
    for c in ["US", "CA", "GB", "IE", "FR", "DE", "NZ", "SG", "IN",
               "HK", "MY", "PH", "CH", "BE", "NL", "IT"]:
        add(fathers_3rd_sun_jun, c, "Father's Day", "fathers_day")

    # AU/NZ: 1st Sunday September
    add(_nth_weekday(year, 9, SUNDAY, 1), "AU", "Father's Day", "fathers_day")

    # Spain: 19 March (St Joseph's Day)
    add(date(year, 3, 19), "ES", "Día del Padre", "fathers_day")

    # Brazil: 2nd Sunday August
    add(_nth_weekday(year, 8, SUNDAY, 2), "BR", "Dia dos Pais", "fathers_day")

    # Sweden/Norway: 2nd Sunday November
    for c in ["SE", "NO"]:
        add(_nth_weekday(year, 11, SUNDAY, 2), c, "Father's Day", "fathers_day")

    # ── Thanksgiving + Black Friday / Cyber Monday ────────────────────────────
    us_thanksgiving = _nth_weekday(year, 11, THURSDAY, 4)
    add(us_thanksgiving, "US", "Thanksgiving", "thanksgiving")

    # Canadian Thanksgiving: 2nd Monday October
    add(_nth_weekday(year, 10, MONDAY, 2), "CA", "Thanksgiving", "thanksgiving")

    black_friday = us_thanksgiving + timedelta(days=1)
    cyber_monday = us_thanksgiving + timedelta(days=4)

    # Black Friday / Cyber Monday are global commercial events now
    for c in CORE_MARKETS:
        add(black_friday, c, "Black Friday",  "black_friday_cm")
        add(cyber_monday, c, "Cyber Monday",  "black_friday_cm")

    # ── Back to School ────────────────────────────────────────────────────────
    # Southern hemisphere: late Jan (AU/NZ school year starts after Australia Day)
    for c in ["AU", "NZ"]:
        add(date(year, 1, 28), c, "Back to School", "back_to_school")

    # Northern hemisphere: late August
    for c in ["US", "CA", "GB", "DE", "FR", "IE", "NL", "BE", "SG"]:
        add(date(year, 8, 25), c, "Back to School", "back_to_school")

    # ── Other key seasons ─────────────────────────────────────────────────────
    # Graduation season (US/AU differ by hemisphere)
    add(date(year, 5, 15), "US", "Graduation Season", "graduation")
    add(date(year, 11, 20), "AU", "Graduation Season", "graduation")


    # ── AU Pride: official dates (Midsumma, Sydney Mardi Gras) ───────────────
    # 2025-2027 from the organisers' published dates; 2028 follows the pattern and is flagged estimated.
    # NZ no longer receives Sydney Mardi Gras dates.
    for d, name, est in AU_PRIDE.get(year, []):
        rows.append({**_commercial(d, "AU", name, "pride"), "is_estimated": est})
    add(_last_weekday_of_month(year, 8, 4), "AU", "Wear It Purple Day", "pride")

    # ── US sports (season "sports") ──────────────────────────────────────────
    add(_nth_weekday(year, 2, SUNDAY, 2), "US", "Super Bowl Sunday", "sports")
    for d, name in MARCH_MADNESS.get(year, []):
        add(d, "US", name, "sports")

    # ── Elections (season "elections") ───────────────────────────────────────
    us_election = _nth_weekday(year, 11, MONDAY, 1) + timedelta(days=1)  # Tuesday after first Monday in Nov
    us_label = {0: "US Presidential Election", 2: "US Midterm Elections"}.get(year % 4, "US Election Day (state and local)")
    add(us_election, "US", us_label, "elections")
    for d, name, sub in AU_ELECTIONS.get(year, []):
        rows.append({**_commercial(d, "AU", name, "elections"), "subdivision_code": sub})

    # ── US marketing moments (Printful eCommerce calendar, season "marketing") ─
    for d, name in printful_moments(year):
        add(d, "US", name, "marketing")

    return rows


# ── AU Fiscal Year ────────────────────────────────────────────────────────────

def au_fiscal_year(d: date) -> str:
    """FY label using end-year convention. July 2026 = FY27."""
    end_year = d.year + 1 if d.month >= 7 else d.year
    return f"FY{str(end_year)[-2:]}"


def au_fiscal_quarter(d: date) -> int:
    """Q1=Jul-Sep, Q2=Oct-Dec, Q3=Jan-Mar, Q4=Apr-Jun."""
    m = d.month
    if 7 <= m <= 9:   return 1
    if 10 <= m <= 12: return 2
    if 1 <= m <= 3:   return 3
    return 4


def au_fiscal_month(d: date) -> int:
    """Month 1 = July, Month 12 = June."""
    return (d.month - 7) % 12 + 1


def au_fiscal_week(d: date) -> int:
    """Week 1 starts July 1 of the fiscal year start."""
    fy_start_year = d.year if d.month >= 7 else d.year - 1
    fy_start = date(fy_start_year, 7, 1)
    return (d - fy_start).days // 7 + 1


# ── Calendar attribute enrichment ─────────────────────────────────────────────

def calendar_attrs(d: date) -> dict:
    iso_day = d.isoweekday()  # 1=Mon, 7=Sun
    return {
        "year":              d.year,
        "month":             d.month,
        "day_of_month":      d.day,
        "day_of_week_iso":   iso_day,
        "day_name":          d.strftime("%A"),
        "is_weekend":        iso_day >= 6,
        "week_of_year_iso":  int(d.strftime("%V")),
        "calendar_quarter":  (d.month - 1) // 3 + 1,
        "fiscal_year_au":    au_fiscal_year(d),
        "fiscal_quarter_au": au_fiscal_quarter(d),
        "fiscal_month_au":   au_fiscal_month(d),
        "fiscal_week_au":    au_fiscal_week(d),
    }


# ── Per-year generation ───────────────────────────────────────────────────────

def generate_year(year: int, countries: list[str]) -> pd.DataFrame:
    rows = []
    core_set = set(CORE_MARKETS)

    # ── Public holidays ───────────────────────────────────────────────────────
    for cc in countries:
        try:
            kw = {"language": "en_US"} if cc in ENGLISH_NAMES else {}
            country_hols = hd.country_holidays(cc, years=year, **kw)
        except Exception as e:
            log.debug(f"Skipping {cc}: {e}")
            continue

        for hol_date, hol_name in country_hols.items():
            rows.append({
                "calendar_date":      hol_date,
                "country_code":       cc,
                "holiday_name":       hol_name,
                "holiday_category":   "public_holiday",
                "commercial_season":  None,
                "is_public_holiday":  True,
                "is_commercial_date": False,
                "is_core_market":     cc in core_set,
                "subdivision_code":   None,
                "tradition":          None,
                "is_estimated":       "(estimated)" in hol_name,
                **calendar_attrs(hol_date),
            })

    # ── AU state/territory holidays ──────────────────────────────────────────
    # Every state and territory, including half-day (from 7pm) and bank holidays.
    # A state holiday is kept unless the national list has the SAME name on that date
    # (previously any state holiday on a national-holiday date was dropped, e.g. SA Proclamation Day).
    if 'AU' in countries:
        national_au = hd.country_holidays('AU', years=year)
        tas_regional = [
            (_nth_weekday(year, 2, 0, 2), "Royal Hobart Regatta (southern Tasmania)"),
            (_nth_weekday(year, 11, 0, 1), "Recreation Day (northern Tasmania)"),
        ]
        for state in AU_STATES:
            state_hols = hd.country_holidays('AU', subdiv=state, years=year, categories=("public", "half_day", "bank"))
            items = list(state_hols.items()) + (tas_regional if state == "TAS" else [])
            for hol_date, hol_name in items:
                if national_au.get(hol_date) == hol_name:
                    continue
                rows.append({
                    "calendar_date":      hol_date,
                    "country_code":       "AU",
                    "holiday_name":       hol_name,
                    "holiday_category":   "public_holiday",
                    "commercial_season":  None,
                    "is_public_holiday":  True,
                    "is_commercial_date": False,
                    "is_core_market":     True,
                    "subdivision_code":   state,
                    "tradition":          None,
                    "is_estimated":       False,
                    **calendar_attrs(hol_date),
                })

        # ── Commercial dates ──────────────────────────────────────────────────────
    country_set = set(countries)
    for comm in get_commercial_dates(year):
        d = comm["calendar_date"]
        target_cc = comm["country_code"]
        targets = countries if target_cc == "*" else [target_cc]

        for cc in targets:
            if cc not in country_set:
                continue
            rows.append({
                "subdivision_code":   None,
                "tradition":          None,
                "is_estimated":       False,
                **comm,
                "country_code": cc,
                "is_core_market": cc in core_set,
                **calendar_attrs(d),
            })

    # ── Religious and cultural observances (every country) ────────────────────
    for obs in get_observances(year):
        for cc in countries:
            rows.append({
                **obs,
                "country_code":     cc,
                "is_core_market":   cc in core_set,
                "subdivision_code": None,
                **calendar_attrs(obs["calendar_date"]),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["calendar_date"] = pd.to_datetime(df["calendar_date"]).dt.date

    # Deduplicate: same date + country + name + category
    df = (
        df
        .drop_duplicates(subset=["calendar_date", "country_code", "holiday_name", "holiday_category", "subdivision_code"])
        .sort_values(["calendar_date", "country_code", "holiday_category"])
        .reset_index(drop=True)
    )
    return df


# ── Parquet writer ────────────────────────────────────────────────────────────

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    int_cols = [
        "year", "month", "day_of_month", "day_of_week_iso",
        "week_of_year_iso", "calendar_quarter",
        "fiscal_quarter_au", "fiscal_month_au", "fiscal_week_au",
    ]
    bool_cols = ["is_public_holiday", "is_commercial_date", "is_weekend", "is_core_market", "is_estimated"]
    df["is_estimated"] = df["is_estimated"].fillna(False)
    df["tradition"] = df["tradition"].where(df["tradition"].notna(), None)

    for c in int_cols:
        df[c] = df[c].astype("int32")
    for c in bool_cols:
        df[c] = df[c].astype(bool)
    df["calendar_date"] = pd.to_datetime(df["calendar_date"]).dt.date
    return df


def write_parquet(df: pd.DataFrame, output_dir: Path) -> None:
    df = cast_types(df)
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    year = df["year"].iloc[0]
    out_path = output_dir / f"year={year}"
    out_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path / "data.parquet", compression="snappy")
    log.info(f"  [{year}] {len(df):>6,} rows -> {out_path}/data.parquet")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate worldwide holiday + commerce calendar (Parquet).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start",   type=int,   default=2025, help="Start year (inclusive)")
    parser.add_argument("--end",     type=int,   default=2028, help="End year (inclusive)")
    parser.add_argument("--output",  type=str,   default="../data/holidays", help="Output directory")
    parser.add_argument("--core-only", action="store_true",
                        help="Generate core markets only (AU, US, GB, CA, DE, FR, NZ, IE, SG, CN, ...)")
    parser.add_argument("--debug",   action="store_true")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    countries = CORE_MARKETS if args.core_only else ALL_COUNTRIES
    log.info(f"Generating {args.start}-{args.end} for {len(countries)} countries")
    log.info(f"Output: {output_dir.resolve()}")

    total = 0
    for year in range(args.start, args.end + 1):
        df = generate_year(year, countries)
        if df.empty:
            log.warning(f"No data for {year}")
            continue
        write_parquet(df, output_dir)
        total += len(df)

    log.info(f"Done. {total:,} total rows across {args.end - args.start + 1} years.")


if __name__ == "__main__":
    main()
