"""
daily_odds.py
-------------
Scrapes yesterday's closing lines from SBR (sportsbookreview.com)
and upserts into Neon game_odds table.

Uses httpx + BeautifulSoup to scrape SBR's NBA odds page.
On scrape failure: logs warning and exits gracefully (non-fatal).

Usage:
    python daily_odds.py               # yesterday's odds
    python daily_odds.py --date 2026-01-15   # specific date
    python daily_odds.py --backfill-days 30  # last 30 days
"""

import argparse
import logging
import os
import re
import sys
import time
from datetime import date, timedelta

import httpx
import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SBR_BASE_URL = "https://www.sportsbookreview.com/betting-odds/nba-basketball/"
REQUEST_TIMEOUT = 20  # seconds
REQUEST_DELAY = 1.5   # seconds between pages

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.sportsbookreview.com/",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Closing-odds book preference order (SBR labels)
CLOSING_BOOKS = ["Pinnacle", "Bet365", "FanDuel", "DraftKings", "Caesars", "BetMGM"]

# ---------------------------------------------------------------------------
# Team name normalisation helpers
# ---------------------------------------------------------------------------
TEAM_NAME_ALIASES: dict[str, str] = {
    "76ers": "Philadelphia 76ers",
    "bucks": "Milwaukee Bucks",
    "bulls": "Chicago Bulls",
    "cavaliers": "Cleveland Cavaliers",
    "celtics": "Boston Celtics",
    "clippers": "LA Clippers",
    "grizzlies": "Memphis Grizzlies",
    "hawks": "Atlanta Hawks",
    "heat": "Miami Heat",
    "hornets": "Charlotte Hornets",
    "jazz": "Utah Jazz",
    "kings": "Sacramento Kings",
    "knicks": "New York Knicks",
    "lakers": "Los Angeles Lakers",
    "magic": "Orlando Magic",
    "mavericks": "Dallas Mavericks",
    "nets": "Brooklyn Nets",
    "nuggets": "Denver Nuggets",
    "pacers": "Indiana Pacers",
    "pelicans": "New Orleans Pelicans",
    "pistons": "Detroit Pistons",
    "raptors": "Toronto Raptors",
    "rockets": "Houston Rockets",
    "spurs": "San Antonio Spurs",
    "suns": "Phoenix Suns",
    "thunder": "Oklahoma City Thunder",
    "timberwolves": "Minnesota Timberwolves",
    "trail blazers": "Portland Trail Blazers",
    "warriors": "Golden State Warriors",
    "wizards": "Washington Wizards",
    # abbreviations / short names
    "phi": "Philadelphia 76ers",
    "mil": "Milwaukee Bucks",
    "chi": "Chicago Bulls",
    "cle": "Cleveland Cavaliers",
    "bos": "Boston Celtics",
    "lac": "LA Clippers",
    "mem": "Memphis Grizzlies",
    "atl": "Atlanta Hawks",
    "mia": "Miami Heat",
    "cha": "Charlotte Hornets",
    "uta": "Utah Jazz",
    "sac": "Sacramento Kings",
    "nyk": "New York Knicks",
    "lal": "Los Angeles Lakers",
    "orl": "Orlando Magic",
    "dal": "Dallas Mavericks",
    "bkn": "Brooklyn Nets",
    "den": "Denver Nuggets",
    "ind": "Indiana Pacers",
    "nop": "New Orleans Pelicans",
    "det": "Detroit Pistons",
    "tor": "Toronto Raptors",
    "hou": "Houston Rockets",
    "sas": "San Antonio Spurs",
    "phx": "Phoenix Suns",
    "okc": "Oklahoma City Thunder",
    "min": "Minnesota Timberwolves",
    "por": "Portland Trail Blazers",
    "gsw": "Golden State Warriors",
    "was": "Washington Wizards",
}


def normalise_team_name(raw: str) -> str:
    """Lowercase + strip then try alias lookup; return cleaned original if no hit."""
    key = raw.strip().lower()
    return TEAM_NAME_ALIASES.get(key, raw.strip())


def fuzzy_team_match(raw: str, candidates: list[str]) -> str | None:
    """Return the best-matching candidate team name, or None."""
    raw_lower = raw.strip().lower()
    # Exact normalised match
    normalised = normalise_team_name(raw).lower()
    for c in candidates:
        if c.lower() == normalised:
            return c
    # Substring match
    for c in candidates:
        if raw_lower in c.lower() or c.lower() in raw_lower:
            return c
    # Word overlap
    raw_words = set(raw_lower.split())
    best: tuple[int, str | None] = (0, None)
    for c in candidates:
        c_words = set(c.lower().split())
        overlap = len(raw_words & c_words)
        if overlap > best[0]:
            best = (overlap, c)
    if best[0] >= 1:
        return best[1]
    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

ODDS_UPSERT = """
INSERT INTO game_odds (
    game_id, game_date, home_team, away_team,
    spread, ml_home, ml_away, over_under, source
)
VALUES %s
ON CONFLICT (game_id) DO UPDATE SET
    spread     = EXCLUDED.spread,
    ml_home    = EXCLUDED.ml_home,
    ml_away    = EXCLUDED.ml_away,
    over_under = EXCLUDED.over_under,
    source     = EXCLUDED.source;
"""


def fetch_game_ids_by_date(conn, target_date: date) -> dict[tuple[str, str], str]:
    """
    Returns {(home_team_name_lower, away_team_name_lower): game_id}
    for games on target_date.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id,
                   LOWER(home_team_name),
                   LOWER(away_team_name)
            FROM   games
            WHERE  game_date = %s;
            """,
            (target_date,),
        )
        rows = cur.fetchall()
    return {(r[1], r[2]): r[0] for r in rows if r[1] and r[2]}


def match_game_id(
    home_raw: str,
    away_raw: str,
    game_map: dict[tuple[str, str], str],
) -> str | None:
    """Attempt fuzzy match of scraped team names to DB game_id."""
    db_home_names = [k[0] for k in game_map]
    db_away_names = [k[1] for k in game_map]

    home_match = fuzzy_team_match(home_raw, [n for n in set(db_home_names)])
    away_match = fuzzy_team_match(away_raw, [n for n in set(db_away_names)])

    if home_match is None or away_match is None:
        return None

    key = (home_match.lower(), away_match.lower())
    if key in game_map:
        return game_map[key]

    # Fallback: try every combination
    for (h, a), gid in game_map.items():
        h_ok = fuzzy_team_match(home_raw, [h]) is not None
        a_ok = fuzzy_team_match(away_raw, [a]) is not None
        if h_ok and a_ok:
            return gid

    return None


# ---------------------------------------------------------------------------
# SBR scraper
# ---------------------------------------------------------------------------

def build_sbr_url(target_date: date) -> str:
    """Return SBR URL for a specific date (YYYYMMDD query param)."""
    date_str = target_date.strftime("%Y%m%d")
    return f"{SBR_BASE_URL}?date={date_str}"


def safe_float_parse(text: str | None) -> float | None:
    if not text:
        return None
    text = text.strip().replace("½", ".5").replace("–", "-").replace("−", "-")
    # Remove trailing letters (e.g., 'u' in over/under display)
    text = re.sub(r"[a-zA-Z]$", "", text).strip()
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def parse_american_odds(text: str | None) -> int | None:
    if not text:
        return None
    text = text.strip().replace("–", "-").replace("−", "-")
    try:
        return int(text)
    except (ValueError, TypeError):
        return None


def scrape_sbr_page(target_date: date) -> list[dict]:
    """
    Scrape SBR NBA odds page for target_date.
    Returns list of dicts with keys:
      home_team, away_team, spread_home, spread_away,
      ml_home, ml_away, over_under, book_name
    Returns empty list on any failure.
    """
    url = build_sbr_url(target_date)
    log.info("Scraping SBR: %s", url)

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url, headers=HEADERS)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        log.warning("SBR HTTP error %s for %s: %s", exc.response.status_code, url, exc)
        return []
    except httpx.RequestError as exc:
        log.warning("SBR request error for %s: %s", url, exc)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    games = []

    # --- Strategy 1: JSON-LD or data attributes (SBR sometimes embeds structured data) ---
    # --- Strategy 2: Parse the odds table HTML ---
    # SBR renders its main odds table with divs having class patterns like
    # "GameRows_container", "OddsTable", etc. We use a resilient multi-strategy
    # approach so that minor HTML changes don't break the scraper.

    games = _parse_sbr_html(soup, target_date)

    if not games:
        log.warning(
            "SBR scrape returned 0 games for %s — page structure may have changed.", target_date
        )

    return games


def _parse_sbr_html(soup: BeautifulSoup, target_date: date) -> list[dict]:
    """
    Multi-strategy parser for SBR NBA odds HTML.
    Tries several known selectors; returns first non-empty result.
    """
    results = []

    # Strategy A: Look for elements containing team names in odds rows
    # SBR typically has a structure like:
    #   <div class="...GameRows..."> <div class="...Participant...">TEAM</div> ... <div>ODDS</div>
    results = _parse_sbr_strategy_a(soup)
    if results:
        log.info("  SBR parse strategy A: found %d games", len(results))
        return results

    # Strategy B: Try to find any table rows with spread-like patterns (+/-X.5)
    results = _parse_sbr_strategy_b(soup)
    if results:
        log.info("  SBR parse strategy B: found %d games", len(results))
        return results

    return []


def _parse_sbr_strategy_a(soup: BeautifulSoup) -> list[dict]:
    """
    Parse SBR's React-rendered odds table.
    Looks for the main event rows and extracts team + odds cells.
    """
    games = []

    # Common SBR class fragments (may change with deployments)
    row_selectors = [
        "[class*='GameRows']",
        "[class*='eventLine']",
        "[class*='game-row']",
        "[data-testid*='game']",
        ".oddsTableModel",
    ]

    event_rows = []
    for sel in row_selectors:
        event_rows = soup.select(sel)
        if event_rows:
            break

    if not event_rows:
        return []

    for row in event_rows:
        try:
            # Extract team name text nodes
            team_els = (
                row.select("[class*='Participant']")
                or row.select("[class*='team']")
                or row.select("[class*='Team']")
                or row.select("span[class*='name']")
            )
            if len(team_els) < 2:
                continue

            away_team = team_els[0].get_text(strip=True)
            home_team = team_els[1].get_text(strip=True)
            if not away_team or not home_team:
                continue

            # Extract odds cells: spread, ml, total
            odds_cells = (
                row.select("[class*='OddsCell']")
                or row.select("[class*='odds-cell']")
                or row.select("[class*='oddsCell']")
                or row.select("td")
            )

            spread_home = spread_away = ml_home = ml_away = over_under = None
            book_name = "SBR Closing"

            # SBR typically orders cells: Spread / MoneyLine / Total
            # Each "cell" may contain two sub-values (home + away)
            nums = []
            for cell in odds_cells:
                text = cell.get_text(strip=True)
                # Extract numbers that look like odds
                found = re.findall(r"[+\-]?\d+(?:\.\d+)?", text)
                nums.extend(found)

            if len(nums) >= 6:
                # Heuristic: first 2 are spread (away, home), next 2 are ML, last 2 are total
                spread_away = safe_float_parse(nums[0])
                spread_home = safe_float_parse(nums[1])
                ml_away_val = parse_american_odds(nums[2])
                ml_home_val = parse_american_odds(nums[3])
                ml_away = ml_away_val
                ml_home = ml_home_val
                # Total: just one number usually
                ou_val = safe_float_parse(nums[4])
                over_under = ou_val
            elif len(nums) >= 2:
                spread_away = safe_float_parse(nums[0])
                spread_home = safe_float_parse(nums[1])

            games.append(
                {
                    "home_team": home_team,
                    "away_team": away_team,
                    "spread_home": spread_home,
                    "spread_away": spread_away,
                    "ml_home": ml_home,
                    "ml_away": ml_away,
                    "over_under": over_under,
                    "book_name": book_name,
                }
            )

        except Exception as exc:
            log.debug("Row parse error (strategy A): %s", exc)
            continue

    return games


def _parse_sbr_strategy_b(soup: BeautifulSoup) -> list[dict]:
    """
    Fallback: scan all text for patterns like '+3.5' next to team names.
    Very broad — may produce noise; used only when strategy A fails.
    """
    games = []

    # Look for <tr> or row-like elements with a spread pattern
    spread_pattern = re.compile(r"[+\-]\d+\.5")
    ml_pattern = re.compile(r"[+\-]\d{3,4}")

    all_rows = soup.find_all("tr")
    if not all_rows:
        all_rows = soup.find_all(attrs={"class": re.compile(r"row|Row|event|game", re.I)})

    i = 0
    while i < len(all_rows) - 1:
        try:
            row_text = all_rows[i].get_text(" ", strip=True)
            next_text = all_rows[i + 1].get_text(" ", strip=True) if i + 1 < len(all_rows) else ""

            # Check if these rows contain spread patterns
            if spread_pattern.search(row_text) or ml_pattern.search(row_text):
                # Crude extraction: first non-numeric word cluster = team name
                team_away = re.sub(r"[+\-]?\d+[.\d]*.*", "", row_text).strip()
                team_home = re.sub(r"[+\-]?\d+[.\d]*.*", "", next_text).strip()

                if not team_away or not team_home:
                    i += 1
                    continue

                nums_away = re.findall(r"[+\-]?\d+(?:\.\d+)?", row_text)
                nums_home = re.findall(r"[+\-]?\d+(?:\.\d+)?", next_text)

                spread_away = safe_float_parse(nums_away[0]) if nums_away else None
                spread_home = safe_float_parse(nums_home[0]) if nums_home else None
                ml_away = parse_american_odds(nums_away[1]) if len(nums_away) > 1 else None
                ml_home = parse_american_odds(nums_home[1]) if len(nums_home) > 1 else None
                over_under = safe_float_parse(nums_away[2]) if len(nums_away) > 2 else None

                games.append(
                    {
                        "home_team": team_home,
                        "away_team": team_away,
                        "spread_home": spread_home,
                        "spread_away": spread_away,
                        "ml_home": ml_home,
                        "ml_away": ml_away,
                        "over_under": over_under,
                        "book_name": "SBR Closing",
                    }
                )
                i += 2
                continue
        except Exception as exc:
            log.debug("Row parse error (strategy B): %s", exc)
        i += 1

    return games


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_date(conn, target_date: date) -> int:
    """Scrape and upsert odds for a single date. Returns row count."""
    scraped = scrape_sbr_page(target_date)
    if not scraped:
        log.warning("No odds data scraped for %s — skipping.", target_date)
        return 0

    game_map = fetch_game_ids_by_date(conn, target_date)
    if not game_map:
        log.warning("No games found in DB for %s — cannot match odds.", target_date)
        return 0

    log.info(
        "Matching %d scraped games against %d DB games for %s …",
        len(scraped), len(game_map), target_date,
    )

    rows_to_upsert = []
    for item in scraped:
        game_id = match_game_id(item["home_team"], item["away_team"], game_map)
        if game_id is None:
            log.warning(
                "  No DB match: home='%s' away='%s' on %s — skipping.",
                item["home_team"], item["away_team"], target_date,
            )
            continue

        rows_to_upsert.append((
            game_id,
            target_date,
            item.get("home_team"),
            item.get("away_team"),
            item.get("spread_home"),   # home-perspective spread per plan
            item.get("ml_home"),
            item.get("ml_away"),
            item.get("over_under"),
            "sbr",
        ))

    if not rows_to_upsert:
        log.info("No rows to upsert for %s after matching.", target_date)
        return 0

    with conn.cursor() as cur:
        execute_values(cur, ODDS_UPSERT, rows_to_upsert)
    conn.commit()
    log.info("  Upserted %d odds rows for %s", len(rows_to_upsert), target_date)
    return len(rows_to_upsert)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape NBA odds from SBR → Neon")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=str, help="Specific date YYYY-MM-DD")
    group.add_argument("--backfill-days", type=int, metavar="N", help="Scrape last N days")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set.")
        sys.exit(1)

    try:
        conn = psycopg2.connect(db_url)
    except Exception as exc:
        log.error("DB connection failed: %s", exc)
        sys.exit(1)

    try:
        if args.date:
            try:
                dates = [date.fromisoformat(args.date)]
            except ValueError:
                log.error("Invalid date format '%s'. Use YYYY-MM-DD.", args.date)
                sys.exit(1)
        elif args.backfill_days:
            today = date.today()
            dates = [today - timedelta(days=i) for i in range(1, args.backfill_days + 1)]
        else:
            dates = [date.today() - timedelta(days=1)]

        total = 0
        for d in dates:
            try:
                n = process_date(conn, d)
                total += n
                if len(dates) > 1:
                    time.sleep(REQUEST_DELAY)
            except Exception as exc:
                log.warning("Error processing date %s: %s — continuing.", d, exc)
                conn.rollback()

        print(f"\n=== Odds ingest complete: {total} rows upserted across {len(dates)} date(s) ===")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
