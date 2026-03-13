"""
daily_injuries.py
-----------------
Scrapes injury reports from ESPN and CBS Sports.
Upserts into Neon injuries table as today's snapshot.

Usage:
    python daily_injuries.py
"""

import os
import sys
import logging
from datetime import date

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(__file__))

# Team name → team_id mapping
TEAM_IDS = {
    "Atlanta Hawks": 1610612737, "Boston Celtics": 1610612738, "Brooklyn Nets": 1610612751,
    "Charlotte Hornets": 1610612766, "Chicago Bulls": 1610612741, "Cleveland Cavaliers": 1610612739,
    "Dallas Mavericks": 1610612742, "Denver Nuggets": 1610612743, "Detroit Pistons": 1610612765,
    "Golden State Warriors": 1610612744, "Houston Rockets": 1610612745, "Indiana Pacers": 1610612754,
    "LA Clippers": 1610612746, "Los Angeles Lakers": 1610612747, "Memphis Grizzlies": 1610612763,
    "Miami Heat": 1610612748, "Milwaukee Bucks": 1610612749, "Minnesota Timberwolves": 1610612750,
    "New Orleans Pelicans": 1610612740, "New York Knicks": 1610612752, "Oklahoma City Thunder": 1610612760,
    "Orlando Magic": 1610612753, "Philadelphia 76ers": 1610612755, "Phoenix Suns": 1610612756,
    "Portland Trail Blazers": 1610612757, "Sacramento Kings": 1610612758, "San Antonio Spurs": 1610612759,
    "Toronto Raptors": 1610612761, "Utah Jazz": 1610612762, "Washington Wizards": 1610612764,
    # Alternate names
    "Hawks": 1610612737, "Celtics": 1610612738, "Nets": 1610612751,
    "Hornets": 1610612766, "Bulls": 1610612741, "Cavaliers": 1610612739,
    "Mavericks": 1610612742, "Nuggets": 1610612743, "Pistons": 1610612765,
    "Warriors": 1610612744, "Rockets": 1610612745, "Pacers": 1610612754,
    "Clippers": 1610612746, "Lakers": 1610612747, "Grizzlies": 1610612763,
    "Heat": 1610612748, "Bucks": 1610612749, "Timberwolves": 1610612750,
    "Pelicans": 1610612740, "Knicks": 1610612752, "Thunder": 1610612760,
    "Magic": 1610612753, "76ers": 1610612755, "Sixers": 1610612755,
    "Suns": 1610612756, "Trail Blazers": 1610612757, "Blazers": 1610612757,
    "Kings": 1610612758, "Spurs": 1610612759, "Raptors": 1610612761,
    "Jazz": 1610612762, "Wizards": 1610612764,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.google.com",
}


def normalize_status(raw: str) -> str:
    s = str(raw).lower().strip()
    if "out for season" in s or "season" in s:
        return "Out For Season"
    if "day-to-day" in s or "dtd" in s or "day to day" in s:
        return "Day-To-Day"
    if "game time" in s or "gtd" in s:
        return "Questionable"
    if "doubtful" in s:
        return "Doubtful"
    if "questionable" in s:
        return "Questionable"
    if "probable" in s:
        return "Probable"
    if "out" in s:
        return "Out"
    return raw.strip()


def team_id_lookup(name: str) -> int | None:
    if not name:
        return None
    name = str(name).strip()
    tid = TEAM_IDS.get(name)
    if tid:
        return tid
    # Partial match
    for k, v in TEAM_IDS.items():
        if k.lower() in name.lower() or name.lower() in k.lower():
            return v
    return None


def scrape_espn() -> list[dict]:
    """Scrape ESPN NBA injuries page."""
    url = "https://www.espn.com/nba/injuries"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning("ESPN request failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    injuries = []
    today = date.today()

    # ESPN injury page: team sections with h2/h3 headers followed by tables
    # Each team section has a class containing team name and table rows
    team_name = None

    for section in soup.find_all(["div", "section"], class_=lambda c: c and "ResponsiveTable" in c):
        # Try to find team name near this section
        header = section.find_previous(["h2", "h3", "h1"])
        if header:
            team_name = header.get_text(strip=True)

        table = section.find("table")
        if not table:
            continue

        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            cols = row.find_all("td")
            if len(cols) < 3:
                continue
            try:
                player_name = cols[0].get_text(strip=True)
                position    = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                injury_type = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                status_raw  = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                updated_str = cols[4].get_text(strip=True) if len(cols) > 4 else ""

                if not player_name or player_name.lower() in ("player", "name"):
                    continue

                status = normalize_status(status_raw)
                tid    = team_id_lookup(team_name)

                injuries.append({
                    "snapshot_date": today,
                    "team_name":     team_name or "",
                    "team_id":       tid,
                    "player_name":   player_name,
                    "position":      position,
                    "injury_type":   injury_type,
                    "status":        status,
                    "updated":       updated_str,
                    "source":        "espn",
                })
            except Exception:
                continue

    # Fallback: try alternative ESPN structure
    if not injuries:
        for team_div in soup.find_all("div", class_=lambda c: c and "injuries" in str(c).lower()):
            header = team_div.find(["h2", "h3"])
            if header:
                team_name = header.get_text(strip=True)
            for row in team_div.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                player_name = cols[0].get_text(strip=True)
                status_raw  = cols[-1].get_text(strip=True)
                if not player_name:
                    continue
                injuries.append({
                    "snapshot_date": today,
                    "team_name":     team_name or "",
                    "team_id":       team_id_lookup(team_name),
                    "player_name":   player_name,
                    "position":      cols[1].get_text(strip=True) if len(cols) > 1 else "",
                    "injury_type":   cols[2].get_text(strip=True) if len(cols) > 2 else "",
                    "status":        normalize_status(status_raw),
                    "updated":       "",
                    "source":        "espn",
                })

    log.info("ESPN: scraped %d injury rows", len(injuries))
    return injuries


def scrape_cbs() -> list[dict]:
    """Scrape CBS Sports NBA injuries page."""
    url = "https://www.cbssports.com/nba/injuries/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning("CBS request failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    injuries = []
    today = date.today()
    team_name = None

    # CBS: sections per team, each with a team title and injury table
    for section in soup.find_all(["section", "div"], class_=lambda c: c and "TeamLogoNameLockup" not in str(c)):
        # Find team header
        h3 = section.find("h3", class_=lambda c: True)
        if h3:
            team_name = h3.get_text(strip=True)

        table = section.find("table")
        if not table:
            continue

        rows = table.find_all("tr")
        for row in rows[1:]:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            try:
                player_name = cols[0].get_text(strip=True)
                position    = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                injury_type = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                status_raw  = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                updated_str = cols[4].get_text(strip=True) if len(cols) > 4 else ""

                if not player_name or player_name.lower() in ("player", "name"):
                    continue

                injuries.append({
                    "snapshot_date": today,
                    "team_name":     team_name or "",
                    "team_id":       team_id_lookup(team_name),
                    "player_name":   player_name,
                    "position":      position,
                    "injury_type":   injury_type,
                    "status":        normalize_status(status_raw),
                    "updated":       updated_str,
                    "source":        "cbs",
                })
            except Exception:
                continue

    log.info("CBS: scraped %d injury rows", len(injuries))
    return injuries


def upsert_injuries(rows: list[dict], source: str):
    """Delete today's existing rows for this source, then insert fresh."""
    if not rows:
        return

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        log.error("DATABASE_URL not set")
        return

    import psycopg2
    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    today = date.today()

    try:
        with conn.cursor() as cur:
            # Delete today's existing rows for this source
            cur.execute(
                "DELETE FROM injuries WHERE snapshot_date = %s AND source = %s",
                [today, source],
            )

            # Bulk insert
            insert_sql = """
                INSERT INTO injuries (
                    snapshot_date, team_name, team_id, player_name,
                    position, injury_type, status, updated, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            vals = [
                (
                    r["snapshot_date"], r.get("team_name"), r.get("team_id"),
                    r["player_name"], r.get("position"), r.get("injury_type"),
                    r.get("status"), r.get("updated"), r["source"],
                )
                for r in rows if r.get("player_name")
            ]
            cur.executemany(insert_sql, vals)
            conn.commit()
            log.info("%s: inserted %d rows into Neon injuries", source.upper(), len(vals))
    except Exception as e:
        conn.rollback()
        log.error("DB insert failed for %s: %s", source, e)
    finally:
        conn.close()


def main():
    log.info("Starting daily injury scrape — %s", date.today())

    # ESPN
    try:
        espn_rows = scrape_espn()
        upsert_injuries(espn_rows, "espn")
    except Exception as e:
        log.warning("ESPN scrape failed entirely: %s", e)

    # CBS
    try:
        cbs_rows = scrape_cbs()
        upsert_injuries(cbs_rows, "cbs")
    except Exception as e:
        log.warning("CBS scrape failed entirely: %s", e)

    log.info("Injury scrape complete")


if __name__ == "__main__":
    main()
