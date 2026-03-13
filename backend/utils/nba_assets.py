"""
utils/nba_assets.py
-------------------
NBA team colors (by team_id), logo URLs, player headshot CDN URLs.
All images served from NBA's public CDN — no API key required.
"""

# ── Team Colors (team_id → {primary, secondary}) ──────────────────────────────
TEAM_COLORS: dict[int, dict] = {
    # Atlantic
    1610612738: {"primary": "#007A33", "secondary": "#BA9653"},   # Boston Celtics
    1610612751: {"primary": "#000000", "secondary": "#FFFFFF"},   # Brooklyn Nets
    1610612752: {"primary": "#F58426", "secondary": "#006BB6"},   # New York Knicks
    1610612755: {"primary": "#006BB6", "secondary": "#ED174C"},   # Philadelphia 76ers
    1610612761: {"primary": "#CE1141", "secondary": "#000000"},   # Toronto Raptors
    # Central
    1610612741: {"primary": "#CE1141", "secondary": "#000000"},   # Chicago Bulls
    1610612739: {"primary": "#860038", "secondary": "#FDBB30"},   # Cleveland Cavaliers
    1610612765: {"primary": "#C8102E", "secondary": "#006BB6"},   # Detroit Pistons
    1610612754: {"primary": "#002D62", "secondary": "#FDBB30"},   # Indiana Pacers
    1610612749: {"primary": "#00471B", "secondary": "#EEE1C6"},   # Milwaukee Bucks
    # Southeast
    1610612737: {"primary": "#E03A3E", "secondary": "#C1D32F"},   # Atlanta Hawks
    1610612766: {"primary": "#1D1160", "secondary": "#00788C"},   # Charlotte Hornets
    1610612748: {"primary": "#98002E", "secondary": "#F9A01B"},   # Miami Heat
    1610612753: {"primary": "#0077C0", "secondary": "#C4CED4"},   # Orlando Magic
    1610612764: {"primary": "#002B5C", "secondary": "#E31837"},   # Washington Wizards
    # Northwest
    1610612743: {"primary": "#0E2240", "secondary": "#FEC524"},   # Denver Nuggets
    1610612750: {"primary": "#0C2340", "secondary": "#236192"},   # Minnesota Timberwolves
    1610612760: {"primary": "#007AC1", "secondary": "#EF3B24"},   # Oklahoma City Thunder
    1610612757: {"primary": "#E03A3E", "secondary": "#000000"},   # Portland Trail Blazers
    1610612762: {"primary": "#002B5C", "secondary": "#00471B"},   # Utah Jazz
    # Pacific
    1610612744: {"primary": "#1D428A", "secondary": "#FFC72C"},   # Golden State Warriors
    1610612746: {"primary": "#C8102E", "secondary": "#1D428A"},   # LA Clippers
    1610612747: {"primary": "#552583", "secondary": "#FDB927"},   # Los Angeles Lakers
    1610612756: {"primary": "#1D1160", "secondary": "#E56020"},   # Phoenix Suns
    1610612758: {"primary": "#5A2D81", "secondary": "#63727A"},   # Sacramento Kings
    # Southwest
    1610612742: {"primary": "#00538C", "secondary": "#002B5E"},   # Dallas Mavericks
    1610612763: {"primary": "#5D76A9", "secondary": "#12173F"},   # Memphis Grizzlies
    1610612745: {"primary": "#CE1141", "secondary": "#000000"},   # Houston Rockets
    1610612740: {"primary": "#0C2340", "secondary": "#C8102E"},   # New Orleans Pelicans
    1610612759: {"primary": "#C4CED4", "secondary": "#000000"},   # San Antonio Spurs
}

# ── Team Tricodes (team_id → tricode) ─────────────────────────────────────────
TEAM_TRICODES: dict[int, str] = {
    1610612737: "ATL",
    1610612738: "BOS",
    1610612751: "BKN",
    1610612766: "CHA",
    1610612741: "CHI",
    1610612739: "CLE",
    1610612742: "DAL",
    1610612743: "DEN",
    1610612765: "DET",
    1610612744: "GSW",
    1610612745: "HOU",
    1610612754: "IND",
    1610612746: "LAC",
    1610612747: "LAL",
    1610612763: "MEM",
    1610612748: "MIA",
    1610612749: "MIL",
    1610612750: "MIN",
    1610612740: "NOP",
    1610612752: "NYK",
    1610612760: "OKC",
    1610612753: "ORL",
    1610612755: "PHI",
    1610612756: "PHX",
    1610612757: "POR",
    1610612758: "SAC",
    1610612759: "SAS",
    1610612761: "TOR",
    1610612762: "UTA",
    1610612764: "WAS",
}

# ── Team Names (team_id → full name) ──────────────────────────────────────────
TEAM_NAMES: dict[int, str] = {
    1610612737: "Atlanta Hawks",
    1610612738: "Boston Celtics",
    1610612751: "Brooklyn Nets",
    1610612766: "Charlotte Hornets",
    1610612741: "Chicago Bulls",
    1610612739: "Cleveland Cavaliers",
    1610612742: "Dallas Mavericks",
    1610612743: "Denver Nuggets",
    1610612765: "Detroit Pistons",
    1610612744: "Golden State Warriors",
    1610612745: "Houston Rockets",
    1610612754: "Indiana Pacers",
    1610612746: "LA Clippers",
    1610612747: "Los Angeles Lakers",
    1610612763: "Memphis Grizzlies",
    1610612748: "Miami Heat",
    1610612749: "Milwaukee Bucks",
    1610612750: "Minnesota Timberwolves",
    1610612740: "New Orleans Pelicans",
    1610612752: "New York Knicks",
    1610612760: "Oklahoma City Thunder",
    1610612753: "Orlando Magic",
    1610612755: "Philadelphia 76ers",
    1610612756: "Phoenix Suns",
    1610612757: "Portland Trail Blazers",
    1610612758: "Sacramento Kings",
    1610612759: "San Antonio Spurs",
    1610612761: "Toronto Raptors",
    1610612762: "Utah Jazz",
    1610612764: "Washington Wizards",
}

# ── Reverse lookup: tricode → team_id ─────────────────────────────────────────
TEAM_IDS: dict[str, int] = {v: k for k, v in TEAM_TRICODES.items()}

# ── Fallback colors ────────────────────────────────────────────────────────────
_DEFAULT_COLORS = {"primary": "#1d428a", "secondary": "#c8102e"}


def get_team_logo_url(team_id: int) -> str:
    """Return the NBA CDN SVG logo URL for a team."""
    return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"


def get_player_headshot_url(player_id: int) -> str:
    """Return the NBA CDN headshot URL for a player."""
    return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png"


def get_team_colors(team_id: int) -> dict:
    """Return {primary, secondary} color dict for team_id, with fallback."""
    return TEAM_COLORS.get(int(team_id), _DEFAULT_COLORS)
