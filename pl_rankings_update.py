import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
import requests

STANDINGS_URL = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v5/competitions/8/seasons/2025/standings?live=false"
MATCHWEEK_MATCHES_URL_TMPL = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/competitions/8/seasons/2025/matchweeks/21/matches"
HISTORY_CSV = "pl_standings_history.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}

COLS = [
    "snapshot_utc",
    "season_id",
    "matchweek",
    "position",
    "team_id",
    "team_name",
    "won",
    "drawn",
    "lost",
    "goalsFor",
    "goalsAgainst",
    "goal_difference",
    "points",
]


def fetch_standings_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def is_matchweek_complete(matchweek: int) -> bool:
    url = (
        f"https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/"
        f"competitions/8/seasons/2025/matchweeks/{matchweek}/matches"
    )

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()

    matches = j.get("data", [])
    if not matches:
        return False

    if len(matches) != 10:
        return False
    periods = {m.get("period") for m in matches}

    return periods == {"FullTime"}


def parse_standings(url: str) -> pd.DataFrame:

    standings_json = fetch_standings_json(url)

    # Minimum defensive checks. This keeps the “defensive parsing” requirement.
    if "tables" not in standings_json or not standings_json["tables"]:
        raise RuntimeError("No tables found in JSON.")

    entries = standings_json["tables"][0]["entries"]

    if len(entries) != 20:
        raise RuntimeError(f"Expected 20 entries, len{entries}")

    snapshot_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    season_id = standings_json["season"]["id"]
    matchweek = standings_json["matchweek"]

    all_rows = []

    if not is_matchweek_complete(matchweek):
        return pd.DataFrame(columns=COLS)

    for entry in entries:
        overall = entry["overall"]
        team = entry["team"]
        position = overall["position"]
        team_id = team["id"]
        team_name = entries[i]["team"]["shortName"]
        won = overall["won"]
        drawn = overall["drawn"]
        lost = overall["lost"]
        goalsFor = overall["goalsFor"]
        goalsAgainst = overall["goalsAgainst"]
        goals_difference = goalsFor - goalsAgainst
        points = overall["points"]

        # Populate columns
        all_rows.append(
            {
                "snapshot_utc": snapshot_utc,
                "season_id": season_id,
                "matchweek": matchweek,
                "position": position,
                "team_id": team_id,
                "team_name": team_name,
                "won": won,
                "drawn": drawn,
                "lost": lost,
                "goalsFor": goalsFor,
                "goalsAgainst": goalsAgainst,
                "goal_difference": goals_difference,
                "points": points,
            }
        )

    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError("Parsed 0 rows - UFC page structure  may have changed.")

    # Sanity check for any odd payloads
    positions = sorted([row["position"] for row in all_rows])
    if positions != list(range(1, 21)):
        raise RuntimeError("Positions are not exactly 1...20.")

    return df[COLS]
