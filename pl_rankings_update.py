import os
import re
from datetime import datetime
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
    "wins",
    "draws",
    "losses",
    "goals_for",
    "goals_against",
    "goal_difference",
    "points",
]


def fetch_standings_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()
