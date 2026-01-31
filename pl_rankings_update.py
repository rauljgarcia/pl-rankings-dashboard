"""
Premier League Standings Scraper (Matchweek Snapshots)

Goal
----
Record FINAL Premier League standings once per COMPLETED matchweek (no mid-matchweek
snapshots) and append them to an append-only CSV history file.

Data source
-----------
premierleague.com standings are client-rendered. This script uses the same Pulselive
JSON endpoints used by the site (no HTML/DOM parsing).

Snapshot semantics
------------------
- Exactly ONE snapshot per completed matchweek.
- A matchweek is considered "complete" when the matchweek has 10 matches and all
  matches have period == "FullTime".
- History is append-only, deduped by matchweek number.

Phase A (standings)
-------------------
Pull standings from the Pulselive standings endpoint and parse the 20-row league table.

Phase B (enrichment)
--------------------
For each team, fetch the next scheduled league fixture and derive:
- next_opponent_id
- next_opponent_name
- is_home_next (True if the team is the home team in its next fixture)

Output
------
CSV history where each completed matchweek contributes exactly 20 rows.
"""

import os
from datetime import datetime, timezone


import pandas as pd
import requests

# -----------------------
# Configuration constants
# -----------------------

STANDINGS_URL = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v5/competitions/8/seasons/2025/standings?live=false"
MATCHWEEK_MATCHES_URL_TMPL = "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/competitions/8/seasons/2025/matchweeks/{matchweek}/matches"
NEXTFIXTURE_URL_TMPL = (
    "https://sdp-prem-prod.premier-league-prod.pulselive.com/api/v1/"
    "competitions/8/seasons/2025/teams/{team_id}/nextfixture"
)
HISTORY_CSV = "pl_standings_history.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}

PHASE_A_COLS = [
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

COLS = PHASE_A_COLS + ["next_opponent_id", "next_opponent_name", "is_home_next"]


def fetch_standings_json(url: str) -> dict:
    """
    Fetch the Pulselive standings payload as JSON.

    Args:
        url: Pulselive standings endpoint URL.

    Returns:
        Parsed JSON response as a Python dict.

    Raises:
        requests.HTTPError: If the response status is not 2xx.
        requests.RequestException: For network-related errors (timeouts, etc.).
        ValueError: If the response body is not valid JSON.
    """
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def is_matchweek_complete(matchweek: int) -> bool:
    """
    Determine whether a matchweek is fully complete.

    A matchweek is considered complete when:
    - the matchweek contains exactly 10 matches (20 teams), and
    - all matches have period == "FullTime"

    Args:
        matchweek: Matchweek number to check.

    Returns:
        True if the matchweek appears complete, otherwise False.

    Raises:
        requests.HTTPError: If the matches endpoint returns a non-2xx status.
        requests.RequestException: For network-related errors (timeouts, etc.).
        ValueError: If the response body is not valid JSON.
    """


def is_matchweek_complete(matchweek: int) -> bool:
    """
    Determine whether a matchweek is fully complete.

    A matchweek is considered complete when:
    - all matches have period == "FullTime", and
    - 20 unique teams appear across the fixtures (covers normal weeks and avoids
      partial/duplicate data issues).

    Args:
        matchweek: Matchweek number to check.

    Returns:
        True if the matchweek appears complete, otherwise False.

    Raises:
        requests.HTTPError: If the matches endpoint returns a non-2xx status.
        requests.RequestException: For network-related errors (timeouts, etc.).
        ValueError: If the response body is not valid JSON.
    """
    url = MATCHWEEK_MATCHES_URL_TMPL.format(matchweek=matchweek)

    r = requests.get(url, headers=HEADERS, timeout=30, params={"_limit": 100})
    r.raise_for_status()
    j = r.json()

    matches = j.get("data", [])
    if not matches:
        return False

    if any(m.get("period") != "FullTime" for m in matches):
        return False

    team_ids = set()
    for m in matches:
        home = m.get("homeTeam", {})
        away = m.get("awayTeam", {})
        if home.get("id") is not None:
            team_ids.add(str(home["id"]))
        if away.get("id") is not None:
            team_ids.add(str(away["id"]))

    # 🔍 Optional debug (safe to leave in or remove later)
    print(
        f"matchweek={matchweek} "
        f"matches={len(matches)} "
        f"periods={set(m.get('period') for m in matches)} "
        f"teams={len(team_ids)}"
    )

    return len(team_ids) == 20


def parse_standings(url: str) -> pd.DataFrame:
    """
    Parse the league table standings for the current matchweek.

    This function performs Phase A:
    - Fetch standings JSON.
    - Gate on matchweek completion.
    - Parse a 20-row league table into a tidy DataFrame.

    Notes:
        - If the current matchweek is not complete, returns an empty DataFrame
          with PHASE_A_COLS (so callers can cleanly exit without exceptions).
        - Defensive checks are included to detect schema or payload changes.

    Args:
        url: Pulselive standings endpoint URL.

    Returns:
        DataFrame with one row per team (20 rows) and columns PHASE_A_COLS.
        Returns an empty DataFrame (PHASE_A_COLS) if the matchweek is not complete.

    Raises:
        RuntimeError: If the standings payload is missing expected keys or has an
            unexpected structure (e.g., not 20 entries, positions not 1..20).
        requests.HTTPError / requests.RequestException / ValueError:
            Propagated from fetch_standings_json().
    """
    standings_json = fetch_standings_json(url)

    # Minimum defensive checks. This keeps the “defensive parsing” requirement.
    if "tables" not in standings_json or not standings_json["tables"]:
        raise RuntimeError("No tables found in JSON.")

    entries = standings_json["tables"][0]["entries"]

    if len(entries) != 20:
        raise RuntimeError(f"Expected 20 entries, got {len(entries)}")

    snapshot_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    season_id = standings_json["season"]["id"]
    matchweek = standings_json["matchweek"]

    all_rows = []

    if not is_matchweek_complete(matchweek):
        return pd.DataFrame(columns=PHASE_A_COLS)

    for entry in entries:
        overall = entry["overall"]
        team = entry["team"]

        position = overall["position"]
        team_id = team["id"]
        team_name = team["shortName"]
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
        raise RuntimeError("Parsed 0 rows - PL standings structure may have changed.")

    # Sanity check for any odd payloads
    positions = sorted([row["position"] for row in all_rows])
    if positions != list(range(1, 21)):
        raise RuntimeError("Positions are not exactly 1...20.")
    return df[PHASE_A_COLS]


def fetch_next_fixture_json(team_id: str) -> dict:
    """
    Fetch the next scheduled league fixture for a given team.

    This endpoint returns a single match object describing the team's next
    Premier League fixture (pre-match), including homeTeam and awayTeam.

    Args:
        team_id: Pulselive team id (string or int-like string).

    Returns:
        Parsed JSON response as a Python dict representing a match object.

    Raises:
        requests.HTTPError: If the response status is not 2xx.
        requests.RequestException: For network-related errors (timeouts, etc.).
        ValueError: If the response body is not valid JSON.
    """
    url = NEXTFIXTURE_URL_TMPL.format(team_id=team_id)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_next_opponent(team_id: str, fixture: dict) -> tuple[str, str, bool]:
    """
    Derive next opponent metadata from a nextfixture match payload.

    Given a match fixture that includes homeTeam and awayTeam, determine:
    - opponent team id
    - opponent display name (shortName preferred)
    - whether the input team is playing at home (is_home_next)

    Args:
        team_id: Team id for which we are extracting opponent information.
        fixture: nextfixture JSON (match object) for the given team.

    Returns:
        (opponent_id, opponent_name, is_home_next)

    Raises:
        RuntimeError: If the given team_id is not found in the fixture's homeTeam/awayTeam.
    """
    home = fixture.get("homeTeam", {})
    away = fixture.get("awayTeam", {})

    home_id = str(home.get("id"))
    away_id = str(away.get("id"))
    team_id = str(team_id)

    if team_id == home_id:
        return (
            str(away.get("id")),
            away.get("shortName") or away.get("name"),
            True,
        )

    if team_id == away_id:
        return (
            str(home.get("id")),
            home.get("shortName") or home.get("name"),
            False,
        )

    raise RuntimeError(f"Team {team_id} not found in nextfixture match.")


def add_next_opponents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich a standings DataFrame with next opponent metadata (Phase B).

    For each row (team) in the standings table, this function:
    - fetches the team's nextfixture match payload
    - extracts opponent id/name and home/away status
    - adds columns: next_opponent_id, next_opponent_name, is_home_next

    Args:
        df: Standings DataFrame from parse_standings() (PHASE_A_COLS).

    Returns:
        A copy of df with next opponent columns added.
        If df is empty, returns df unchanged.

    Raises:
        requests.HTTPError / requests.RequestException / ValueError:
            Propagated from fetch_next_fixture_json().
        RuntimeError:
            Propagated from extract_next_opponent() if fixture structure is unexpected.
    """
    if df.empty:
        return df

    next_ids = []
    next_names = []
    next_home_flags = []

    for team_id in df["team_id"]:
        fixture = fetch_next_fixture_json(team_id)
        opp_id, opp_name, is_home = extract_next_opponent(team_id, fixture)
        next_ids.append(opp_id)
        next_names.append(opp_name)
        next_home_flags.append(is_home)

    df = df.copy()
    df["next_opponent_id"] = next_ids
    df["next_opponent_name"] = next_names
    df["is_home_next"] = next_home_flags
    return df


def append_history(df_new: pd.DataFrame, history_csv: str):
    """
    Append a completed matchweek snapshot to the history CSV (append-only).

    Dedupe policy:
        - A matchweek is stored at most once.
        - If the history file already contains the matchweek number, the append is skipped.

    Args:
        df_new: Snapshot DataFrame to append (should contain COLS and 20 rows).
        history_csv: Path to the append-only CSV file.

    Returns:
        None
    """
    if df_new.empty:
        print("No rows to append.")
        return

    matchweek = df_new["matchweek"].iloc[0]

    # If history exists, dedupe by matchweek (append-only snapshots)
    if os.path.exists(history_csv):
        hist = pd.read_csv(history_csv)

        # last saved matchweek
        saved_matchweeks = set(hist["matchweek"])
        if matchweek in saved_matchweeks:
            print(f"Matchweek {matchweek} already recorded. Skipping append.")
            return

        df_new.to_csv(history_csv, mode="a", header=False, index=False)
        print(f"Appended {len(df_new)} rows for matchweek {matchweek}.")
    else:
        # first run: write header
        df_new.to_csv(history_csv, index=False)
        print(
            f"Created {history_csv} with {len(df_new)} rows for matchweek {matchweek}."
        )


def main():
    """
    Orchestrate standings snapshot creation and persistence.

    Flow:
        1) Parse standings (Phase A). If matchweek is incomplete, exit early.
        2) Enrich with next opponent data (Phase B).
        3) Append to history CSV if this matchweek is not already recorded.

    Returns:
        None
    """
    df_new = parse_standings(STANDINGS_URL)
    if df_new.empty:
        print("No completed matchweek snapshot available. Exiting.")
        return

    df_new = add_next_opponents(df_new)

    # Now the columns exist
    df_new = df_new[COLS]

    append_history(df_new, HISTORY_CSV)


if __name__ == "__main__":
    main()
