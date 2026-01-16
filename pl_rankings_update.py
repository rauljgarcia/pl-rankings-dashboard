import os
from datetime import datetime, timezone


import pandas as pd
import requests


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

COLS = PHASE_A_COLS + ["next_opponent_id", "next_opponent_name"]


def fetch_standings_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def is_matchweek_complete(matchweek: int) -> bool:
    url = MATCHWEEK_MATCHES_URL_TMPL.format(matchweek=matchweek)

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    j = r.json()

    matches = j.get("data", [])
    if not matches:
        return False

    if len(matches) != 10:  # 10 matches are required, else 20 teams didn't play
        return False
    periods = {m.get("period") for m in matches}

    # return will be a boolean on the whole set of periods in the json
    return periods == {"FullTime"}


def parse_standings(url: str) -> pd.DataFrame:

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
    url = NEXTFIXTURE_URL_TMPL.format(team_id=team_id)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_next_opponent(team_id: str, fixture: dict) -> tuple[str, str]:
    home = fixture.get("homeTeam", {})
    away = fixture.get("awayTeam", {})

    home_id = str(home.get("id"))
    away_id = str(away.get("id"))
    team_id = str(team_id)

    if team_id == home_id:
        return str(away.get("id")), away.get("shortName") or away.get("name")
    if team_id == away_id:
        return str(home.get("id")), home.get("shortName") or home.get("name")

    raise RuntimeError(f"Team {team_id} not found in nextfixture match.")


def add_next_opponents(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    next_ids = []
    next_names = []

    for team_id in df["team_id"]:
        fixture = fetch_next_fixture_json(team_id)
        opp_id, opp_name = extract_next_opponent(team_id, fixture)
        next_ids.append(opp_id)
        next_names.append(opp_name)

    df = df.copy()
    df["next_opponent_id"] = next_ids
    df["next_opponent_name"] = next_names
    return df


def append_history(df_new: pd.DataFrame, history_csv: str):

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
