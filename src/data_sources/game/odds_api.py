"""Fetches NBA odds and scores from The Odds API, updates games and lines tables."""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests
from dotenv import load_dotenv
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker

from src.config import team_name_mapper
from src.database import get_engine
from src.database_orm import GamesTable, LinesTable
from src.utils.timezone import APP_TIMEZONE, get_current_time

load_dotenv()
ODDS_API_KEY = os.getenv("ODDS_API_KEY")

pd.set_option("display.max_columns", 100)

# Sportsbooks to aggregate for consensus line calculation
VALID_SPORTSBOOKS = [
    "barstool",
    "betonlineag",
    "betmgm",
    "betrivers",
    "betus",
    "bovada",
    "draftkings",
    "fanduel",
    "lowvig",
    "mybookieag",
    "pointsbetus",
    "superbook",
    "twinspires",
    "unibet_us",
    "williamhill_us",
    "wynnbet",
]


class OddsAPI:
    ODDS_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
    SCORES_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/scores/"

    def __init__(self, api_key):
        self.api_key = api_key
        self.database_engine = get_engine()

    @staticmethod
    def convert_timezone(original_time):
        # Original timezone (UTC)
        original_timezone = pytz.timezone("UTC")
        # Process
        localized_time = original_timezone.localize(
            original_time
        )  # Associate the given time with its time zone
        target_time = localized_time.astimezone(APP_TIMEZONE)

        return target_time

    def fetch_odds_data(self):
        if not self.api_key:
            print("Warning: ODDS_API_KEY not set, skipping odds fetch")
            return []

        # NOTE: The Odds API only supports authentication via URL query parameters.
        # This is their API design limitation - header auth is not supported.
        # Be cautious with network logging tools as the API key will be visible in URLs.
        params = {
            "apiKey": self.api_key,
            "regions": "us",
            "markets": "spreads",
            "oddsFormat": "american",
        }
        response = requests.get(self.ODDS_URL, params=params)
        if response.status_code != 200:
            print(f"Odds API error: HTTP {response.status_code} - {response.text[:200]}")
            return []
        return response.json()

    def process_odds_data(self, data):
        data_list = []
        current_datetime = get_current_time().replace(tzinfo=None)
        for game in data:
            game_data = {}
            utc_time = datetime.strptime(game["commence_time"], "%Y-%m-%dT%H:%M:%SZ")
            mountain_time = self.convert_timezone(utc_time).replace(tzinfo=None)

            game_data["game_id"] = (
                mountain_time.strftime("%Y%m%d")
                + team_name_mapper(game["home_team"])
                + team_name_mapper(game["away_team"])
            )

            game_data["game_datetime"] = mountain_time  # Keep as datetime object
            game_data["home_team"] = team_name_mapper(game["home_team"])
            game_data["away_team"] = team_name_mapper(game["away_team"])

            # Extract line and price data for each sportsbook
            for bookmaker in game["bookmakers"]:
                prefix = bookmaker["key"]
                if prefix in VALID_SPORTSBOOKS:
                    for outcome in bookmaker["markets"][0][
                        "outcomes"
                    ]:  # Assuming 'spreads' is always the first in 'markets'
                        if outcome["name"] == game["home_team"]:
                            game_data[f"{prefix}_home_line"] = outcome["point"]
                            game_data[f"{prefix}_home_line_price"] = outcome["price"]
                        else:
                            game_data[f"{prefix}_away_line"] = outcome["point"]
                            game_data[f"{prefix}_away_line_price"] = outcome["price"]

            game_data["odds_last_update"] = current_datetime  # Already a datetime object
            data_list.append(game_data)

        odds_df = pd.DataFrame(data_list) if data_list else pd.DataFrame()
        return odds_df

    def fetch_scores_data(self, get_past_games=False):
        if not self.api_key:
            print("Warning: ODDS_API_KEY not set, skipping scores fetch")
            return []

        # API key in params required by The Odds API (no header auth supported)
        params = {
            "apiKey": self.api_key,
        }
        if get_past_games:
            params["daysFrom"] = 3
        response = requests.get(self.SCORES_URL, params=params)
        if response.status_code != 200:
            print(f"Scores API error: HTTP {response.status_code} - {response.text[:200]}")
            return []
        return response.json()

    def process_scores_data(self, data):
        """
        Processes the scores data and returns a list of dictionaries.
        """
        data_list = []

        for game in data:
            game_data = {}

            original_time = datetime.strptime(game["commence_time"], "%Y-%m-%dT%H:%M:%SZ")
            mountain_time = self.convert_timezone(original_time).replace(tzinfo=None)

            game_data["game_id"] = (
                mountain_time.strftime("%Y%m%d")
                + team_name_mapper(game["home_team"])
                + team_name_mapper(game["away_team"])
            )

            game_data["game_datetime"] = mountain_time  # Keep as datetime object

            game_data["home_team"] = team_name_mapper(game["home_team"])
            game_data["away_team"] = team_name_mapper(game["away_team"])

            # Add game_completed column
            game_data["game_completed"] = game["completed"]

            # Add home_score and away_score columns
            # Match scores by team name since API doesn't guarantee order
            if game["scores"]:
                for score_entry in game["scores"]:
                    if score_entry["name"] == game["home_team"]:
                        game_data["home_score"] = int(score_entry["score"])
                    else:
                        game_data["away_score"] = int(score_entry["score"])
            else:
                game_data["home_score"] = None
                game_data["away_score"] = None

            if game["last_update"]:
                update_time = datetime.strptime(game["last_update"], "%Y-%m-%dT%H:%M:%SZ")
                game_data["scores_last_update"] = self.convert_timezone(update_time).replace(
                    tzinfo=None
                )
            else:
                game_data["scores_last_update"] = None

            data_list.append(game_data)

        scores_df = pd.DataFrame(data_list) if data_list else pd.DataFrame()
        return scores_df

    def merge_data(self, odds_df, scores_df):
        merge_columns = ["game_id", "game_datetime", "home_team", "away_team"]
        merged_df = pd.merge(odds_df, scores_df, on=merge_columns, how="outer")
        merged_df.replace({np.nan: None}, inplace=True)
        return merged_df

    def update_database(self, merged_df):
        self.update_games_table(merged_df)
        self.update_lines_table(merged_df)

    def update_games_table(self, merged_df):
        """
        Update games table using upsert logic.

        Odds API is a SECONDARY/enhancement source. Covers is primary for most fields.
        Uses SQLite INSERT ... ON CONFLICT DO UPDATE for safe concurrent writes.

        Fields updated on conflict (Odds API tracking fields only):
        - odds_last_update: When we last fetched odds
        - scores_last_update: When we last fetched scores

        Fields NOT touched (preserved from Covers - the primary source):
        - game_completed: Covers owns this (detected from postgamebox class)
        - open_line: Covers owns this (historic opening lines)
        - home_score, away_score: Covers owns these

        Special handling for game_datetime:
        - Odds API provides full datetime with actual game times
        - Covers only has times for pregame boxes (upcoming games)
        - Covers pipeline preserves Odds API times for completed games
        """
        table = GamesTable.__table__
        Session = sessionmaker(bind=self.database_engine)

        with Session() as session:
            for index, row in merged_df.iterrows():
                # Build insert data (for new games not yet in Covers)
                insert_data = {
                    "game_id": row["game_id"],
                    "game_datetime": row["game_datetime"],
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                }

                # Add optional fields if present and not null
                for col in [
                    "home_score",
                    "away_score",
                    "game_completed",
                    "scores_last_update",
                    "odds_last_update",
                ]:
                    if col in row and not pd.isnull(row[col]):
                        insert_data[col] = row[col]

                # Build update dict for ON CONFLICT - only update tracking fields
                # Covers owns: game_datetime, game_completed, open_line, scores
                update_data = {}

                # Only update our tracking timestamps
                if not pd.isnull(row.get("odds_last_update")):
                    update_data["odds_last_update"] = row["odds_last_update"]
                if not pd.isnull(row.get("scores_last_update")):
                    update_data["scores_last_update"] = row["scores_last_update"]

                # Execute upsert
                stmt = sqlite_insert(table).values(insert_data)
                if update_data:  # Only add ON CONFLICT if we have something to update
                    stmt = stmt.on_conflict_do_update(index_elements=["game_id"], set_=update_data)
                else:
                    stmt = stmt.on_conflict_do_nothing()
                session.execute(stmt)

            session.commit()

    def update_lines_table(self, merged_df):
        """
        Update simplified lines table with consensus lines.

        Calculates median line/price across all available sportsbooks.
        Uses upsert: first fetch sets open_line, subsequent fetches update current_line.
        """
        table = LinesTable.__table__
        Session = sessionmaker(bind=self.database_engine)

        with Session() as session:
            for index, row in merged_df.iterrows():
                if pd.isnull(row.get("odds_last_update")):
                    continue

                # Collect all available lines and prices
                home_lines = []
                home_prices = []
                for book in VALID_SPORTSBOOKS:
                    line_col = f"{book}_home_line"
                    price_col = f"{book}_home_line_price"
                    if line_col in row and not pd.isnull(row[line_col]):
                        home_lines.append(row[line_col])
                    if price_col in row and not pd.isnull(row[price_col]):
                        home_prices.append(row[price_col])

                # Skip if no lines available
                if not home_lines:
                    continue

                # Calculate consensus (median) line and price
                consensus_line = float(np.median(home_lines))
                consensus_price = float(np.median(home_prices)) if home_prices else -110.0

                # Build upsert data
                insert_data = {
                    "game_id": row["game_id"],
                    "open_line": consensus_line,
                    "open_line_price": consensus_price,
                    "current_line": consensus_line,
                    "current_line_price": consensus_price,
                    "line_last_update": row["odds_last_update"],
                }

                # On conflict: only update current_line (preserve open_line)
                update_data = {
                    "current_line": consensus_line,
                    "current_line_price": consensus_price,
                    "line_last_update": row["odds_last_update"],
                }

                stmt = sqlite_insert(table).values(insert_data)
                stmt = stmt.on_conflict_do_update(index_elements=["game_id"], set_=update_data)
                session.execute(stmt)

            session.commit()


def update_game_data(past_games=False):
    """Fetch and update NBA game data from The Odds API."""
    odds_api = OddsAPI(ODDS_API_KEY)
    odds_data = odds_api.fetch_odds_data()
    processed_odds_data = odds_api.process_odds_data(odds_data)
    scores_data = odds_api.fetch_scores_data(get_past_games=past_games)
    processed_scores_data = odds_api.process_scores_data(scores_data)
    merged_data = odds_api.merge_data(processed_odds_data, processed_scores_data)
    odds_api.update_database(merged_data)


if __name__ == "__main__":
    update_game_data(past_games=True)
    # pass
