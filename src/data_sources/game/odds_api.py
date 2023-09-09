import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../../.."))
sys.path.append(os.path.join(here, "../.."))
from config import team_name_mapper
from database_orm import GamesTable, LinesTable

load_dotenv()
ODDS_API_KEY = os.getenv("ODDS_API_KEY")
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

pd.set_option("display.max_columns", 100)


class OddsAPI:
    ODDS_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds/"
    SCORES_URL = "https://api.the-odds-api.com/v4/sports/basketball_nba/scores/"

    def __init__(self, api_key, endpoint, password):
        self.api_key = api_key
        self.database_engine = create_engine(
            f"postgresql+psycopg2://postgres:{password}@{endpoint}/nba_betting"
        )

    def fetch_odds_data(self):
        params = {
            "apiKey": self.api_key,
            "regions": "us",
            "markets": "spreads",
            "oddsFormat": "american",
        }
        response = requests.get(self.ODDS_URL, params=params)
        return response.json() if response.status_code == 200 else []

    def process_odds_data(self, data):
        data_list = []
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for game in data:
            game_data = {}
            game_data["game_id"] = (
                datetime.strptime(game["commence_time"], "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y%m%d"
                )
                + team_name_mapper(game["home_team"])
                + team_name_mapper(game["away_team"])
            )

            game_data["game_datetime"] = datetime.strptime(
                game["commence_time"], "%Y-%m-%dT%H:%M:%SZ"
            ).strftime("%Y-%m-%d %H:%M:%S")
            game_data["home_team"] = team_name_mapper(game["home_team"])
            game_data["away_team"] = team_name_mapper(game["away_team"])

            # Extract line and price data for each sportsbook
            valid_bookmakers = [
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

            for bookmaker in game["bookmakers"]:
                prefix = bookmaker["key"]
                if prefix in valid_bookmakers:
                    for outcome in bookmaker["markets"][0][
                        "outcomes"
                    ]:  # Assuming 'spreads' is always the first in 'markets'
                        if outcome["name"] == game["home_team"]:
                            game_data[f"{prefix}_home_line"] = outcome["point"]
                            game_data[f"{prefix}_home_line_price"] = outcome["price"]
                        else:
                            game_data[f"{prefix}_away_line"] = outcome["point"]
                            game_data[f"{prefix}_away_line_price"] = outcome["price"]

            game_data["odds_last_update"] = current_datetime
            data_list.append(game_data)
            odds_df = pd.DataFrame(data_list)

        return odds_df

    def fetch_scores_data(self, get_past_games=False):
        params = {
            "apiKey": self.api_key,
        }
        if get_past_games:
            params["daysFrom"] = 3
        response = requests.get(self.SCORES_URL, params=params)
        return response.json() if response.status_code == 200 else []

    def process_scores_data(self, data):
        """
        Processes the scores data and returns a list of dictionaries.
        """
        data_list = []

        for game in data:
            game_data = {}

            game_data["game_id"] = (
                datetime.strptime(game["commence_time"], "%Y-%m-%dT%H:%M:%SZ").strftime(
                    "%Y%m%d"
                )
                + team_name_mapper(game["home_team"])
                + team_name_mapper(game["away_team"])
            )

            game_data["game_datetime"] = datetime.strptime(
                game["commence_time"], "%Y-%m-%dT%H:%M:%SZ"
            ).strftime("%Y-%m-%d %H:%M:%S")

            game_data["home_team"] = team_name_mapper(game["home_team"])
            game_data["away_team"] = team_name_mapper(game["away_team"])

            # Add game_completed column
            game_data["game_completed"] = game["completed"]

            # Add home_score and away_score columns
            if (
                game["scores"]
                and "home_score" in game["scores"]
                and "away_score" in game["scores"]
            ):
                game_data["home_score"] = game["scores"]["home_score"]
                game_data["away_score"] = game["scores"]["away_score"]
            else:
                game_data["home_score"] = None
                game_data["away_score"] = None

            if game["last_update"]:
                game_data["scores_last_update"] = datetime.strptime(
                    game["last_update"], "%Y-%m-%dT%H:%M:%SZ"
                ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                game_data["scores_last_update"] = None

            data_list.append(game_data)
            scores_df = pd.DataFrame(data_list)

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
        Session = sessionmaker(bind=self.database_engine)
        with Session() as session:
            for index, row in merged_df.iterrows():
                # Query for existing game
                game = (
                    session.query(GamesTable)
                    .filter(GamesTable.game_id == row["game_id"])
                    .first()
                )

                # Columns to be added or updated in GamesTable
                columns_to_include = [
                    "game_id",
                    "game_datetime",
                    "home_team",
                    "away_team",
                    "home_score",
                    "away_score",
                    "game_completed",
                    "scores_last_update",
                    "odds_last_update",
                ]

                # If game does not exist, add new record
                if not game:
                    # Create a new game dictionary using only the specified columns
                    game_data = {
                        col: row[col] for col in columns_to_include if col in row
                    }
                    new_game = GamesTable(**game_data)
                    session.add(new_game)

                # If game exists, update the specified columns
                else:
                    # Always update game_datetime
                    setattr(game, "game_datetime", row["game_datetime"])

                    # Update odds_last_update if not null
                    if not pd.isnull(row["odds_last_update"]):
                        setattr(game, "odds_last_update", row["odds_last_update"])

                    # If scores_last_updated is not null, update score-related columns
                    for col in [
                        "scores_last_update",
                        "game_completed",
                        "home_score",
                        "away_score",
                    ]:
                        if col in row and not pd.isnull(row[col]):
                            setattr(game, col, row[col])

            session.commit()

    def update_lines_table(self, merged_df):
        Session = sessionmaker(bind=self.database_engine)
        with Session() as session:
            for index, row in merged_df.iterrows():
                # If odds_last_updated is not null, add a new record to LinesTable
                if not pd.isnull(row["odds_last_update"]):
                    line_data = {
                        "game_id": row["game_id"],
                        "line_datetime": row["odds_last_update"],
                    }
                    odds_columns = [
                        # Barstool Sportsbook
                        "barstool_home_line",
                        "barstool_home_line_price",
                        "barstool_away_line",
                        "barstool_away_line_price",
                        # BetOnline.ag
                        "betonlineag_home_line",
                        "betonlineag_home_line_price",
                        "betonlineag_away_line",
                        "betonlineag_away_line_price",
                        # BetMGM
                        "betmgm_home_line",
                        "betmgm_home_line_price",
                        "betmgm_away_line",
                        "betmgm_away_line_price",
                        # BetRivers
                        "betrivers_home_line",
                        "betrivers_home_line_price",
                        "betrivers_away_line",
                        "betrivers_away_line_price",
                        # BetUS
                        "betus_home_line",
                        "betus_home_line_price",
                        "betus_away_line",
                        "betus_away_line_price",
                        # Bovada
                        "bovada_home_line",
                        "bovada_home_line_price",
                        "bovada_away_line",
                        "bovada_away_line_price",
                        # DraftKings
                        "draftkings_home_line",
                        "draftkings_home_line_price",
                        "draftkings_away_line",
                        "draftkings_away_line_price",
                        # FanDuel
                        "fanduel_home_line",
                        "fanduel_home_line_price",
                        "fanduel_away_line",
                        "fanduel_away_line_price",
                        # LowVig.ag
                        "lowvig_home_line",
                        "lowvig_home_line_price",
                        "lowvig_away_line",
                        "lowvig_away_line_price",
                        # MyBookie.ag
                        "mybookieag_home_line",
                        "mybookieag_home_line_price",
                        "mybookieag_away_line",
                        "mybookieag_away_line_price",
                        # PointsBet (US)
                        "pointsbetus_home_line",
                        "pointsbetus_home_line_price",
                        "pointsbetus_away_line",
                        "pointsbetus_away_line_price",
                        # SuperBook
                        "superbook_home_line",
                        "superbook_home_line_price",
                        "superbook_away_line",
                        "superbook_away_line_price",
                        # TwinSpires
                        "twinspires_home_line",
                        "twinspires_home_line_price",
                        "twinspires_away_line",
                        "twinspires_away_line_price",
                        # Unibet
                        "unibet_us_home_line",
                        "unibet_us_home_line_price",
                        "unibet_us_away_line",
                        "unibet_us_away_line_price",
                        # William Hill (Caesars)
                        "williamhill_us_home_line",
                        "williamhill_us_home_line_price",
                        "williamhill_us_away_line",
                        "williamhill_us_away_line_price",
                        # WynnBET
                        "wynnbet_home_line",
                        "wynnbet_home_line_price",
                        "wynnbet_away_line",
                        "wynnbet_away_line_price",
                    ]
                    for col in odds_columns:
                        if col in row and not pd.isnull(row[col]):
                            line_data[col] = row[col]
                        else:
                            line_data[col] = None

                    new_line = LinesTable(**line_data)
                    session.add(new_line)

            session.commit()


def update_game_data(past_games):
    odds_api = OddsAPI(ODDS_API_KEY, RDS_ENDPOINT, RDS_PASSWORD)
    odds_data = odds_api.fetch_odds_data()
    processed_odds_data = odds_api.process_odds_data(odds_data)
    scores_data = odds_api.fetch_scores_data(get_past_games=past_games)
    processed_scores_data = odds_api.process_scores_data(scores_data)
    merged_data = odds_api.merge_data(processed_odds_data, processed_scores_data)
    # print(merged_data)
    odds_api.update_database(merged_data)


if __name__ == "__main__":
    pass
