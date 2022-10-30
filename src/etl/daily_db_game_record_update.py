import sys
import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../../')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD


def update_previous_days_records(date, engine, team_map):
    """Collects game results from previous day and
       adds them to previous days game records.
       Typically run as part of a daily cron job.

    Args:
        date (str): Date to update records for. Almost always yesterday's date.
                    Format: YYYYMMDD, %Y%m%d Examples: '20211130' or '20210702'
        engine (sqlalchemy engine object): Connected to nba_betting database.
        team_map (dict): For standardizing team name and abbreviation fields.
    """
    with engine.connect() as connection:
        covers = pd.read_sql(
            f"SELECT * FROM covers WHERE date = '{date}'",
            connection,
        )

        # Standardize Team Names
        covers["team"] = covers["team"].map(team_map)
        covers["opponent"] = covers["opponent"].map(team_map)

        # Unique Record ID
        covers["game_id"] = (covers["date"] + covers["team"] +
                             covers["opponent"])

        # Save to RDS
        record_list_of_dicts = covers.to_dict(orient="records")

        print(record_list_of_dicts)

        # for game_result in record_list_of_dicts:
        #     game_id = game_result["game_id"]
        #     home_score = game_result["score"]
        #     away_score = game_result["opponent_score"]
        #     home_result = game_result["result"]
        #     home_spread_result = game_result["spread_result"]

        #     stmt = f"""
        #         UPDATE combined_inbound_data
        #         SET home_score = {home_score},
        #             away_score = {away_score},
        #             home_result = '{home_result}',
        #             home_spread_result = '{home_spread_result}'
        #         WHERE game_id = '{game_id}'
        #         ;
        #         """

        #     connection.execute(stmt)


if __name__ == "__main__":
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}")

    team_full_name_map = {
        "Washington Wizards": "WAS",
        "Brooklyn Nets": "BKN",
        "Chicago Bulls": "CHI",
        "Miami Heat": "MIA",
        "Cleveland Cavaliers": "CLE",
        "Philadelphia 76ers": "PHI",
        "New York Knicks": "NYK",
        "Charlotte Hornets": "CHA",
        "Boston Celtics": "BOS",
        "Toronto Raptors": "TOR",
        "Milwaukee Bucks": "MIL",
        "Atlanta Hawks": "ATL",
        "Indiana Pacers": "IND",
        "Detroit Pistons": "DET",
        "Orlando Magic": "ORL",
        "Golden State Warriors": "GSW",
        "Phoenix Suns": "PHX",
        "Dallas Mavericks": "DAL",
        "Denver Nuggets": "DEN",
        "Los Angeles Clippers": "LAC",
        "LA Clippers": "LAC",
        "Utah Jazz": "UTA",
        "Los Angeles Lakers": "LAL",
        "Memphis Grizzlies": "MEM",
        "Portland Trail Blazers": "POR",
        "Sacramento Kings": "SAC",
        "Oklahoma City Thunder": "OKC",
        "Minnesota Timberwolves": "MIN",
        "San Antonio Spurs": "SAS",
        "New Orleans Pelicans": "NOP",
        "Houston Rockets": "HOU",
        "Charlotte Bobcats": "CHA",
        "New Orleans Hornets": "NOP",
        "New Jersey Nets": "BKN",
        "Seattle SuperSonics": "OKC",
        "New Orleans/Oklahoma City Hornets": "NOP",
    }

    team_abrv_map = {
        "BK": "BKN",
        "BRK": "BKN",
        "BKN": "BKN",
        "BOS": "BOS",
        "MIL": "MIL",
        "ATL": "ATL",
        "CHA": "CHA",
        "CHO": "CHA",
        "CHI": "CHI",
        "CLE": "CLE",
        "DAL": "DAL",
        "DEN": "DEN",
        "DET": "DET",
        "GS": "GSW",
        "GSW": "GSW",
        "HOU": "HOU",
        "IND": "IND",
        "LAC": "LAC",
        "LAL": "LAL",
        "MEM": "MEM",
        "MIA": "MIA",
        "MIN": "MIN",
        "NO": "NOP",
        "NOP": "NOP",
        "NY": "NYK",
        "NYK": "NYK",
        "OKC": "OKC",
        "ORL": "ORL",
        "PHI": "PHI",
        "PHO": "PHX",
        "PHX": "PHX",
        "POR": "POR",
        "SA": "SAS",
        "SAS": "SAS",
        "SAC": "SAC",
        "TOR": "TOR",
        "UTA": "UTA",
        "WAS": "WAS",
    }

    team_short_name_map = {
        "Nets": "BKN",
        "Celtics": "BOS",
        "Bucks": "MIL",
        "Hawks": "ATL",
        "Hornets": "CHA",
        "Bulls": "CHI",
        "Cavaliers": "CLE",
        "Mavericks": "DAL",
        "Nuggets": "DEN",
        "Pistons": "DET",
        "Warriors": "GSW",
        "Rockets": "HOU",
        "Pacers": "IND",
        "Clippers": "LAC",
        "Lakers": "LAL",
        "Grizzlies": "MEM",
        "Heat": "MIA",
        "Timberwolves": "MIN",
        "Pelicans": "NOP",
        "Knicks": "NYK",
        "Thunder": "OKC",
        "Magic": "ORL",
        "76ers": "PHI",
        "Suns": "PHX",
        "Trail Blazers": "POR",
        "Spurs": "SAS",
        "Kings": "SAC",
        "Raptors": "TOR",
        "Jazz": "UTA",
        "Wizards": "WAS",
    }

    team_map = dict(team_full_name_map.items()
                    | team_abrv_map.items()
                    | team_short_name_map.items())

    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
    todays_date_str = todays_datetime.strftime("%Y%m%d")
    yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")

    update_previous_days_records(yesterdays_date_str, engine, team_map)
