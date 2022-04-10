import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine


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
        # Loading relevent data from RDS.
        pd_covers_game_results = pd.read_sql(
            f"SELECT * FROM covers_game_results WHERE date = '{date}'",
            connection,
        )

        # Standardize team names using team map argument.
        pd_covers_game_results["home_teamname"] = pd_covers_game_results[
            "home_team"
        ].map(team_map)
        pd_covers_game_results["away_teamname"] = pd_covers_game_results[
            "away_team"
        ].map(team_map)

        # Unique Record ID
        pd_covers_game_results["game_id"] = (
            pd_covers_game_results["date"]
            + pd_covers_game_results["home_teamname"]
            + pd_covers_game_results["away_teamname"]
        )

        # Save to RDS
        record_list_of_dicts = pd_covers_game_results.to_dict(orient="records")
        for game_result in record_list_of_dicts:
            game_id = game_result["game_id"]
            home_score = game_result["home_score"]
            away_score = game_result["away_score"]

            stmt = f"""
                UPDATE combined_data_inbound 
                SET home_score = {home_score}, 
                    away_score = {away_score} 
                WHERE game_id = '{game_id}'
                ;
                """

            connection.execute(stmt)


if __name__ == "__main__":
    username = "postgres"
    password = ""
    endpoint = ""
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

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
        "BKN": "BKN",
        "BOS": "BOS",
        "MIL": "MIL",
        "ATL": "ATL",
        "CHA": "CHA",
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

    team_map = dict(
        team_full_name_map.items()
        | team_abrv_map.items()
        | team_short_name_map.items()
    )

    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
    todays_date_str = todays_datetime.strftime("%Y%m%d")
    yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")

    update_previous_days_records(yesterdays_date_str, engine, team_map)
