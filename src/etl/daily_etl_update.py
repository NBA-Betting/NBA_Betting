import datetime
import sys

import pandas as pd
import pytz
from daily_etl_config import TEAM_MAP
from sqlalchemy import create_engine

sys.path.append("../../")
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
        covers["game_id"] = covers["date"] + covers["team"] + covers["opponent"]

        # Save to RDS
        record_list_of_dicts = covers.to_dict(orient="records")

        # print(record_list_of_dicts)

        for game_result in record_list_of_dicts:
            game_id = game_result["game_id"]
            home_score = game_result["score"]
            away_score = game_result["opponent_score"]
            home_result = game_result["result"]
            home_spread_result = game_result["spread_result"]

            stmt = f"""
                UPDATE combined_inbound_data
                SET home_score = {home_score},
                    away_score = {away_score},
                    home_result = '{home_result}',
                    home_spread_result = '{home_spread_result}'
                WHERE game_id = '{game_id}'
                ;
                """

            connection.execute(stmt)


if __name__ == "__main__":
    username = "postgres"
    password = RDS_PASSWORD
    endpoint = RDS_ENDPOINT
    database = "nba_betting"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
    )

    todays_datetime = datetime.datetime.now(pytz.timezone("America/Denver"))
    yesterdays_datetime = todays_datetime - datetime.timedelta(days=1)
    todays_date_str = todays_datetime.strftime("%Y%m%d")
    yesterdays_date_str = yesterdays_datetime.strftime("%Y%m%d")

    try:
        update_previous_days_records(yesterdays_date_str, engine, TEAM_MAP)
    except Exception as e:
        print(f"Error updating record for {yesterdays_date_str}.")
        print("----- DAILY ETL UPDATE STEP FAILED -----")
        raise e
    else:
        print("----- DAILY ETL UPDATE STEP SUCCESSFUL -----")
