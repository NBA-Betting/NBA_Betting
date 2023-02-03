import datetime
import sys

import pandas as pd
import pytz
from sqlalchemy import create_engine

sys.path.append("../../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option("display.max_columns", 100)
pd.options.display.width = 0


def update_all_data_538(connection):
    """Replaces Entire 538 Table
       !!! Will add future games that may
       need to be deleted.

    Args:
        connection (): Database connection
    """
    url = "https://projects.fivethirtyeight.com/nba-model/nba_elo.csv"
    data = pd.read_csv(url)
    df = data.copy()
    print(df.info())
    print(df.head())
    df.to_sql("five_thirty_eight", connection, index=False, if_exists="replace")


def daily_update_538(connection, dates_to_update):
    """Appends 538 data for specified dates to the PostgreSQL database.

    Args:
        connection (sqlalchemy.engine.base.Connection): SQLAlchemy connection to the database.
        dates_to_update (list of str): Dates to be downloaded and appended.

    Returns:
        None
    """
    url = "https://projects.fivethirtyeight.com/nba-model/nba_elo.csv"
    df = pd.read_csv(url)
    df = df[df["date"].isin(dates_to_update)]
    df.to_sql(
        "five_thirty_eight", connection, index=False, if_exists="append", method="multi"
    )


if __name__ == "__main__":
    try:
        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )

        today = datetime.datetime.now(pytz.timezone("America/Denver"))
        yesterday = today - datetime.timedelta(days=1)
        dates = [today.strftime("%Y-%m-%d")]
        # dates = ['2022-10-26', '2022-10-27', '2022-10-28']
        # dates = pd.date_range(start=season_dates["start_date"],
        #                       end=season_dates["final_date"],
        #                       freq='D')

        with engine.connect() as connection:
            # update_all_data_538(connection)
            daily_update_538(connection, dates)
            print("-----538 Data Update Successful-----")
    except Exception as e:
        print("-----538 Data Update Failed-----")
        raise e
