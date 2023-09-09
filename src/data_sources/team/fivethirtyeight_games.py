import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "../.."))

from database_orm import FivethirtyeightGamesTable

load_dotenv()
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")


def update_all_data_538(engine):
    """Replaces Entire 538 Table
       Will add future games.

    Args:
        engine (): Database engine
    """
    url = "https://projects.fivethirtyeight.com/nba-model/nba_elo.csv"
    data = pd.read_csv(url)
    df = data.copy()

    df["season"] = df["season"].apply(lambda x: f"{x-1}-{x}")
    df["season_type"] = df["playoff"].apply(
        lambda x: "Playoffs" if pd.notnull(x) and x != "" else "Regular Season"
    )
    df.drop("playoff", axis=1, inplace=True)

    # define the columns to check for duplicates
    cols = ["date", "team1", "team2"]

    # mark duplicate records
    duplicates = df.duplicated(subset=cols, keep=False)

    # keep only the records that are not marked as duplicates
    df = df[~duplicates]

    with Session(engine) as session:
        # delete existing data
        session.query(FivethirtyeightGamesTable).delete()

        # add new data
        for index, row in df.iterrows():
            game = FivethirtyeightGamesTable(
                date=row["date"],
                season=row["season"],
                neutral=row["neutral"],
                season_type=row["season_type"],
                team1=row["team1"],
                team2=row["team2"],
                elo1_pre=row["elo1_pre"],
                elo2_pre=row["elo2_pre"],
                elo_prob1=row["elo_prob1"],
                elo_prob2=row["elo_prob2"],
                elo1_post=row["elo1_post"],
                elo2_post=row["elo2_post"],
                carm_elo1_pre=row["carm-elo1_pre"],
                carm_elo2_pre=row["carm-elo2_pre"],
                carm_elo_prob1=row["carm-elo_prob1"],
                carm_elo_prob2=row["carm-elo_prob2"],
                carm_elo1_post=row["carm-elo1_post"],
                carm_elo2_post=row["carm-elo2_post"],
                raptor1_pre=row["raptor1_pre"],
                raptor2_pre=row["raptor2_pre"],
                raptor_prob1=row["raptor_prob1"],
                raptor_prob2=row["raptor_prob2"],
                score1=row["score1"],
                score2=row["score2"],
                quality=row["quality"],
                importance=row["importance"],
                total_rating=row["total_rating"],
            )
            session.add(game)
        session.commit()

    print(df.info())


if __name__ == "__main__":
    try:
        username = "postgres"
        password = RDS_PASSWORD
        endpoint = RDS_ENDPOINT
        database = "nba_betting"
        engine = create_engine(
            f"postgresql+psycopg2://{username}:{password}@{endpoint}/{database}"
        )

        update_all_data_538(engine)
        print("-----538 Data Update Successful-----")
    except Exception as e:
        print("-----538 Data Update Failed-----")
        raise e
