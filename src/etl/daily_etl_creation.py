import datetime
import sys

import pandas as pd
import pytz
from daily_etl_config import TEAM_MAP
from sqlalchemy import create_engine, text

sys.path.append("../../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option("display.max_columns", None)


def load_data_from_rds(connection, game_date):
    """
    Loads relevant data from a relational database for a given date.

    Parameters:
    connection: A SQLAlchemy database connection.
    game_date (str): The date to load data for in the format 'YYYYMMDD'.

    Returns:
    A tuple of pandas dataframes containing the loaded data.
    """

    # Getting correct date format for querying db tables.
    previous_date = (
        datetime.datetime.strptime(game_date, "%Y%m%d") - datetime.timedelta(days=1)
    ).strftime("%Y%m%d")

    todays_date_538 = datetime.datetime.strptime(game_date, "%Y%m%d").strftime(
        "%Y-%m-%d"
    )

    # Covers
    covers_query = text("SELECT * FROM covers WHERE date = :date")
    covers = pd.read_sql(covers_query, connection, params={"date": game_date})

    # FiveThirtyEight
    elo_538_query = text(
        """
        SELECT date,
            team1,
            team2,
            elo1_pre,
            elo2_pre,
            elo_prob1,
            elo_prob2,
            raptor1_pre,
            raptor2_pre,
            raptor_prob1,
            raptor_prob2,
            quality,
            importance,
            total_rating AS total_rating_538
        FROM five_thirty_eight
        WHERE date = :date
    """
    )
    elo_538 = pd.read_sql(elo_538_query, connection, params={"date": todays_date_538})

    # NBA Stats
    traditional_query = text("SELECT * FROM nbastats_traditional WHERE date = :date")
    traditional = pd.read_sql(
        traditional_query, connection, params={"date": previous_date}
    )
    advanced_query = text("SELECT * FROM nbastats_advanced WHERE date = :date")
    advanced = pd.read_sql(advanced_query, connection, params={"date": previous_date})
    four_factors_query = text("SELECT * FROM nbastats_four_factors WHERE date = :date")
    four_factors = pd.read_sql(
        four_factors_query, connection, params={"date": previous_date}
    )
    misc_query = text("SELECT * FROM nbastats_misc WHERE date = :date")
    misc = pd.read_sql(misc_query, connection, params={"date": previous_date})
    scoring_query = text("SELECT * FROM nbastats_scoring WHERE date = :date")
    scoring = pd.read_sql(scoring_query, connection, params={"date": previous_date})
    opponent_query = text("SELECT * FROM nbastats_opponent WHERE date = :date")
    opponent = pd.read_sql(opponent_query, connection, params={"date": previous_date})
    speed_distance_query = text(
        "SELECT * FROM nbastats_speed_distance WHERE date = :date"
    )
    speed_distance = pd.read_sql(
        speed_distance_query, connection, params={"date": previous_date}
    )
    shooting_query = text("SELECT * FROM nbastats_shooting WHERE date = :date")
    shooting = pd.read_sql(shooting_query, connection, params={"date": previous_date})
    opponent_shooting_query = text(
        "SELECT * FROM nbastats_opponent_shooting WHERE date = :date"
    )
    opponent_shooting = pd.read_sql(
        opponent_shooting_query, connection, params={"date": previous_date}
    )
    hustle_query = text("SELECT * FROM nbastats_hustle WHERE date = :date")
    hustle = pd.read_sql(hustle_query, connection, params={"date": previous_date})

    return (
        covers,
        elo_538,
        traditional,
        advanced,
        four_factors,
        misc,
        scoring,
        opponent,
        speed_distance,
        shooting,
        opponent_shooting,
        hustle,
    )


def standardize_team_names(
    team_map,
    covers,
    elo_538,
    traditional,
    advanced,
    four_factors,
    misc,
    scoring,
    opponent,
    speed_distance,
    shooting,
    opponent_shooting,
    hustle,
):
    """
    Standardizes team names in a set of pandas dataframes using a dictionary mapping team names to team IDs.

    Parameters:
        team_map (dict): A dictionary mapping team names to team IDs.
        covers (pd.DataFrame): Covers dataset.
        elo_538 (pd.DataFrame): FiveThirtyEight ELO dataset.
        traditional, advanced, four_factors, misc, scoring, opponent, speed_distance, shooting, opponent_shooting, hustle (pd.DataFrame): NBAStats datasets.

    Returns:
        Tuple of pandas dataframes: Covers, FiveThirtyEight, and NBAStats datasets with standardized team names."""
    # Covers
    covers["team"] = covers["team"].map(team_map)
    covers["opponent"] = covers["opponent"].map(team_map)

    # FiveThirtyEight
    elo_538["team"] = elo_538["team1"].map(team_map)
    elo_538["opponent"] = elo_538["team2"].map(team_map)

    # NBA Stats
    for df in [
        traditional,
        advanced,
        four_factors,
        misc,
        scoring,
        opponent,
        speed_distance,
        shooting,
        opponent_shooting,
        hustle,
    ]:
        df["team"] = df["team"].map(team_map)

    return (
        covers,
        elo_538,
        traditional,
        advanced,
        four_factors,
        misc,
        scoring,
        opponent,
        speed_distance,
        shooting,
        opponent_shooting,
        hustle,
    )


def combine_datasets(covers, elo_538, *nbastats_tables):
    """
    Combine multiple datasets into a single dataframe.

    Parameters:
        covers (pd.DataFrame): Covers dataset.
        elo_538 (pd.DataFrame): FiveThirtyEight ELO dataset.
        *nbastats_tables (pd.DataFrame): List of NBA statistics datasets.

    Returns:
        pd.DataFrame: Combined dataset.
    """

    # Make a copy of the Covers dataset
    full_dataset = covers.copy()

    # Merge each statistics group with the dataset
    for stat_group in [*nbastats_tables]:
        full_dataset = full_dataset.merge(
            stat_group,
            how="left",
            left_on=["team"],
            right_on=["team"],
            suffixes=(None, "_nba"),
            validate="1:1",
        )
        full_dataset = full_dataset.merge(
            stat_group,
            how="left",
            left_on=["opponent"],
            right_on=["team"],
            suffixes=(None, "_opp"),
            validate="1:1",
        )

    # Merge with the FiveThirtyEight ELO dataset
    full_dataset = full_dataset.merge(
        elo_538,
        how="left",
        left_on=["date", "team", "opponent"],
        right_on=["date", "team", "opponent"],
        suffixes=(None, "_538"),
        validate="1:1",
    )

    return full_dataset


def complete_dataset(full_dataset):
    # Unique Record ID
    full_dataset["game_id"] = (
        full_dataset["date"] + full_dataset["team"] + full_dataset["opponent"]
    )

    # Datetime Fields
    full_dataset["datetime_str"] = full_dataset.apply(
        lambda x: x["date"] + " " + x["time"] if pd.notnull(x["time"]) else x["date"],
        axis=1,
    )
    full_dataset["datetime"] = full_dataset["datetime_str"].apply(
        lambda x: pd.to_datetime(x)
    )
    full_dataset["pred_date"] = full_dataset["date"].apply(
        lambda x: (
            datetime.datetime.strptime(x, "%Y%m%d") - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")
    )

    return full_dataset


def cleanup_dataset(full_dataset):
    # Define the columns to keep in the output DataFrame
    main_features = [
        "game_id",
        "datetime",
        "league_year",
        "team",
        "opponent",
        "game_url",
        "spread",
        "fanduel_line_home",
        "fanduel_line_price_home",
        "fanduel_line_away",
        "fanduel_line_price_away",
        "draftkings_line_home",
        "draftkings_line_price_home",
        "draftkings_line_away",
        "draftkings_line_price_away",
        "covers_home_consensus",
        "covers_away_consensus",
        "pred_date",
    ]

    # Define the columns to drop from the input DataFrame
    drop_features = [
        "id_num",
        "date",
        "time",
        "home_team_short_name",
        "away_team_short_name",
        "date_nba",
        "date_opp",
        "team_opp",
        "datetime_str",
        "team1",
        "team2",
    ]

    # Reorder the columns in the output DataFrame
    all_features = main_features + [
        i for i in list(full_dataset) if i not in (drop_features + main_features)
    ]
    output_df = full_dataset[all_features]

    # Rename some of the columns in the output DataFrame
    column_rename_dict = {
        "datetime": "game_date",
        "team": "home_team",
        "opponent": "away_team",
        "game_url": "covers_game_url",
        "spread": "home_spread",
        "score": "home_score",
        "opponent_score": "away_score",
        "result": "home_result",
        "spread_result": "home_spread_result",
        "fanduel_line_home": "fd_line_home",
        "fanduel_line_price_home": "fd_line_price_home",
        "fanduel_line_away": "fd_line_away",
        "fanduel_line_price_away": "fd_line_price_away",
        "draftkings_line_home": "dk_line_home",
        "draftkings_line_price_home": "dk_line_price_home",
        "draftkings_line_away": "dk_line_away",
        "draftkings_line_price_away": "dk_line_price_away",
        "covers_home_consensus": "covers_consensus_home",
        "covers_away_consensus": "covers_consensus_away",
    }
    output_df = output_df.rename(columns=column_rename_dict)

    return output_df


def create_record_batch(game_date, engine, team_map):
    """
    Collects pregame odds and feature data for a given date from various data sources.
    The collected data is then standardized, combined, and cleaned before being saved.
    The resulting dataset is saved as a new record in the 'combined_inbound_data' table.

    Args:
        game_date (str): The date for which data will be collected.
                         Format: 'YYYYMMDD', e.g. '20220101'
        engine (sqlalchemy.engine.Engine): An engine object connected to a SQL database.
        team_map (Dict[str, str]): A dictionary containing mappings between team names and abbreviations.

    Returns:
        None: If no games are available for the given date or if there are errors during the data processing and saving.

    Raises:
        ValueError: If the input date is not in the correct format.

    """
    with engine.connect() as connection:

        # ----- CHECK IF GAMES AVAILABLE FOR DATE -----

        game_count_query = text("SELECT COUNT(*) FROM covers WHERE date = :date")
        game_count = connection.execute(game_count_query, date=game_date).fetchone()[0]
        if game_count == 0:
            print(f"No games available for {game_date}.")
            return

        # ----- LOAD DATA FROM RDS -----

        (
            covers,
            elo_538,
            traditional,
            advanced,
            four_factors,
            misc,
            scoring,
            opponent,
            speed_distance,
            shooting,
            opponent_shooting,
            hustle,
        ) = load_data_from_rds(connection, game_date)

        # ----- STANDARDIZE TEAM NAMES -----

        (
            covers,
            elo_538,
            traditional,
            advanced,
            four_factors,
            misc,
            scoring,
            opponent,
            speed_distance,
            shooting,
            opponent_shooting,
            hustle,
        ) = standardize_team_names(
            team_map,
            covers,
            elo_538,
            traditional,
            advanced,
            four_factors,
            misc,
            scoring,
            opponent,
            speed_distance,
            shooting,
            opponent_shooting,
            hustle,
        )

        # ----- STANDARDIZE DATES -----

        # FiveThirtyEight
        elo_538["date"] = elo_538["date"].apply(lambda x: x.replace("-", ""))

        # ----- COMBINE DATA -----

        full_dataset = combine_datasets(
            covers,
            elo_538,
            traditional,
            advanced,
            four_factors,
            misc,
            scoring,
            opponent,
            speed_distance,
            shooting,
            opponent_shooting,
            hustle,
        )

        # ----- FINALIZE FULL DATASET -----

        full_dataset = complete_dataset(full_dataset)
        full_dataset = cleanup_dataset(full_dataset)

        # ----- VERIFY AND SAVE TO RDS -----

        # print(full_dataset.info(verbose=True, show_counts=True))
        # print(full_dataset.head())

        # Save to RDS
        full_dataset.to_sql(
            name="combined_inbound_data",
            con=connection,
            index=False,
            if_exists="append",
        )


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

    # Daily Updating
    # try:
    #     create_record_batch(todays_date_str, engine, TEAM_MAP)
    # except Exception as e:
    #     print(f"Error creating record for {todays_date_str}.")
    #     print("----- DAILY ETL CREATION STEP FAILED -----")
    #     raise e
    # else:
    #     print("----- DAILY ETL CREATION STEP SUCCESSFUL -----")

    # Specific Dates
    # dates = ["20230307"]
    # failed_dates = []
    # for date in dates:
    #     try:
    #         create_record_batch(date, engine, TEAM_MAP)
    #     except Exception as e:
    #         print(f"Error creating record for {date}.\nError: {e}")
    #         failed_dates.append(date)
    # if len(failed_dates) == 0:
    #     print("----- ALL UPDATES SUCCESSFUL -----")
    # else:
    #     print(f"----- {len(failed_dates)} out of {len(dates)} UPDATES FAILED -----")

    # Full Table Reset
    # min_date = "20141028"

    # min_datetime = datetime.datetime.strptime(min_date, "%Y%m%d")
    # todays_datetime = datetime.datetime.now()
    # delta = todays_datetime - min_datetime

    # failed_dates = []
    # for i in range(delta.days + 1):
    #     date = (min_datetime + datetime.timedelta(days=i)).strftime("%Y%m%d")
    #     try:
    #         create_record_batch(date, engine, TEAM_MAP)
    #         print(f"{date} updated.")
    #     except Exception as e:
    #         print(f"Error creating record for {date}.\nError: {e}")
    #         failed_dates.append(date)

    # if len(failed_dates) == 0:
    #     print("----- ALL UPDATES SUCCESSFUL -----")
    # else:
    #     print(
    #         f"----- {delta.days + 1 - len(failed_dates)} UPDATES SUCCESSFUL ----- {len(failed_dates)} UPDATES FAILED -----"
    #     )
