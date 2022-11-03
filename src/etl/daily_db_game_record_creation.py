import sys
import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine

sys.path.append('../../')
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

pd.set_option('display.max_columns', None)


def create_record_batch(date, engine, team_map):
    """Collects current days pregame odds data from Covers.
       Cleans and combines with feature data from previous day (End of Day).
       Saves as a new record to RDS database combined_inbound_data table.
       Typically run as part of a daily cron job.

    Args:
        date (str): Date to create records for. Almost always current date.
                    Format: YYYYMMDD, %Y%m%d Examples: '20211130' or '20210702'
        engine (sqlalchemy engine object): Connected to nba_betting database.
        team_map (dict): For standardizing team name and abbreviation fields.
    """
    with engine.connect() as connection:
        # Getting correct date format for querying db tables.
        prev_date = (datetime.datetime.strptime(date, "%Y%m%d") -
                     datetime.timedelta(days=1)).strftime("%Y%m%d")

        todays_date_538 = datetime.datetime.strptime(
            date, "%Y%m%d").strftime("%Y-%m-%d")

        # ----- Loading relevant data from RDS -----

        # Covers
        covers = pd.read_sql(f"SELECT * FROM covers WHERE date = '{date}'",
                             connection)

        # FiveThirtyEight
        elo_538 = pd.read_sql(
            f"""
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
            WHERE date = '{todays_date_538}'
            """, connection)

        # NBA Stats
        traditional = pd.read_sql(
            f"SELECT * FROM traditional WHERE date = '{prev_date}'",
            connection)
        advanced = pd.read_sql(
            f"SELECT * FROM advanced WHERE date = '{prev_date}'", connection)
        four_factors = pd.read_sql(
            f"SELECT * FROM four_factors WHERE date = '{prev_date}'",
            connection)
        misc = pd.read_sql(f"SELECT * FROM misc WHERE date = '{prev_date}'",
                           connection)
        scoring = pd.read_sql(
            f"SELECT * FROM scoring WHERE date = '{prev_date}'", connection)
        opponent = pd.read_sql(
            f"SELECT * FROM opponent WHERE date = '{prev_date}'", connection)
        speed_distance = pd.read_sql(
            f"SELECT * FROM speed_distance WHERE date = '{prev_date}'",
            connection)
        shooting = pd.read_sql(
            f"SELECT * FROM shooting WHERE date = '{prev_date}'", connection)
        opponent_shooting = pd.read_sql(
            f"SELECT * FROM opponent_shooting WHERE date = '{prev_date}'",
            connection)
        hustle = pd.read_sql(
            f"SELECT * FROM hustle WHERE date = '{prev_date}'", connection)

        # ----- STANDARDIZE TEAM NAMES -----

        # Covers
        covers["team"] = covers["team"].map(team_map)
        covers["opponent"] = covers["opponent"].map(team_map)

        # FiveThirtyEight
        elo_538['team'] = elo_538['team1'].map(team_map)
        elo_538['opponent'] = elo_538['team2'].map(team_map)

        # NBA Stats
        traditional['team'] = traditional['team'].map(team_map)
        advanced['team'] = advanced['team'].map(team_map)
        four_factors['team'] = four_factors['team'].map(team_map)
        misc['team'] = misc['team'].map(team_map)
        scoring['team'] = scoring['team'].map(team_map)
        opponent['team'] = opponent['team'].map(team_map)
        speed_distance['team'] = speed_distance['team'].map(team_map)
        shooting['team'] = shooting['team'].map(team_map)
        opponent_shooting['team'] = opponent_shooting['team'].map(team_map)
        hustle['team'] = hustle['team'].map(team_map)

        # ----- STANDARDIZE DATES -----

        # FiveThirtyEight
        elo_538['date'] = elo_538['date'].apply(lambda x: x.replace('-', ''))

        # ----- COMBINE DATA -----

        # Covers and NBA Stats
        full_dataset = covers.merge(
            traditional,
            how="left",
            left_on=["team"],
            right_on=["team"],
            suffixes=(None, "_nba"),
            validate="1:1",
        )
        full_dataset = full_dataset.merge(traditional,
                                          how='left',
                                          left_on=['opponent'],
                                          right_on=['team'],
                                          suffixes=(None, '_opp'),
                                          validate='1:1')

        for stat_group in [
                advanced, four_factors, misc, scoring, opponent,
                speed_distance, shooting, opponent_shooting, hustle
        ]:
            full_dataset = full_dataset.merge(stat_group,
                                              how='left',
                                              left_on=['team'],
                                              right_on=['team'],
                                              suffixes=(None, '_nba'),
                                              validate='1:1')
            full_dataset = full_dataset.merge(stat_group,
                                              how='left',
                                              left_on=['opponent'],
                                              right_on=['team'],
                                              suffixes=(None, '_opp'),
                                              validate='1:1')

        # FiveThirtyEight
        full_dataset = full_dataset.merge(
            elo_538,
            how='left',
            left_on=['date', 'team', 'opponent'],
            right_on=['date', 'team', 'opponent'],
            suffixes=(None, '_538'),
            validate='1:1')

        # ----- FULL DATASET -----

        # Unique Record ID
        full_dataset["game_id"] = (full_dataset["date"] +
                                   full_dataset["team"] +
                                   full_dataset["opponent"])

        # Datetime Fields
        full_dataset["datetime_str"] = full_dataset.apply(
            lambda x: x["date"] + " " + x["time"]
            if pd.notnull(x['time']) else x['date'],
            axis=1)
        full_dataset["datetime"] = full_dataset["datetime_str"].apply(
            lambda x: pd.to_datetime(x))
        full_dataset['pred_date'] = full_dataset['date'].apply(
            lambda x: (datetime.datetime.strptime(x, '%Y%m%d') - datetime.
                       timedelta(days=1)).strftime('%Y%m%d'))

        # Cleanup - Rename, Remove, and Reorder
        main_features = [
            "game_id", "datetime", "league_year", "team", "opponent",
            "game_url", "spread", "fanduel_line_home",
            "fanduel_line_price_home", "fanduel_line_away",
            "fanduel_line_price_away", "draftkings_line_home",
            "draftkings_line_price_home", "draftkings_line_away",
            "draftkings_line_price_away", "covers_home_consensus",
            "covers_away_consensus", "pred_date"
        ]

        drop_features = [
            'id_num', 'date', 'time', 'home_team_short_name',
            'away_team_short_name', 'date_nba', 'date_opp', 'team_opp',
            'datetime_str', 'team1', 'team2'
        ]

        all_features = main_features + [
            i for i in list(full_dataset)
            if i not in (drop_features + main_features)
        ]
        full_dataset = full_dataset[all_features]

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
        full_dataset = full_dataset.rename(columns=column_rename_dict)

        print(full_dataset.info(verbose=True, show_counts=True))
        print(full_dataset.head(10))

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

    create_record_batch(todays_date_str, engine, team_map)

    # dates = ['20221101', '20221102']

    # for date in dates:
    #     create_record_batch(date, engine, team_map)
