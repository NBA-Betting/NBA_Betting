import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine


def create_record_batch(date, engine, team_map):
    """Collects current days pregame odds data from Covers.
       Cleans and combines with feature data from previous day (End of Day).
       Saves as a new record to RDS database combined_data_inbound table.
       Typically run as part of a daily cron job.

    Args:
        date (str): Date to create records for. Almost always current date.
                    Format: YYYYMMDD, %Y%m%d Examples: '20211130' or '20210702'
        engine (sqlalchemy engine object): Connected to nba_betting database.
        team_map (dict): For standardizing team name and abbreviation fields.
    """
    with engine.connect() as connection:
        # Getting correct date format for querying db tables.
        prev_date = (
            datetime.datetime.strptime(date, "%Y%m%d")
            - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")

        # Loading relevent data from RDS.
        cd_covers_odds = pd.read_sql(
            f"SELECT * FROM covers_odds WHERE date = '{date}'", connection
        )
        pd_BR_standings = pd.read_sql(
            f"SELECT * FROM basketball_reference_standings WHERE date = '{prev_date}'",
            connection,
        )
        pd_BR_team_stats = pd.read_sql(
            f"SELECT * FROM basketball_reference_team_stats WHERE date = '{prev_date}'",
            connection,
        )
        pd_BR_opponent_stats = pd.read_sql(
            f"SELECT * FROM basketball_reference_opponent_stats WHERE date = '{prev_date}'",
            connection,
        )

        # Standardize team names using team map argument.
        cd_covers_odds["home_teamname"] = cd_covers_odds[
            "home_team_short_name"
        ].map(team_map)
        cd_covers_odds["away_teamname"] = cd_covers_odds[
            "away_team_short_name"
        ].map(team_map)
        pd_BR_standings["teamname"] = pd_BR_standings["team"].map(team_map)
        pd_BR_team_stats["teamname"] = pd_BR_team_stats["team"].map(team_map)
        pd_BR_opponent_stats["teamname"] = pd_BR_opponent_stats["team"].map(
            team_map
        )

        # Combining data into one dataframe.
        full_dataset = cd_covers_odds.merge(
            pd_BR_standings,
            how="left",
            left_on=["home_teamname"],
            right_on=["teamname"],
            suffixes=(None, "_sta"),
            validate="1:m",
        )
        full_dataset = full_dataset.merge(
            pd_BR_standings,
            how="left",
            left_on=["away_teamname"],
            right_on=["teamname"],
            suffixes=(None, "_osta"),
            validate="1:m",
        )
        full_dataset = full_dataset.merge(
            pd_BR_team_stats,
            how="left",
            left_on=["home_teamname"],
            right_on=["teamname"],
            suffixes=(None, "_ts"),
            validate="1:m",
        )
        full_dataset = full_dataset.merge(
            pd_BR_opponent_stats,
            how="left",
            left_on=["away_teamname"],
            right_on=["teamname"],
            suffixes=(None, "_os"),
            validate="1:m",
        )

        # Unique Record ID
        full_dataset["id"] = (
            full_dataset["date"]
            + full_dataset["home_teamname"]
            + full_dataset["away_teamname"]
        )

        # Datetime Fields
        full_dataset["datetime_str"] = (
            full_dataset["date"] + " " + full_dataset["time"]
        )
        full_dataset["datetime"] = full_dataset["datetime_str"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y%m%d %I:%M %p")
        )
        full_dataset["pred_datetime"] = full_dataset["date_sta"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y%m%d")
        )

        # Cleanup - Rename, Remove, and Reorder
        columns_to_keep = [
            "id",
            "datetime",
            "league_year",
            "home_teamname",
            "away_teamname",
            "link",
            "open_line_home",
            "open_line_away",
            "fanduel_line_home",
            "fanduel_line_price_home",
            "fanduel_line_away",
            "fanduel_line_price_away",
            "draftkings_line_home",
            "draftkings_line_price_home",
            "draftkings_line_away",
            "draftkings_line_price_away",
            "covers_home_consenses",
            "covers_away_consenses",
            "pred_datetime",
            "wins",
            "losses",
            "win_perc",
            "expected_wins",
            "expected_losses",
            "points_scored_per_game",
            "points_allowed_per_game",
            "wins_osta",
            "losses_osta",
            "win_perc_osta",
            "expected_wins_osta",
            "expected_losses_osta",
            "points_scored_per_game_osta",
            "points_allowed_per_game_osta",
            "g",
            "mp",
            "pts",
            "ast",
            "trb",
            "blk",
            "stl",
            "tov",
            "pf",
            "drb",
            "orb",
            "fg",
            "fga",
            "fg_pct",
            "fg2",
            "fg2a",
            "fg2_pct",
            "fg3",
            "fg3a",
            "fg3_pct",
            "ft",
            "fta",
            "ft_pct",
            "opp_g",
            "opp_mp",
            "opp_pts",
            "opp_ast",
            "opp_trb",
            "opp_blk",
            "opp_stl",
            "opp_tov",
            "opp_pf",
            "opp_drb",
            "opp_orb",
            "opp_fg",
            "opp_fga",
            "opp_fg_pct",
            "opp_fg2",
            "opp_fg2a",
            "opp_fg2_pct",
            "opp_fg3",
            "opp_fg3a",
            "opp_fg3_pct",
            "opp_ft",
            "opp_fta",
            "opp_ft_pct",
        ]
        full_dataset = full_dataset[columns_to_keep]
        column_rename_dict = {
            "id": "game_id",
            "datetime": "game_date",
            "league_year": "league_year",
            "home_teamname": "home_team",
            "away_teamname": "away_team",
            "link": "covers_game_url",
            "open_line_home": "open_line_home",
            "open_line_away": "open_line_away",
            "fanduel_line_home": "fd_line_home",
            "fanduel_line_price_home": "fd_line_price_home",
            "fanduel_line_away": "fd_line_away",
            "fanduel_line_price_away": "fd_line_price_away",
            "draftkings_line_home": "dk_line_home",
            "draftkings_line_price_home": "dk_line_price_home",
            "draftkings_line_away": "dk_line_away",
            "draftkings_line_price_away": "dk_line_price_away",
            "covers_home_consenses": "covers_consenses_home",
            "covers_away_consenses": "covers_consenses_away",
            "pred_datetime": "pred_date",
            "wins": "wins",
            "losses": "losses",
            "win_perc": "win_pct",
            "expected_wins": "expected_wins",
            "expected_losses": "expected_losses",
            "points_scored_per_game": "home_ppg",
            "points_allowed_per_game": "home_papg",
            "wins_osta": "away_wins",
            "losses_osta": "away_losses",
            "win_perc_osta": "away_win_pct",
            "expected_wins_osta": "away_expected_wins",
            "expected_losses_osta": "away_expected_losses",
            "points_scored_per_game_osta": "away_ppg",
            "points_allowed_per_game_osta": "away_papg",
            "g": "g",
            "mp": "mp",
            "pts": "pts",
            "ast": "ast",
            "trb": "trb",
            "blk": "blk",
            "stl": "stl",
            "tov": "tov",
            "pf": "pf",
            "drb": "drb",
            "orb": "orb",
            "fg": "fg",
            "fga": "fga",
            "fg_pct": "fg_pct",
            "fg2": "fg2",
            "fg2a": "fg2a",
            "fg2_pct": "fg2_pct",
            "fg3": "fg3",
            "fg3a": "fg3a",
            "fg3_pct": "fg3_pct",
            "ft": "ft",
            "fta": "fta",
            "ft_pct": "ft_pct",
            "opp_g": "away_g",
            "opp_mp": "away_mp",
            "opp_pts": "away_pts",
            "opp_ast": "away_ast",
            "opp_trb": "away_trb",
            "opp_blk": "away_blk",
            "opp_stl": "away_stl",
            "opp_tov": "away_tov",
            "opp_pf": "away_pf",
            "opp_drb": "away_drb",
            "opp_orb": "away_orb",
            "opp_fg": "away_fg",
            "opp_fga": "away_fga",
            "opp_fg_pct": "away_fg_pct",
            "opp_fg2": "away_fg2",
            "opp_fg2a": "away_fg2a",
            "opp_fg2_pct": "away_fg2_pct",
            "opp_fg3": "away_fg3",
            "opp_fg3a": "away_fg3a",
            "opp_fg3_pct": "away_fg3_pct",
            "opp_ft": "away_ft",
            "opp_fta": "away_fta",
            "opp_ft_pct": "away_fta_pct",
        }
        full_dataset = full_dataset.rename(columns=column_rename_dict)

        # Save to RDS
        full_dataset.to_sql(
            name="combined_data_inbound",
            con=connection,
            index=False,
            if_exists="append",
        )


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

    create_record_batch(todays_date_str, engine, team_map)
