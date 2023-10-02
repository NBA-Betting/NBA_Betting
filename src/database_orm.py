import os

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    PrimaryKeyConstraint,
    String,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

load_dotenv()
RDS_ENDPOINT = os.environ.get("RDS_ENDPOINT")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD")


Base = declarative_base()


class AllFeaturesJSONTable(Base):
    __tablename__ = "all_features_json"
    __table_args__ = (PrimaryKeyConstraint("game_id"),)
    game_id = Column(String)
    data = Column(JSONB)


class PredictionsTable(Base):
    __tablename__ = "predictions"
    __table_args__ = (PrimaryKeyConstraint("game_id", "prediction_datetime"),)
    game_id = Column(String)
    prediction_datetime = Column(DateTime)
    open_line_hv = Column(Float)
    prediction_line_hv = Column(Float)
    ml_cls_rating_hv = Column(Float)
    dl_cls_rating_hv = Column(Float)
    game_rating_hv = Column(Float)
    prediction_direction = Column(String)
    directional_game_rating = Column(Float)
    ml_reg_pred_1 = Column(Float)
    ml_reg_pred_2 = Column(Float)
    ml_cls_pred_1 = Column(Float)
    ml_cls_pred_2 = Column(Float)
    ml_cls_prob_1 = Column(Float)
    ml_cls_prob_2 = Column(Float)
    dl_reg_pred_1 = Column(Float)
    dl_reg_pred_2 = Column(Float)
    dl_cls_pred_1 = Column(Float)
    dl_cls_pred_2 = Column(Float)
    dl_cls_prob_1 = Column(Float)
    dl_cls_prob_2 = Column(Float)


class GamesTable(Base):
    __tablename__ = "games"
    __table_args__ = (PrimaryKeyConstraint("game_id"),)

    game_id = Column(String)
    game_datetime = Column(DateTime)
    home_team = Column(String)
    away_team = Column(String)
    open_line = Column(Float)
    home_score = Column(Integer)
    away_score = Column(Integer)
    game_completed = Column(Boolean)
    scores_last_update = Column(DateTime)
    odds_last_update = Column(DateTime)


class LinesTable(Base):
    __tablename__ = "lines"
    __table_args__ = (PrimaryKeyConstraint("game_id", "line_datetime"),)

    game_id = Column(String)
    line_datetime = Column(DateTime)
    # Columns for each bookmaker
    # Barstool Sportsbook
    barstool_home_line = Column(Float)
    barstool_home_line_price = Column(Float)
    barstool_away_line = Column(Float)
    barstool_away_line_price = Column(Float)

    # BetOnline.ag
    betonlineag_home_line = Column(Float)
    betonlineag_home_line_price = Column(Float)
    betonlineag_away_line = Column(Float)
    betonlineag_away_line_price = Column(Float)

    # BetMGM
    betmgm_home_line = Column(Float)
    betmgm_home_line_price = Column(Float)
    betmgm_away_line = Column(Float)
    betmgm_away_line_price = Column(Float)

    # BetRivers
    betrivers_home_line = Column(Float)
    betrivers_home_line_price = Column(Float)
    betrivers_away_line = Column(Float)
    betrivers_away_line_price = Column(Float)

    # BetUS
    betus_home_line = Column(Float)
    betus_home_line_price = Column(Float)
    betus_away_line = Column(Float)
    betus_away_line_price = Column(Float)

    # Bovada
    bovada_home_line = Column(Float)
    bovada_home_line_price = Column(Float)
    bovada_away_line = Column(Float)
    bovada_away_line_price = Column(Float)

    # DraftKings
    draftkings_home_line = Column(Float)
    draftkings_home_line_price = Column(Float)
    draftkings_away_line = Column(Float)
    draftkings_away_line_price = Column(Float)

    # FanDuel
    fanduel_home_line = Column(Float)
    fanduel_home_line_price = Column(Float)
    fanduel_away_line = Column(Float)
    fanduel_away_line_price = Column(Float)

    # LowVig.ag
    lowvig_home_line = Column(Float)
    lowvig_home_line_price = Column(Float)
    lowvig_away_line = Column(Float)
    lowvig_away_line_price = Column(Float)

    # MyBookie.ag
    mybookieag_home_line = Column(Float)
    mybookieag_home_line_price = Column(Float)
    mybookieag_away_line = Column(Float)
    mybookieag_away_line_price = Column(Float)

    # PointsBet (US)
    pointsbetus_home_line = Column(Float)
    pointsbetus_home_line_price = Column(Float)
    pointsbetus_away_line = Column(Float)
    pointsbetus_away_line_price = Column(Float)

    # SuperBook
    superbook_home_line = Column(Float)
    superbook_home_line_price = Column(Float)
    superbook_away_line = Column(Float)
    superbook_away_line_price = Column(Float)

    # TwinSpires
    twinspires_home_line = Column(Float)
    twinspires_home_line_price = Column(Float)
    twinspires_away_line = Column(Float)
    twinspires_away_line_price = Column(Float)

    # Unibet
    unibet_us_home_line = Column(Float)
    unibet_us_home_line_price = Column(Float)
    unibet_us_away_line = Column(Float)
    unibet_us_away_line_price = Column(Float)

    # William Hill (Caesars)
    williamhill_us_home_line = Column(Float)
    williamhill_us_home_line_price = Column(Float)
    williamhill_us_away_line = Column(Float)
    williamhill_us_away_line_price = Column(Float)

    # WynnBET
    wynnbet_home_line = Column(Float)
    wynnbet_home_line_price = Column(Float)
    wynnbet_away_line = Column(Float)
    wynnbet_away_line_price = Column(Float)


class NbastatsGeneralTraditionalTable(Base):
    """
    Data source provider: nbastats
    Data source URL: https://www.nba.com/stats/teams/traditional
    Data source description:
    """

    __tablename__ = "team_nbastats_general_traditional"
    __table_args__ = (PrimaryKeyConstraint("team_name", "to_date", "games"),)
    team_name = Column(String)
    to_date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    games = Column(String)
    gp = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    w_pct = Column(Float)
    min = Column(Float)
    fgm = Column(Float)
    fga = Column(Float)
    fg_pct = Column(Float)
    fg3m = Column(Float)
    fg3a = Column(Float)
    fg3_pct = Column(Float)
    ftm = Column(Float)
    fta = Column(Float)
    ft_pct = Column(Float)
    oreb = Column(Float)
    dreb = Column(Float)
    reb = Column(Float)
    ast = Column(Float)
    tov = Column(Float)
    stl = Column(Float)
    blk = Column(Float)
    blka = Column(Float)
    pf = Column(Float)
    pfd = Column(Float)
    pts = Column(Float)
    plus_minus = Column(Float)


class NbastatsGeneralAdvancedTable(Base):
    """
    Data source provider: nbastats
    Data source URL: https://www.nba.com/stats/teams/advanced
    Data source description:
    """

    __tablename__ = "team_nbastats_general_advanced"
    __table_args__ = (PrimaryKeyConstraint("team_name", "to_date", "games"),)
    team_name = Column(String)
    to_date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    games = Column(String)
    gp = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    w_pct = Column(Float)
    min = Column(Float)
    e_off_rating = Column(Float)
    off_rating = Column(Float)
    e_def_rating = Column(Float)
    def_rating = Column(Float)
    e_net_rating = Column(Float)
    net_rating = Column(Float)
    ast_pct = Column(Float)
    ast_to = Column(Float)
    ast_ratio = Column(Float)
    oreb_pct = Column(Float)
    dreb_pct = Column(Float)
    reb_pct = Column(Float)
    tm_tov_pct = Column(Float)
    efg_pct = Column(Float)
    ts_pct = Column(Float)
    e_pace = Column(Float)
    pace = Column(Float)
    pace_per40 = Column(Float)
    poss = Column(Integer)
    pie = Column(Float)


class NbastatsGeneralFourfactorsTable(Base):
    """
    Data source provider: nbastats
    Data source URL: https://www.nba.com/stats/teams/four-factors
    Data source description:
    """

    __tablename__ = "team_nbastats_general_fourfactors"
    __table_args__ = (PrimaryKeyConstraint("team_name", "to_date", "games"),)
    team_name = Column(String)
    to_date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    games = Column(String)
    gp = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    w_pct = Column(Float)
    min = Column(Float)
    efg_pct = Column(Float)
    fta_rate = Column(Float)
    tm_tov_pct = Column(Float)
    oreb_pct = Column(Float)
    opp_efg_pct = Column(Float)
    opp_fta_rate = Column(Float)
    opp_tov_pct = Column(Float)
    opp_oreb_pct = Column(Float)


class NbastatsGeneralOpponentTable(Base):
    """
    Data source provider: nbastats
    Data source URL: https://www.nba.com/stats/teams/opponent
    Data source description:
    """

    __tablename__ = "team_nbastats_general_opponent"
    __table_args__ = (PrimaryKeyConstraint("team_name", "to_date", "games"),)
    team_name = Column(String)
    to_date = Column(Date)
    season = Column(String)
    season_type = Column(String)
    games = Column(String)
    gp = Column(Integer)
    w = Column(Integer)
    l = Column(Integer)
    w_pct = Column(Float)
    min = Column(Float)
    opp_fgm = Column(Float)
    opp_fga = Column(Float)
    opp_fg_pct = Column(Float)
    opp_fg3m = Column(Float)
    opp_fg3a = Column(Float)
    opp_fg3_pct = Column(Float)
    opp_ftm = Column(Float)
    opp_fta = Column(Float)
    opp_ft_pct = Column(Float)
    opp_oreb = Column(Float)
    opp_dreb = Column(Float)
    opp_reb = Column(Float)
    opp_ast = Column(Float)
    opp_tov = Column(Float)
    opp_stl = Column(Float)
    opp_blk = Column(Float)
    opp_blka = Column(Float)
    opp_pf = Column(Float)
    opp_pfd = Column(Float)
    opp_pts = Column(Float)
    plus_minus = Column(Float)


class FivethirtyeightGamesTable(Base):
    """
    Data source provider: fivethirtyeight
    Data source URL: https://projects.fivethirtyeight.com/nba-model/nba_elo.csv
    Data source description: CSV file going back to 1947. Also includes game scores.
    """

    __tablename__ = "team_fivethirtyeight_games"
    __table_args__ = (PrimaryKeyConstraint("date", "team1", "team2"),)
    date = Column(Date)
    season = Column(String)
    neutral = Column(Boolean)
    season_type = Column(String)
    team1 = Column(String)
    team2 = Column(String)
    elo1_pre = Column(Float)
    elo2_pre = Column(Float)
    elo_prob1 = Column(Float)
    elo_prob2 = Column(Float)
    elo1_post = Column(Float)
    elo2_post = Column(Float)
    carm_elo1_pre = Column(Float)
    carm_elo2_pre = Column(Float)
    carm_elo_prob1 = Column(Float)
    carm_elo_prob2 = Column(Float)
    carm_elo1_post = Column(Float)
    carm_elo2_post = Column(Float)
    raptor1_pre = Column(Float)
    raptor2_pre = Column(Float)
    raptor_prob1 = Column(Float)
    raptor_prob2 = Column(Float)
    score1 = Column(Float)
    score2 = Column(Float)
    quality = Column(Float)
    importance = Column(Float)
    total_rating = Column(Float)


if __name__ == "__main__":
    # Creates all database tables defined above that haven't been created yet.
    engine = create_engine(
        f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
    )
    Base.metadata.create_all(engine)
