"""SQLAlchemy ORM models for all database tables."""

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    JSON,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.orm import declarative_base

from src.database import engine

# Create a base class for declarative models
Base = declarative_base()


# Define the BettingAccountTable model
class BettingAccountTable(Base):
    __tablename__ = "betting_account"
    __table_args__ = (PrimaryKeyConstraint("datetime"),)
    datetime = Column(DateTime)  # Date and time of the account balance
    balance = Column(Float)  # Current account balance


# Define the BetsTable model
class BetsTable(Base):
    __tablename__ = "bets"
    __table_args__ = (PrimaryKeyConstraint("game_id", "bet_datetime"),)
    game_id = Column(String)  # Unique identifier for the game
    bet_datetime = Column(DateTime)  # Date and time of the bet
    bet_status = Column(String)  # Status of the bet (e.g., open, settled)
    bet_amount = Column(Float)  # Amount of the bet
    bet_price = Column(Integer)  # Price of the bet
    bet_location = Column(String)  # Location of the bet
    bet_profit_loss = Column(Float)  # Profit or loss of the bet
    bet_direction = Column(String)  # Direction of the bet (e.g., home, away)
    bet_line = Column(Float)  # Line of the bet


# Define the AllFeaturesJSONTable model
class AllFeaturesJSONTable(Base):
    __tablename__ = "all_features_json"
    __table_args__ = (PrimaryKeyConstraint("game_id"),)
    game_id = Column(String)  # Unique identifier for the game
    data = Column(JSON)  # JSON data containing all features


class PredictionsTable(Base):
    """
    Stores predictions from trained models.

    Each game can have multiple prediction records - one per model variant:
    - "standard": Models without vegas line as a feature
    - "vegas": Models that include vegas line as a feature

    The model_variant column allows comparing accuracy between variants.
    """

    __tablename__ = "predictions"
    __table_args__ = (PrimaryKeyConstraint("game_id", "model_variant"),)

    game_id = Column(String)  # Unique game identifier (e.g., "202501150LAL")
    model_variant = Column(String)  # "standard" or "vegas"
    predicted_at = Column(DateTime)  # When this prediction was generated
    open_spread = Column(Float)  # Opening spread (home team perspective, negative = home favored)
    prediction_spread = Column(Float)  # Spread used for this prediction (open or current)
    home_cover_prob = Column(Float)  # Probability home team covers spread (0-1)
    home_cover_pred = Column(Boolean)  # True if model predicts home covers
    predicted_margin = Column(Float)  # Regression: predicted home margin (home_score - away_score)
    pick = Column(String)  # "Home" or "Away" - recommended bet direction
    confidence = Column(Float)  # Confidence in pick (0-100 scale)


# Define the GamesTable model
class GamesTable(Base):
    __tablename__ = "games"
    __table_args__ = (PrimaryKeyConstraint("game_id"),)
    game_id = Column(String)  # Unique identifier for the game
    game_datetime = Column(DateTime)  # Date and time of the game
    home_team = Column(String)  # Home team
    away_team = Column(String)  # Away team
    open_line = Column(Float)  # Opening line
    home_score = Column(Integer)  # Home team score
    away_score = Column(Integer)  # Away team score
    game_completed = Column(Boolean)  # Flag indicating if the game is completed
    scores_last_update = Column(DateTime)  # Date and time of the last score update
    odds_last_update = Column(DateTime)  # Date and time of the last odds update


# Define the LinesTable model
class LinesTable(Base):
    """
    Simplified betting lines table - stores open and current lines per game.

    Open line comes from Covers (historical) or first Odds API fetch.
    Current line is updated by Odds API with consensus/average across books.
    Line movement = current_line - open_line (useful for modeling).
    """

    __tablename__ = "lines"
    __table_args__ = (PrimaryKeyConstraint("game_id"),)

    game_id = Column(String)  # FK to games table
    open_line = Column(Float)  # Opening spread (home team perspective)
    open_line_price = Column(Float)  # Opening price (e.g., -110)
    current_line = Column(Float)  # Most recent spread
    current_line_price = Column(Float)  # Most recent price
    line_last_update = Column(DateTime)  # When current_line was last updated


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


if __name__ == "__main__":
    # Creates all database tables defined above that haven't been created yet.
    # Uses the centralized engine from src/database.py
    Base.metadata.create_all(engine)
    print(f"Database tables created successfully.")
