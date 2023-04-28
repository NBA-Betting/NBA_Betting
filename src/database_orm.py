import sys

from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    PrimaryKeyConstraint,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base

sys.path.append("../")
from passkeys import RDS_ENDPOINT, RDS_PASSWORD

Base = declarative_base()


# ADD TABLES HERE


class FivethirtyeightPlayerTable(Base):
    __tablename__ = "ibd_fivethirtyeight_player"
    __table_args__ = (PrimaryKeyConstraint("player_id", "season", "to_date"),)
    to_date = Column(Date)
    player_name = Column(String)
    player_id = Column(String)
    season = Column(Integer)
    poss = Column(Integer)
    mp = Column(Integer)
    raptor_box_offense = Column(Float)
    raptor_box_defense = Column(Float)
    raptor_box_total = Column(Float)
    raptor_onoff_offense = Column(Float)
    raptor_onoff_defense = Column(Float)
    raptor_onoff_total = Column(Float)
    raptor_offense = Column(Float)
    raptor_defense = Column(Float)
    raptor_total = Column(Float)
    war_total = Column(Float)
    war_reg_season = Column(Float)
    war_playoffs = Column(Float)
    predator_offense = Column(Float)
    predator_defense = Column(Float)
    predator_total = Column(Float)
    pace_impact = Column(Float)


class InpredictableWPATable(Base):
    __tablename__ = "ibd_inpredictable_wpa"
    __table_args__ = (PrimaryKeyConstraint("to_date", "player", "rnk"),)

    rnk = Column(Integer)
    player = Column(String)
    pos = Column(String)
    gms = Column(Integer)
    wpa = Column(Float)
    ewpa = Column(Float)
    clwpa = Column(Float)
    gbwpa = Column(Float)
    sh = Column(Float)
    to = Column(Float)
    ft = Column(Float)
    reb = Column(Float)
    ast = Column(Float)
    stl = Column(Float)
    blk = Column(Float)
    kwpa = Column(Float)
    to_date = Column(Date)


if __name__ == "__main__":
    # Creates all database tables defined above that haven't been created yet.
    engine = create_engine(
        f"postgresql://postgres:{RDS_PASSWORD}@{RDS_ENDPOINT}/nba_betting"
    )
    Base.metadata.create_all(engine)
