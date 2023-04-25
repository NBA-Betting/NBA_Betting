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
