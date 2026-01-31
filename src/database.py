"""
SQLite database connection management.

Exports: get_engine(), get_session(), get_connection(), engine
Default DB path: data/nba_betting.db (override with DB_PATH env var)
"""

import os
from contextlib import contextmanager
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Database configuration
# Use absolute path to ensure it works regardless of current working directory
_DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "nba_betting.db"
DB_PATH = os.environ.get("DB_PATH", str(_DEFAULT_DB_PATH))


def get_database_url() -> str:
    """Return SQLite URL, creating parent directory if needed."""
    db_path = Path(DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{DB_PATH}"


def _set_sqlite_pragmas(dbapi_conn, connection_record):
    """Set WAL mode and busy_timeout for concurrent access."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA busy_timeout=5000;")
    cursor.close()


def get_engine():
    """Return SQLAlchemy engine with SQLite pragmas configured."""
    eng = create_engine(
        get_database_url(),
        connect_args={"check_same_thread": False},
    )
    event.listen(eng, "connect", _set_sqlite_pragmas)
    return eng


def get_session():
    """Return new SQLAlchemy session."""
    Session = sessionmaker(bind=get_engine())
    return Session()


@contextmanager
def get_connection():
    """Context manager for pandas read_sql operations."""
    engine = get_engine()
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()


# Shared engine instance (for imports that need a pre-configured engine)
engine = get_engine()

# Shared session factory
Session = sessionmaker(bind=engine)
