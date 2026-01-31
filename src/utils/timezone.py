"""Timezone utilities. Default: America/Denver (override with APP_TIMEZONE env var)."""

import os
from datetime import datetime

import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default timezone (US Mountain Time - where NBA betting was originally tracked)
DEFAULT_TIMEZONE = "America/Denver"

# Get timezone from environment, with fallback to default
APP_TIMEZONE_STR = os.getenv("APP_TIMEZONE", DEFAULT_TIMEZONE)

# Create timezone object
try:
    APP_TIMEZONE = pytz.timezone(APP_TIMEZONE_STR)
except pytz.UnknownTimeZoneError:
    print(f"Warning: Unknown timezone '{APP_TIMEZONE_STR}', falling back to {DEFAULT_TIMEZONE}")
    APP_TIMEZONE_STR = DEFAULT_TIMEZONE
    APP_TIMEZONE = pytz.timezone(DEFAULT_TIMEZONE)


def get_current_time() -> datetime:
    """Get current time in the application timezone."""
    return datetime.now(APP_TIMEZONE)


def get_current_date():
    """Get current date in the application timezone."""
    return get_current_time().date()


def get_current_year() -> int:
    """Get current year in the application timezone."""
    return get_current_time().year


def localize(dt: datetime) -> datetime:
    """Localize a naive datetime to the application timezone."""
    if dt.tzinfo is None:
        return APP_TIMEZONE.localize(dt)
    return dt.astimezone(APP_TIMEZONE)


def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Format a datetime in the application timezone."""
    localized = localize(dt) if dt.tzinfo is None else dt.astimezone(APP_TIMEZONE)
    return localized.strftime(fmt)
