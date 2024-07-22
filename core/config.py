"""
Configuration settings for the Portfolio Manager application.

This module contains all the configuration variables used throughout the application.
It uses the python-decouple library to allow for easy environment variable overrides.
"""

from datetime import date
import logging
from decouple import config, Csv
from typing import Final, cast

# Database
_db_path = config('DB_PATH', default='portfolio.db')
if not isinstance(_db_path, str):
    raise TypeError(f"DB_PATH must be a string, got {type(_db_path)}")
DEFAULT_DB_PATH: Final[str] = cast(str, _db_path)
LOG_FILE_PATH = config('LOG_FILE_PATH', default='portfolio.log')
LOG_LEVEL = config('LOG_LEVEL', default=logging.INFO, cast=int)

# Date ranges
MAX_DATE_RANGE = config('MAX_DATE_RANGE', default=365, cast=int)

# Price fetching
PRICE_FETCH_RETRY_ATTEMPTS = config('PRICE_FETCH_RETRY_ATTEMPTS', default=3, cast=int)
PRICE_FETCH_RETRY_DELAY = config('PRICE_FETCH_RETRY_DELAY', default=1, cast=int)  # in seconds

# GUI
DEFAULT_CHART_HEIGHT = config('DEFAULT_CHART_HEIGHT', default=400, cast=int)
DEFAULT_CHART_WIDTH = config('DEFAULT_CHART_WIDTH', default=600, cast=int)

# Current date (for testing purposes, normally this would be date.today())
CURRENT_DATE = config('CURRENT_DATE', default=date.today()) # cast=date creates an error.

# API keys (example of handling sensitive information)
API_KEYS = config('API_KEYS', default='', cast=Csv())

class DatabaseConfig:
    """Database-related configuration settings."""
    PATH = DEFAULT_DB_PATH

class LoggingConfig:
    """Logging-related configuration settings."""
    FILE_PATH = LOG_FILE_PATH
    LEVEL = LOG_LEVEL

class PriceFetchingConfig:
    """Price fetching-related configuration settings."""
    RETRY_ATTEMPTS = PRICE_FETCH_RETRY_ATTEMPTS
    RETRY_DELAY = PRICE_FETCH_RETRY_DELAY

class GUIConfig:
    """GUI-related configuration settings."""
    CHART_HEIGHT = DEFAULT_CHART_HEIGHT
    CHART_WIDTH = DEFAULT_CHART_WIDTH

# Exporting grouped configurations
DB_CONFIG = DatabaseConfig
LOG_CONFIG = LoggingConfig
PRICE_CONFIG = PriceFetchingConfig
GUI_CONFIG = GUIConfig