"""
Utility functions and classes for date handling in the Portfolio Manager application.

This module provides functions for parsing, formatting, and validating dates,
as well as a DateValidator class for ensuring date ranges are valid.
"""

from datetime import datetime, date
from typing import Union, Optional
from dateutil.parser import parse as dateutil_parse
from dateutil.tz import tzutc

InputDate = Union[str, date, datetime]

def parse_date(d: InputDate) -> date:
    """
    Parse a date from string, datetime, or date object.
    
    Args:
        d (InputDate): The input date.
        
    Returns:
        date: The parsed date.
    
    Raises:
        ValueError: If the date format is unsupported or invalid.
    """
    if isinstance(d, str):
        try:
            return dateutil_parse(d).date()
        except ValueError:
            raise ValueError(f"Unsupported or invalid date format: {d}")
    elif isinstance(d, datetime):
        return d.date()
    elif isinstance(d, date):
        return d
    else:
        raise ValueError(f"Unsupported date type: {type(d)}")

def format_date(d: date, format_str: str = '%Y-%m-%d') -> str:
    """
    Format a date to a specified string format.
    
    Args:
        d (date): The date to format.
        format_str (str): The desired output format (default: ISO format).
        
    Returns:
        str: The formatted date string.
    """
    return d.strftime(format_str)

def convert_timezone(d: datetime, target_tz: Optional[str] = None) -> datetime:
    """
    Convert a datetime object to the specified timezone.
    
    Args:
        d (datetime): The datetime to convert.
        target_tz (Optional[str]): The target timezone (default: UTC).
        
    Returns:
        datetime: The datetime in the target timezone.
    """
    if target_tz is None:
        return d.astimezone(tzutc())
    return d.astimezone(dateutil_parse(f'2020-01-01T00:00:00{target_tz}').tzinfo)

class DateValidator:
    @staticmethod
    def validate_date_range(start_date: date, end_date: date, operation_name: str):
        """
        Validate that the date range is correct.
        
        Args:
            start_date (date): The start date.
            end_date (date): The end date.
            operation_name (str): The operation name for error messages.
        
        Raises:
            ValueError: If the date range is invalid.
        """
        today = date.today()
        if start_date > today or end_date > today:
            raise ValueError(
                f"Error: Cannot perform {operation_name} with future dates. "
                f"Date range {start_date} to {end_date} contains future dates. "
                f"Today is {today}."
            )
        if start_date > end_date:
            raise ValueError(
                f"Error: Invalid date range for {operation_name}. "
                f"Start date {start_date} is after end date {end_date}."
            )

    @staticmethod
    def is_valid_date_range(start_date: date, end_date: date) -> bool:
        """
        Check if a date range is valid.
        
        Args:
            start_date (date): The start date.
            end_date (date): The end date.
        
        Returns:
            bool: True if the date range is valid, False otherwise.
        """
        today = date.today()
        return start_date <= end_date <= today