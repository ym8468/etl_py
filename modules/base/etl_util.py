"""
Provides ETL utility functions.
"""

from datetime import datetime
from datetime import timedelta
from os import path
import math


def get_app_root_path():
    """
    Gets etl_py's root directory absolute path.

    Returns:
        application's root directory absolute path.
    """
    app_root_path = path.dirname(path.abspath(__file__)) + '/../../'
    return path.normpath(app_root_path)

def is_date_format(date_string, date_format='%Y%m%d'):
    """
    Checks if the date_string value matches the date format or not.

    Args:
        date_string: a string

    Returns:
        True: matches the date format
        False: not match the date format
    """
    try:
        datetime.strptime(date_string, date_format)
    except ValueError:
        # Can't be parsed
        return False
    except TypeError:
        return False
    return True

def to_sql_date_format_from_yyyymmdd(date_string, to_date_format='%s-%s-%s'):
    """
    Converts a date string to a SQL date string.

    Args:
        date_string: a date string [yyyyMMdd]
        date_format: a date format converting to, default value is '%s-%s-%s'

    Returns:
        a SQL date string

    Raises:
        ValueError: date_string argument is invalid
    """
    if not is_date_format(date_string):
        raise ValueError('date_string is invalid. [date_string=%s]' % date_string)

    year = date_string[0:4]
    month = date_string[4:6]
    date = date_string[6:8]
    sql_date = to_date_format % (year, month, date)
    return sql_date

def add_date_from_yyyymmdd(date_string, add_days):
    """
    Adds days to the date_string.

    Args:
        date_string: a date string [yyyyMMdd]
        add_days: an integer to add

    Returns:
        a calculated date string

    Raises:
        ValueError: arguments are invalid
    """
    if not is_date_format(date_string):
        raise ValueError('date_string is invalid. [date_string=%s]' % date_string)
    if not isinstance(add_days, int):
        raise ValueError('add_days must be an integer. [add_days=%s]' % add_days)

    add_flag = True
    if add_days < 0:
        add_flag = False
        add_days = math.fabs(add_days)

    date = datetime.strptime(date_string, '%Y%m%d')
    days = timedelta(days=add_days)

    calc_date = None
    if add_flag:
        calc_date = date + days
    else:
        calc_date = date - days

    calc_date_string = calc_date.strftime('%Y%m%d')
    return calc_date_string
