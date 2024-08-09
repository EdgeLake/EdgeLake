"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys
import time
import pytz
import operator
import re

from datetime import date, datetime, timedelta, timezone
from dateutil import parser, tz

import edge_lake.generic.process_log as process_log
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data

pattern_func_ = re.compile(r'\[(.*?)\]')    # Pull keys in casting

name_to_instance_ = {
    "str" : str,
    "string" : str,
    "int" : int,
    "bigint": int,
    "integer" : int,
    "float" : float,
    "timestamp" : str,
    "date" : str,
    "time" : str,
    "varchar" : str,
    "bool" : bool,
}

def get_instance_by_name(instance_name):
    '''
    If instance name in the dictionary, return instance
    '''
    if isinstance(instance_name, str):
        instance = name_to_instance_[instance_name] if instance_name in name_to_instance_ else None
    else:
        instance = None
    return instance


cast_to_type_ = {        # Map casting name to data type

    "float" : "float",
    "int"   : "int",
    "ljust" : "str",
    "rjust" : "str"
}

tz_list = {}

#datetime_pattern_ = r"'([\d-]+\s[\d:.]+)'" # using re - captures the datetime string in the format 'YYYY-MM-DD HH:MM:SS.ffffff'.
# captures fixed datetimes with optional hours, minutes, and seconds. Support - '2024-07-10T12:34:56.789Z'", and "'2024-07-10T12:34:56'",
datetime_pattern_ = r"'(\d{4}-\d{2}-\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}(?:\.\d+)?))?)?(?:Z)?'"

interval_pattern_ = r'(\d+)\s*(minute|hour|day|week|month|year)s?' # captures the interval value and the unit, allowing for plural forms.

# Map units to timedelta arguments
unit_to_timedelta_ = {
    'minute': 'minutes',
    'hour': 'hours',
    'day': 'days',
    'week': 'weeks',
    'month': 'days',  # approximate month as 30 days
    'year': 'days'  # approximate year as 365 days
}

second_conversions_ = [
    ("year", 31536000),
    ("month", 259200),
    ("day", 86400),
    ("hour", 3600),
    ("minute", 60),
]


# =======================================================================================================================
# Get a timezone object, if not exists - create a new timezone instance
# =======================================================================================================================
def get_timezone_object(timezone):

    global tz_list      # a list of time zones

    try:
        tz_object = tz_list[timezone]
    except:
        # create new
        try:
            tz_object = tz.gettz(timezone)
        except:
            tz_object = None
        finally:
            tz_list[timezone] = tz_object       # Set for next time (including a null)
    return tz_object

utc_zone = tz.tzutc()
local_zone = tz.tzlocal()

# Create a list of time zone objects - https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

tz_list["utc"] =  utc_zone
tz_list["local"] =  local_zone
tz_list["pt"] =  get_timezone_object("America/Los_Angeles")       # pacific time
tz_list["mt"] =  get_timezone_object("America/Denver")        # mountain time
tz_list["ct"] =  get_timezone_object("America/Chicago")        # central time
tz_list["et"] =  get_timezone_object("America/New_York")        # eastern time


utc_diff = 0


TIME_FORMAT_STR = "%Y-%m-%d %H:%M:%S.%f"
TIME_FORMAT_STR_NO_MS = "%Y-%m-%d %H:%M:%S"     # Without milliseconds
UTC_TIME_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%fZ"
UTC_TIME_FORMAT_STR_NO_MS = "%Y-%m-%dT%H:%M:%S" # Without milliseconds

trunc_str = {
    #"year": "%Y-%m-%d %H:%M:%f",
    "year": "%Y-01-01 00:00:0.0",
    "month": "%Y-%m-01 00:00:0.0",
    "day": "%Y-%m-%d 00:00:0.0",
    "hour": "%Y-%m-%d %H:00:0.0",
    "minute": "%Y-%m-%d %H:%M:0.0",
    "second": "%Y-%m-%d %H:%M:%S.0",
}


# ----------------------------------------------------------
# Get a format string to truncate a date string:
# get_trunc_format(hour) --> %Y-%m-%d %H:0.0.0
# ----------------------------------------------------------
def get_time_trunc_format(trunc_time):
    return trunc_str[trunc_time]

extract_str = {
    "year": "%Y",
    "month": "%m",
    "day": "%d",
    "hour": "%H",
    "minute": "%M",
    "second": "%S",
}

# ----------------------------------------------------------
# Extract units from a date string:
# get_trunc_format(hour) --> %Y-%m-%d %H:0.0.0
# ----------------------------------------------------------
def get_time_extract_format(extract_time):
    return extract_str[extract_time]

# ----------------------------------------------------------
# Given 2 strings, determine if string_b is a substring of string_a
# if not strings - return False
# ----------------------------------------------------------
def contains(string_a:str, string_b:str):
    if not isinstance(string_a,str) or not isinstance(string_b,str):
        return 0        # Return False
    if string_a.find(string_b) == -1:
        return 0        # Not a substring
    return 1

comarison = {

    "<": operator.lt,
    "<=": operator.le,
    "=": operator.eq,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
    "is": None,
    "contains" : contains

}


def DATE_TIME_LT(date_a, date_b):
    if date_a[0:10] < date_b[0:10]:
        # compare YY:MM:DD
        return True
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return False
        if date_a[11:19] < date_b[11:19]:
            return True
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return False
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] < date_b[20:20 + length_b]

    return False


def DATE_TIME_LE(date_a, date_b):
    if date_a[0:10] < date_b[0:10]:
        # compare YY:MM:DD
        return True
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return True
        if date_a[11:19] < date_b[11:19]:
            return True
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return True
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] <= date_b[20:20 + length_b]

    return False


def DATE_TIME_EQ(date_a, date_b):
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return True
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return True
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] == date_b[20:20 + length_b]

    return False


def DATE_TIME_NE(date_a, date_b):
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return False
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return False
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] != date_b[20:20 + length_b]

    return True


def DATE_TIME_GT(date_a, date_b):
    if date_a[0:10] > date_b[0:10]:
        # compare YY:MM:DD
        return True
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return False
        if date_a[11:19] > date_b[11:19]:
            return True
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return False
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] > date_b[20:20 + length_b]

    return False


def DATE_TIME_GE(date_a, date_b):
    if date_a[0:10] > date_b[0:10]:
        # compare YY:MM:DD
        return True
    if date_a[0:10] == date_b[0:10]:
        # compare HH:SS:MM
        if len(date_a) == 10 or len(date_b) == 10:
            return True
        if date_a[11:19] > date_b[11:19]:
            return True
        if date_a[11:19] == date_b[11:19]:
            if len(date_a) <= 20 or len(date_b) <= 20:
                return True
            # compare seconds
            length_a = len(date_a[20:])
            if not date_a[-1].isdigit():
                length_a -= 1
            length_b = len(date_b[20:])
            if not date_b[-1].isdigit():
                length_b -= 1
            return date_a[20:20 + length_a] >= date_b[20:20 + length_b]

    return False


def DATE_TIME_IS(date_a, date_b):
    return DATE_TIME_EQ(date_a, date_b)


comarison_date_time = {

    "<": DATE_TIME_LT,
    "<=": DATE_TIME_LE,
    "=": DATE_TIME_EQ,
    "!=": DATE_TIME_NE,
    ">=": DATE_TIME_GE,
    ">": DATE_TIME_GT,
    "is": None,

}


# =======================================================================================================================
# Get current time in seconds (UTC time since EPOCH)
# =======================================================================================================================
def get_current_time_in_sec():
    return int(time.time())
# =======================================================================================================================
# Get current time
# default date format  \'2019-07-29 19:34:00.123\'
# =======================================================================================================================
def get_current_time(format_string=TIME_FORMAT_STR):
    return datetime.now().strftime(format_string)
# =======================================================================================================================
# Get current UTC time
# default date format  \'2019-07-29 19:34:00.123\'
# =======================================================================================================================
def get_current_utc_time(format_string = UTC_TIME_FORMAT_STR):
    return datetime.utcnow().strftime(format_string)

# =======================================================================================================================
# Get current UTC time from the number of seconds since January 1, 1970
# =======================================================================================================================
def get_utc_time_from_seconds(unix_time_seconds, format_string = UTC_TIME_FORMAT_STR):

    try:
        # Convert to a UTC datetime object
        utc_time = datetime.utcfromtimestamp(unix_time_seconds)

        # Format the datetime object as a string
        formatted_time = utc_time.strftime(format_string)
    except:
        formatted_time = ""

    return formatted_time

# =======================================================================================================================
# Checks whether a string matches a given date format
# =======================================================================================================================
def is_valid_date(date_string, date_format):
    try:
        datetime.strptime(date_string, date_format)
        ret_val = True
    except ValueError:
        ret_val = False
    return ret_val

# =======================================================================================================================
# From UTC time in second to current formatted time
# =======================================================================================================================
def get_date_time_from_utc_sec(utc_seconds, format_string=TIME_FORMAT_STR_NO_MS):
    '''
    Convert a time expressed in seconds since the epoch to a struct_time in local time
    '''
    time_struct = time.localtime(utc_seconds)
    if format_string[-3:] == ".%f":
        format_str = format_string[:-3]     # Avoid the MS
        suffix = ".0"
    else:
        format_str = format_string
        suffix = ""
    return time.strftime(format_str, time_struct) + suffix

# =======================================================================================================================
# Get the time in seconds since 1970-01-01
# =======================================================================================================================
def get_time_in_sec(time_str, is_local = False):
    time_unified = unify_date_time(time_str)
    seconds_time = get_time_in_seconds(time_unified)
    return seconds_time
# =======================================================================================================================
# Seconds - Get the time difference between dates
# =======================================================================================================================
def get_seconds_diff(new_date, base_seconds):
    time_unified = unify_date_time(new_date)
    new_seconds = get_time_in_seconds(time_unified)
    timediff = new_seconds - base_seconds
    return timediff


# =======================================================================================================================
# Minutes - Get the time difference between dates
# =======================================================================================================================
def get_minutes_diff(new_date, base_seconds):
    time_unified = unify_date_time(new_date)
    new_seconds = get_time_in_seconds(time_unified) / 60
    timediff = new_seconds - base_seconds / 60
    return timediff


# =======================================================================================================================
# Hours - Get the time difference between dates
# =======================================================================================================================
def get_hours_diff(new_date, base_seconds):
    time_unified = unify_date_time(new_date)
    new_seconds = get_time_in_seconds(time_unified) / 3600
    timediff = new_seconds - base_seconds / 3600
    return timediff


# =======================================================================================================================
# Days - Get the time difference between dates
# =======================================================================================================================
def get_days_diff(new_date, base_obj):
    try:
        new_obj = get_date_obj(new_date)
        timediff = (new_obj - base_obj).days
    except ValueError as e:
        process_log.add("Error", "Failed to process date string: %s" % new_date)
        timediff = 0
    return timediff


# =======================================================================================================================
# Weeks - Get the time difference between dates
# =======================================================================================================================
def get_weeks_diff(new_date, base_obj):
    timediff = get_days_diff(new_date, base_obj)
    return timediff / 7


# =======================================================================================================================
# Years - Get the time difference between dates
# =======================================================================================================================
def get_years_diff(new_date, base_obj):
    try:
        new_obj = get_date_obj(new_date)
        timediff = abs(new_obj.year - base_obj.year)
    except ValueError as e:
        process_log.add("Error", "Failed to process date string: %s" % new_date)
        timediff = 0
    return timediff


# =======================================================================================================================
# Months - Get the time difference between dates
# =======================================================================================================================
def get_months_diff(new_date, base_obj):
    timediff = get_years_diff(new_date, base_obj) * 12
    timediff += (int(new_date[5:7]) - base_obj.month)
    return timediff


# =======================================================================================================================
# Subtract seconds from date - return new date
# =======================================================================================================================
def subtract_seconds(end_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(end_date)
    date_obj = datetime.fromtimestamp(timestamp - units)
    start_date = date_obj.strftime(date_format)
    return start_date


# =======================================================================================================================
# Add seconds from date - return new date
# =======================================================================================================================
def add_seconds(start_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(start_date)
    date_obj = datetime.fromtimestamp(timestamp + units)
    end_date = date_obj.strftime(date_format)
    return end_date


# =======================================================================================================================
# Subtract minutes from date - return new date
# =======================================================================================================================
def subtract_minutes(end_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(end_date)
    date_obj = datetime.fromtimestamp(timestamp - units * 60)
    start_date = date_obj.strftime(date_format)
    return start_date


# =======================================================================================================================
# Add minutes from date - return new date
# =======================================================================================================================
def add_minutes(start_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(start_date)
    date_obj = datetime.fromtimestamp(timestamp + units * 60)
    end_date = date_obj.strftime(date_format)
    return end_date


# =======================================================================================================================
# Subtract hours from date - return new date
# =======================================================================================================================
def subtract_hours(end_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(end_date)
    date_obj = datetime.fromtimestamp(timestamp - units * 3600)
    start_date = date_obj.strftime(date_format)
    return start_date


# =======================================================================================================================
# Add hours from date - return new date
# =======================================================================================================================
def add_hours(start_date, units, date_format = TIME_FORMAT_STR):
    timestamp = get_time_in_seconds(start_date)
    date_obj = datetime.fromtimestamp(timestamp + units * 3600)
    end_date = date_obj.strftime(date_format)
    return end_date


# =======================================================================================================================
# Subtract days from date - return new date
# =======================================================================================================================
def subtract_days(end_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(end_date)
    if date_obj:
        new_date_obj = date_obj - timedelta(days=units)
        start_date = new_date_obj.strftime("%Y-%m-%d") + end_date[10:]
    else:
        start_date = None

    return start_date

# =======================================================================================================================
# Add days from date - return new date
# =======================================================================================================================
def add_days(start_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(start_date)
    if date_obj:
        new_date_obj = date_obj + timedelta(days=units)
        end_date = new_date_obj.strftime("%Y-%m-%d") + start_date[10:]
    else:
        end_date = None
    return end_date

# =======================================================================================================================
# Subtract weeks from date - return new date
# =======================================================================================================================
def subtract_weeks(end_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(end_date)
    if date_obj:
        new_date_obj = date_obj - timedelta(days=units * 7)
        start_date = new_date_obj.strftime("%Y-%m-%d") + end_date[10:]
    else:
        start_date = None
    return start_date

# =======================================================================================================================
# Add weeks to date - return new date
# =======================================================================================================================
def add_weeks(start_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(start_date)
    if date_obj:
        new_date_obj = date_obj + timedelta(days=units * 7)
        end_date = new_date_obj.strftime("%Y-%m-%d") + start_date[10:]
    else:
        end_date = None
    return end_date

# =======================================================================================================================
# Subtract months from date - return new date
# =======================================================================================================================
def subtract_months(end_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(end_date)
    if date_obj:
        if units < date_obj.month:
            month = date_obj.month - units
            year = date_obj.year
        else:
            month = (date_obj.month + 12 - units)
            year = date_obj.year - 1

        start_date = "%04d-%02d-%02d" % (year, month, date_obj.day) + end_date[10:]
    else:
        start_date = None
    return start_date


# =======================================================================================================================
# Add months to date - return new date
# =======================================================================================================================
def add_months(start_date, units, date_format = TIME_FORMAT_STR):
    date_obj = get_date_obj(start_date)
    if date_obj:
        if units + date_obj.month <= 12:
            month = date_obj.month + units
            year = date_obj.year
        else:
            month = (date_obj.month + units - 12)
            year = date_obj.year + 1

        end_date = "%04d-%02d-%02d" % (year, month, date_obj.day) + start_date[10:]
    else:
        end_date = None
    return end_date


# =======================================================================================================================
# Subtract years from date - return new date
# =======================================================================================================================
def subtract_years(end_date, units, date_format = TIME_FORMAT_STR):
    year = end_date[:4]
    if year.isnumeric():
        start_date = ("%04u" % (int(year) - units)) + end_date[4:]
    else:
        start_date = end_date

    return start_date

# =======================================================================================================================
# Add years to date - return new date
# =======================================================================================================================
def add_years(start_date, units, date_format = TIME_FORMAT_STR):
    year = start_date[:4]
    if year.isnumeric():
        end_date = ("%04u" % (int(year) + units)) + start_date[4:]
    else:
        end_date = start_date

    return end_date

# =======================================================================================================================
# Seconds to date
# =======================================================================================================================
def seconds_to_date(timestamp, format_string=TIME_FORMAT_STR):
    try:
        date_obj = datetime.fromtimestamp(timestamp)
        ret_date = date_obj.strftime(format_string)
    except OverflowError  as err:
        ret_date = ""
    except:
        ret_date = ""
    return ret_date

# =======================================================================================================================
# date-time object to date
# =======================================================================================================================
def object_to_date(date_obj, format_string=TIME_FORMAT_STR):
    try:
        ret_date = date_obj.strftime(format_string)
    except:
        ret_date = ""
    return ret_date


# '2017-10-20 10:48:02.38' --> '2017-10-20 10:00:00.0'

time_units_unifier = {
    "second": ["second", 1, get_seconds_diff, subtract_seconds, 17, 19, 20, "0", "minute"],
    "minute": ["second", 60, get_minutes_diff, subtract_minutes, 14, 16, 17, "00.0", "hour"],
    "hour": ["second", 3600, get_hours_diff, subtract_hours, 11, 13, 14, "00:00.0", "day"],
    "day": ["day", 1, get_days_diff, subtract_days, 8, 10, 11, "00:00:00.0", "month"],
    "week": ["day", 7, get_weeks_diff, subtract_weeks, 8, 10, 11, "00:00:00.0", "month"],
    "month": ["month", 1, get_months_diff, subtract_months, 5, 7, 8, "01 00:00:00.0", "year"],
    "year": ["year", 1, get_years_diff, subtract_years, 0, 4, 5, "01-01 00:00:00.0", "century"],
    "century": ["", 1, None, None, 0, 0, 0, "1970-01-01 00:00:00.0", "century"]
}

time_units_calculator = {
    "second": [subtract_seconds, add_seconds],
    "minute": [subtract_minutes, add_minutes],
    "hour": [subtract_hours, add_hours],
    "day": [subtract_days, add_days],
    "week": [subtract_weeks, add_weeks],
    "month": [subtract_months, add_months],
    "year": [subtract_years, add_years],
}

time_units_char = {         # Addressing -3d -> -3 day
    "y" :   "year",
    "m" :   "month",
    "w" :   "week",
    "d" :   "day",
    "h" :   "hour",
    "t" :   "minute",
    "s" :   "second"
}


def __unify_time_format(time_string):
    return unify_time_format(None, time_string)


# =======================================================================================================================
# Unify time format, '12/20/2019 11:46:33 PM' --> '2019-12-10T17:57:22.0600128Z'
# =======================================================================================================================
def unify_time_format(status, time_string):
    time_length = len(time_string)
    if time_length < 10:
        status.add_error("Time format not supported: %s" % time_string)
        return ""
    elif time_length == 10:
        time_spec = 'auto'
    elif time_length <= 19 or time_string[-1] == 'M':
        # only with seconds or with 'PM', 'AM' ending
        time_spec = 'seconds'
    else:
        time_spec = 'milliseconds'

    try:
        unified_time = parser.parse(time_string).isoformat(sep='T', timespec=time_spec)
    except:
        unified_time = ""

    if unified_time:
        if time_spec[0] == 'd':  # only date
            unified_time = unified_time[:10]
        elif time_spec[0] == 'm':  # milliseconds
            # remove + sign
            index = unified_time.rfind('+')
            if index > -1:
                unified_time = unified_time[:index]

    return unified_time


# =======================================================================================================================
# Remove T and Z from time
# =======================================================================================================================
def remove_time_tz(time_str: str):
    length = len(time_str)
    if time_str[-1] == 'Z':
        length -= 1
        if time_str[10] == "T":
            new_str = time_str[:10] + ' ' + time_str[11:length]
        else:
            new_str = time_str[:length]
    elif time_str[10] == "T":
        new_str = time_str[:10] + ' ' + time_str[11:length]
    else:
        new_str = time_str

    return new_str


# =======================================================================================================================
# Unify the date - time formats
# '2019-06-20T15:42:20Z' --> '2019-06-20 15:42:20'
# =======================================================================================================================
def unify_date_time(time_value):
    if (time_value[0] == '\'' and time_value[-1] == '\'') or (time_value[0] == '"' and time_value[-1] == '"'):
        t_val = time_value[1:-1]
    else:
        t_val = time_value

    if len(t_val) == 10:
        updated_tv = t_val + " 00:00:00.0"
    else:
        # remove the T and Z from string
        end_offset = len(t_val)
        if t_val[-1] == 'Z':
            end_offset -= 1
        if end_offset > 26:  # max size is with 6 digits '2019-06-20 15:42:20.123456'
            end_offset = 26

        if end_offset > 10 and t_val[10] == 'T':
            updated_tv = t_val[:10] + ' ' + t_val[11:end_offset]
        else:
            updated_tv = t_val[:end_offset]

        if end_offset == 19:
            updated_tv += ".0"

    return updated_tv  # needs to be in the following format: "%Y-%m-%d %H:%M:%S.%f"


data_types = {
    "int": int,
    "string": str,
    "timestamp without time zone": __unify_time_format,
    "timestamp": __unify_time_format,
    "float": float,
}


# ==================================================================
# Info about increments - organizing the fiunctions by date intervals
# ==================================================================
class INCREMENTS():
    def __init__(self, time_unit, start_time, units, time_field_name):

        self.time_unit = time_unit  # seconds, minutes, houres, days, etc
        self.start_time = start_time  # the start date for increments
        self.base_time_obj = None
        self.base_date_obj = None
        self.units = units
        self.time_field_name = time_field_name
        self.seconds_time = 0

    # ==================================================================
    # get a value which is a key for the increments (based on the date)
    # ==================================================================
    def get_hash_value(self, event_date):

        time_calculator = time_units_unifier[self.time_unit]  # an array with info to calculate the hash
        function = time_calculator[2]
        if time_calculator[0] == "second":
            # diff is measured in seconds
            diff = function(event_date, self.seconds_time)
        else:
            diff = function(event_date, self.base_date_obj)

        return int(diff / self.units)

    # ==================================================================
    # Set Increment base values depending on the time_units
    # ==================================================================
    def set_increments_base(self):
        ret_val = True
        try:
            time_unified = unify_date_time(self.start_time)
            self.base_time_obj = time.strptime(time_unified, TIME_FORMAT_STR)
            self.base_date_obj = date(self.base_time_obj.tm_year, self.base_time_obj.tm_mon, self.base_time_obj.tm_mday)
            self.seconds_time = get_time_in_seconds(time_unified)
        except ValueError as e:
            process_log.add("Error", "Failed to process date string: %s" % self.start_time)
            ret_val = False

        return ret_val

    # ==================================================================
    # Return the field name used for TIME
    # ==================================================================
    def get_time_field_name(self):
        return self.time_field_name


# ==================================================================
# The functions that needs to be processed
# ==================================================================
class AL_FUNCTION():
    def __init__(self, function_name, column_name, data_type):
        self.function_name = function_name
        self.column_name = column_name
        self.data_type = data_type  # the data type managed by this function
        self.first_func_value = 0
        self.is_first = True
        self.counter_values = 1  # the number of values to return (1 or 2)
        self.is_increments = False

    # ==================================================================
    # Set Increment function
    # The function is applied to time intervals
    # ==================================================================
    def set_increment(self):
        self.is_increments = True
        self.hash_entries = {}  # with increments, the hash-entries maintain functions by time entries

    # ==================================================================
    # For increments functions - save the date generated the hash value
    # ==================================================================
    def set_time_field_value(self, time_field_value):
        self.time_field_value = time_field_value

    # ==================================================================
    # With increments, functions are organized by hash values.
    # Each entry represent an increment of time.
    # ==================================================================
    def get_function_by_hash(self, hash_value, time_field_value):
        if hash_value in self.hash_entries.keys():
            function = self.hash_entries[hash_value]
        else:
            # create a new function and place it in the hash table
            function = functions_classes[self.function_name](self.function_name, self.column_name, self.data_type)
            self.hash_entries[hash_value] = function
            function.set_time_field_value(time_field_value)  # keep the date generated the hash

            # if increments - need to add the increment info to every instance
            if self.function_name == "date_truncate":
                function.set_time_unit(self.time_unit)
            elif self.function_name == "date_extract":
                function.set_time_unit(self.time_unit, self.units)

        return function

    # ==================================================================
    # Get a list of all the hash values
    # ==================================================================
    def get_hash_values(self):
        return self.hash_entries.keys()

    # ==================================================================
    # Return the data type
    # ==================================================================
    def get_data_type(self):
        return self.data_type

    # ==================================================================
    # Return the column name
    # ==================================================================
    def get_column_name(self):
        return self.column_name

    # ==================================================================
    # Return results of the function in an array (Average returns 2 values)
    # ==================================================================
    def get_results(self):
        return [self.first_func_value]  # return one result


# ==================================================================
# Count Class
# ==================================================================
class AL_COUNT(AL_FUNCTION):

    def process_value(self, value):
        self.first_func_value += 1  # count the occurences


# ==================================================================
# MIN Class
# ==================================================================
class AL_MIN(AL_FUNCTION):

    def process_value(self, value):

        if self.is_first:
            self.first_func_value = value
            self.is_first = False
        else:
            if isinstance(value, bool) or isinstance(self.first_func_value, bool):
                a = 1
            if value < self.first_func_value:
                self.first_func_value = value


# ==================================================================
# Max Class
# ==================================================================
class AL_MAX(AL_FUNCTION):

    def process_value(self, value):
        if self.is_first:
            self.first_func_value = value
            self.is_first = False
        else:
            if value > self.first_func_value:
                self.first_func_value = value


# ==================================================================
# SUM Class
# ==================================================================
class AL_SUM(AL_FUNCTION):
    def process_value(self, value):
        self.first_func_value += value


# ==================================================================
# Average Class
# ==================================================================
class AL_AVG(AL_FUNCTION):
    def __init__(self, column_name, data_type):
        self.counter_values = 2  # returning SUM and Count
        self.second_func_value = 0
        AL_FUNCTION.__init__(self, column_name, data_type)

    def process_value(self, value):
        self.first_func_value += value
        self.second_func_value += 1

    # ==================================================================
    # Method Overidding
    # ==================================================================
    def get_results(self):
        return [self.first_func_value, self.second_func_value]


# ==================================================================
# DATE Truncate -
# Truncate to specified precision: date_trunc('hour', '2017-10-20 10:48:02' --> '2017-10-20 10:00:00'
# ==================================================================
class AL_DATE_TRUNC(AL_FUNCTION):

    def process_value(self, value):
        if self.is_first:
            self.is_first = False
        self.first_func_value = value

    # ==================================================================
    # The date type to truncate
    # ==================================================================
    def set_time_unit(self, time_unit):
        self.time_unit = time_unit

    # ==================================================================
    # Method Overidding
    # ==================================================================
    def get_results(self):

        if self.is_first:
            # no data provided
            trunc_date_array = []
        else:
            trunc_date = date_trunc(self.time_unit, self.first_func_value)
            trunc_date_array = [trunc_date]
        return trunc_date_array


# ==================================================================
# DATE Extract -
# Retrieve subfield from the date string : date_extract('hour', '2017-10-20 10:48:02' --> 10
# ==================================================================
class AL_DATE_EXTRACT(AL_FUNCTION):

    def process_value(self, value):
        if self.is_first:
            self.is_first = False

        self.first_func_value = value

    # ==================================================================
    # The date portion to extract and the number of date units that are considered as a single unit
    # ==================================================================
    def set_time_unit(self, time_unit, units):
        self.time_unit = time_unit
        self.units = int(units)

    # ==================================================================
    # Method Overidding
    # ==================================================================
    def get_results(self):

        if self.is_first:
            value = 0  # no data provided to process_value()
        else:
            extract_date = date_extract(self.time_unit, self.first_func_value)
            if extract_date.isdecimal():
                value = int(int(extract_date) / self.units)  # external int() to change from float to int
            else:
                value = 0
        return [value]


functions_classes = {
    "count": AL_COUNT,
    "min": AL_MIN,
    "max": AL_MAX,
    "sum": AL_SUM,
    "avg": AL_AVG,
    "date_truncate": AL_DATE_TRUNC,
    "date_extract": AL_DATE_EXTRACT
}


# ===================================================================================
# An object that maintains the functions that needs to be processed against the data
# ===================================================================================
class ProjectionFunctions():

    def __init__(self):
        self.functions = []  # an array of functions that needs to be processed in the query

    def reset(self):
        self.functions = []  # After query is executed, reset to allow next query

    # -----------------------------------------
    # Update projection functions and index
    # The list provides the order of the returned data
    # The index provides a fast way to update the function by the field name
    # -----------------------------------------
    def update_projection_functions(self, field_name, function, layer):
        self.functions.append((field_name, function, layer))  # append a tupple

    # -----------------------------------------
    # Get an array of functions that needs to be processed in the query
    # -----------------------------------------
    def get_functions(self):
        return self.functions

    def is_with_functions(self):
        return len(self.functions) > 0


# =======================================================================================================================
# Is the Operator supported
# =======================================================================================================================
def is_valid_operator(opr):
    return opr in comarison.keys()


# =======================================================================================================================
# Test - is null - comparison
# =======================================================================================================================
def test_null_oprand(a, b, opr, d_type):
    if opr == "is":
        if b == "null":
            if d_type == "int":
                if not a or not a.isdecimal():
                    return True
            elif d_type == "float":
                if not a or not utils_data.isfloat(a):
                    return True
    return False


# =======================================================================================================================
# Transform to a target data type and compare values
# Value a can be a list, compare if one of the list values returns True
# =======================================================================================================================
def compare(a, b, opr, d_type):
    if isinstance(a, list):
        ret_value = False
        for entry in a:
            if compare_values(entry, b, opr, d_type):
                ret_value = True
                break
    else:
        ret_value = compare_values(a, b, opr, d_type)

    return ret_value


# =======================================================================================================================
# Transform to a target data type and compare values
# Value a can be a list, compare if one of the list values returns True
# =======================================================================================================================
def compare_values(a, b, opr, d_type):
    if d_type in data_types:

        ret_value, value_a = transform_by_data_type(a, d_type)
        if not ret_value:
            if test_null_oprand(a, b, opr, d_type):
                return True
            return False  # can't compare

        ret_value, value_b = transform_by_data_type(b, d_type)
        if not ret_value:
            return False  # can't compare
    else:
        value_a = a
        value_b = b

    if d_type.startswith("time"):
        ret_val = comarison_date_time[opr](value_a, value_b)
    else:
        ret_val = comarison[opr](value_a, value_b)

    return ret_val


# =======================================================================================================================
# Transform to the target data type
# if target_data_type not in he array - return True
# =======================================================================================================================
def transform_by_data_type(value, target_data_type):
    if target_data_type in data_types.keys():
        try:
            return [True, data_types[target_data_type](value)]
        except:
            return [False, None]  # failed

    return [True, value]  # return original value


# =======================================================================================================================
# Given a date string, return number of seconds since epoch
# =======================================================================================================================
def get_time_in_seconds(date_time):
    date_str = unify_date_time(date_time)
    index = date_str.rfind('.')  # offset microseconds

    if len(date_str) <= 18 or date_str[18] == '.':
        date_str_no_seconds = date_str[:18]
    else:
        date_str_no_seconds = date_str[:19]

    try:
        date_obj = time.strptime(date_str_no_seconds[:19], TIME_FORMAT_STR_NO_MS)
        timestamp = time.mktime(date_obj)
    except ValueError as e:
        process_log.add("Error", "Failed to process date string: %s" % date_time)
        timestamp = 0
    else:
        if not timestamp - int(timestamp):
            # no fractional part
            if index != -1:
                # add fractional time
                timestamp += float(date_str[index:])

    return timestamp


# =======================================================================================================================
# Given a date string in a format YYYY-MM-DD, return date object
# =======================================================================================================================
def get_date_obj(date_string):
    year = date_string[:4]
    month = date_string[5:7]
    day = date_string[8:10]
    try:
        date_obj = date(int(year), int(month), int(day))
    except:
        date_obj = None
    return date_obj


# =======================================================================================================================
# Given a date, subtract time_units (like date) to find the start date
# =======================================================================================================================
def get_start_date(end_date, time_unit, units):
    function = time_units_unifier[time_unit][3]  # function to subtract time from date
    start_date = function(end_date, units)
    return start_date


# =======================================================================================================================
# Given a time unit (like minute, day etc.) return the number of seconds X counter
# =======================================================================================================================
def get_interval_in_seconds(time_unit, units):
    if time_unit in time_units_unifier.keys() and units.isdecimal():
        return time_units_unifier[time_unit][1] * int(units)
    return 0


# =======================================================================================================================
# Truncate to specified precision: date_trunc('hour', '2017-10-20 10:48:02.38' --> '2017-10-20 10:00:00.0'
# =======================================================================================================================
def date_trunc(time_unit, date_time):
    time_calculator = time_units_unifier[time_unit]  # an array with info to calculate the hash

    trunc_date = date_time[:time_calculator[6]] + time_calculator[7]
    return trunc_date


# =======================================================================================================================
# Retrieve subfield from the date string : date_extract('hour', '2017-10-20 10:48:02' --> 10
# =======================================================================================================================
def date_extract(time_unit, date_time):
    time_calculator = time_units_unifier[time_unit]  # an array with info to calculate the hash
    extract_date = date_time[time_calculator[4]: time_calculator[5]]
    return extract_date


# =======================================================================================================================
# Retrieve subfield from the date string : date_extract('hour', '2017-10-20 10:48:02' --> 10
# =======================================================================================================================
def get_next_time_unit(time_unit):  # change seconds to minutes, minutes to hours etc.
    return time_units_unifier[time_unit][8]


# =======================================================================================================================
# Get the time in PI format
# return formatted time string
# Example format - '2019-12-10T17:57:22.0600128Z' -
# can be extended as explained in -
# https://techsupport.osisoft.com/Documentation/PI-Web-API/help/topics/time-strings.html
# =======================================================================================================================
def get_pi_time_format(status, time_value):
    source_len = len(time_value)
    if source_len < 10 or time_value[4] != '-' or time_value[7] != '-':
        if status:
            status.add_error("PI time format error, date is expected to be YYYY-MM-DD and data is: %s" % time_value)
        pi_format = ""
    else:
        pi_format = time_value[:10] + "T"

        if source_len >= 19:
            date_suffix = ".0000000Z"
            # add HH:MM:SS
            pi_format += time_value[11:19]
            if time_value[-1] == 'Z' or time_value[-1] == 'z':
                source_len -= 1
            if source_len > 19:
                pi_format += time_value[19:27]
                sufix_len = len(pi_format[19:])
                pi_format += date_suffix[sufix_len:]
            else:
                pi_format += date_suffix
        else:
            pi_format += "00:00:00.0000000Z"

    return pi_format


# =======================================================================================================================
# Get PI column value - given a dictionary, get the column value. If the column is a dictionary, try again
# =======================================================================================================================
def get_pi_column_val(row, pi_column_name):
    ret_val = True

    if isinstance(row, dict) and pi_column_name in row.keys():
        pi_value = row[pi_column_name]
        if isinstance(pi_value, dict):
            # we see a case where pi_value is a dictionary containing more info
            if pi_column_name in pi_value.keys():
                pi_value = pi_value[pi_column_name]
            else:
                ret_val = False
                pi_value = ""
    else:
        ret_val = False
        pi_value = ""

    return [ret_val, pi_value]


# =======================================================================================================================
# Mapped entries for PI have the following structure: key.value
# The key can be a number representing a layer in the PI hierarchy or a name that is converted to a number by the dictionary.
# =======================================================================================================================
def get_pi_layer_and_value(status, layer_key):
    index = layer_key.find(".")
    if index == -1:
        # layer 0 (leaf) condition
        layer = 0
        key = layer_key
    else:
        layer_str = layer_key[:index]  # key.value
        if not layer_str.isdecimal():
            status.add_keep_error(
                "PI process error: Mapping conditions error: the key: '%s' is used in the atttributes section and not represented in the dictionary" % layer_str)
            return [False, -1, ""]

        if index > (len(layer_key) - 2):
            status.add_keep_error("PI process error: Mapping conditions error: the key: '%s' has no value" % layer_key)
            return [False, -1, ""]

        layer = int(layer_str)
        key = layer_key[index + 1:]

    return [True, layer, key]


# =======================================================================================================================
# Test if date is within the time interval
# =======================================================================================================================
def is_date_in_range(start_date, end_date, test_date):
    # The case of a single day such as the following example

    if len(end_date) < len(test_date):
        # Change: 2021-05-24 to: 2021-05-24 24:59:59.999999
        end_date_time = end_date + test_date[len(end_date):]
    else:
        end_date_time = end_date

    ret_val = False
    if comarison_date_time[">="](test_date, start_date):
        if comarison_date_time["<="](test_date, end_date_time):
            ret_val = True


    return ret_val


# =======================================================================================================================
# Manipulate a date / time string
# Example: 'now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts'
# =======================================================================================================================
def function_to_time(functions_str):
    func_array = functions_str.split(',')

    # first entry needs to be a date - time string or now

    if not func_array:
        time_str = ""
    else:
        entry = func_array[0].strip()
        if entry == '\'now\'' or entry == 'now':
            time_str = get_current_time()
        elif utils_data.check_timestamp(entry) or utils_data.check_date(entry):
            time_str = entry
        else:
            time_str = ""

    if time_str:
        # manipulate the time by the rest of the string
        for entry in func_array[1:]:
            entry = entry.strip()
            if len(entry) < 3:
                break  # entry is within quotations
            time_str = apply_time_function(time_str, entry[1:-1])
            if not entry:
                break  # non supported function`

    return time_str


# =======================================================================================================================
# Manipulate a date / time string
# Applu functions like 'start of month','+1 month','-1 day', '-2 hours', '+2 minuts'
# Options: "start of year", "start of month", "start of day",
# =======================================================================================================================
def apply_time_function(time_str, function_unit):
    updated_time = ""
    if function_unit.startswith("start "):
        # "start of year", "start of month", "start of day",
        function_array = function_unit.split()
        if len(function_array) == 3 and function_array[1] == "of":
            if function_array[2] == "year":
                # change 2020-12-10 --> 2020-01-01 00:0.0.0
                updated_time = time_str[:4] + "-01-01 00:00:0.0"
            elif function_array[2] == "month":
                # change 2020-12-10 --> 2020-12-01 00:0.0.0
                updated_time = time_str[:7] + "-01 00:00:0.0"
            elif function_array[2] == "month":
                # change 2020-12-10 --> 2020-12-10 00:0.0.0
                updated_time = time_str[:10] + " 00:00:0.0"
    else:
        operation = function_unit[0]
        if operation == '+' or operation == '-':
            word_start, word_end = utils_data.find_word_after(function_unit, 1)
            if word_start >= 1:
                units = function_unit[word_start:word_end]
                if units.isnumeric():
                    word_start, word_end = utils_data.find_word_after(function_unit, word_end)
                    time_unit = function_unit[word_start:word_end]
                    if time_unit[-1] == "s":
                        # remove ploral
                        time_unit = time_unit[:-1]

                    if time_unit in time_units_calculator:
                        if operation == '-':
                            function = time_units_calculator[time_unit][0]  # function to subtract time from date
                        else:
                            function = time_units_calculator[time_unit][1]  # function to add time from date
                        updated_time = function(time_str, int(units))

    return updated_time
# =======================================================================================================================
# Change UTC to current timezone
# =======================================================================================================================
def _utc_to_local(utc_dt, format_utc = '%Y-%m-%dT%H:%M:%S.%fZ', format_local = '%Y-%m-%d %H:%M:%S.%f'):
    global utc_zone
    global local_zone

    try:
        # utc = datetime.utcnow()
        # utc = datetime.strptime('2011-01-21 02:37:21', '%Y-%m-%d %H:%M:%S')
        utc_obj = datetime.strptime(utc_dt, format_utc)

        # Tell the datetime object that it's in UTC time zone since
        # datetime objects are 'naive' by default
        utc = utc_obj.replace(tzinfo=utc_zone)

        # Convert time zone
        converted_dt = utc.astimezone(local_zone)

        dest_dt = converted_dt.strftime(format_local)
    except:
        dest_dt = utc_dt

    return dest_dt  # Change to the destination DATE-TIME

# =======================================================================================================================
# Change UTC timezone to destination timezone
# Details at - https://dateutil.readthedocs.io/en/stable/exercises/index.html?highlight=cst#parsing-a-local-tzname
# https://www.timeanddate.com/time/zones/ --- https://en.wikipedia.org/wiki/List_of_time_zone_abbreviations
# Timezone
# =======================================================================================================================
def utc_to_timezone(utc_dt, format_string, timezone = "local"):
    '''
    If error on timezone change - keep source timezone
    '''
    global utc_zone
    global local_zone

    new_timezone = get_timezone_object(timezone)

    if not new_timezone:
        dest_dt = utc_dt
    else:

        try:
            # utc = datetime.utcnow()
            # utc = datetime.strptime('2011-01-21 02:37:21', '%Y-%m-%d %H:%M:%S')
            utc_obj = datetime.strptime(utc_dt[:19], format_string[:17])

            # Tell the datetime object that it's in UTC time zone since
            # datetime objects are 'naive' by default
            utc = utc_obj.replace(tzinfo=utc_zone) # Set the time received on UTC

            # Convert time zone
            converted_dt = utc.astimezone(new_timezone) # Change to local

            dest_dt = converted_dt.strftime(TIME_FORMAT_STR_NO_MS)

            if len(utc_dt) > 19:        # Add the sub-seconds
                if utc_dt[-1] == 'Z':
                    dest_dt += utc_dt[19:-1]
                else:
                    dest_dt += utc_dt[19:]

        except:
            dest_dt = utc_dt

    return dest_dt  # Change to the destination DATE-TIME


# =======================================================================================================================
# Change local timezone to UTC
# "2021-05-24 23:59:59.999999"
# =======================================================================================================================
def local_to_utc(local_dt):

    global local_zone

    try:
        local_time = datetime.strptime(local_dt[:19], TIME_FORMAT_STR_NO_MS)
        local_time = local_time.replace(tzinfo=local_zone)      # set the time received on local zone
        utc_time = local_time.astimezone(utc_zone)              # Change to UTC
        utc_str = utc_time.strftime('%Y-%m-%dT%H:%M:%S')
        if len(local_dt) > 19:
            utc_string = utc_str + local_dt[19:] + 'Z'       # Add MS + Z (stands for UTC)
        else:
            utc_string = utc_str + 'Z'                      # Add Z (stands for UTC)

    except:
        utc_string = local_dt

    return utc_string  # Change to the destination DATE-TIME

# =======================================================================================================================
# Change time zone or apply casting
# 1) Given a list with rows, each row is a dictionary with column names and values.
#    Use the list of DATE columns to change the date from utc to current timezone
# 2) Use the list of Casting columns to cast the values
# =======================================================================================================================
def change_columns_values(status, timezone, time_columns, casting_columns,  casting_list, title_list, data_rows):
    ret_val = process_status.SUCCESS
    key_type = 0  # only for speed - avoiding if utc_time[0] in row.keys() for every column:
    for row in data_rows:
        # Map the timezone
        for utc_time in time_columns:
            if not key_type:
                # Only once
                if utc_time[0] in row:
                    key_type = 1
                else:
                    key_type = 2

            if key_type == 1:
                key = utc_time[0]  # Key is the column id
            else:
                key = utc_time[1]  # Key is the column name

            if key in row:
                date_value = row[key]
                if  (len(date_value) > 10 and date_value[10] == 'T'):
                    format_string = UTC_TIME_FORMAT_STR
                elif not timezone or timezone == "local":
                    format_string = TIME_FORMAT_STR
                else:
                    format_string = TIME_FORMAT_STR
                if format_string:
                    # The data is flagged as UTC - or force conversion by specifying timezone is "local"
                    row[key] = utc_to_timezone(row[key], format_string, timezone)


        for index, key in enumerate(casting_columns):

            if key in row:           # either '0', '1' ... are identifying the columns
                column_name = key
            elif title_list[int(key)] in row:       # or the column names are identifying the columns
                column_name = title_list[int(key)]
            else:
                column_name = None
            if column_name:
                col_castings = casting_list[index]      # Get the custing to apply om the column
                column_val = row[column_name]

                ret_val, row[column_name] = cast_column(status, row, col_castings, column_val)
                if ret_val:
                    break

            if ret_val:
                break

        if ret_val:
            break

    return ret_val

# =======================================================================================================================
# cast to int
# =======================================================================================================================
def cast_to_int(status, row, casting_str, value ):
    try:
        casted_value = int(value)
    except:
        status.add_error(f"Casting failed with ::int on: {value}, column is not convertable to a int")
        casted_value = None

    return casted_value

# =======================================================================================================================
# cast to String
# =======================================================================================================================
def cast_to_str(status, row, casting_str, value):
    return str(value)

# =======================================================================================================================
# cast to float
# =======================================================================================================================
def cast_to_float(status, row, casting_str, value):

    ret_val = True
    if casting_str[:6] == "float(" and casting_str[-1] == ')':
        float_details = casting_str[6:-1]
        length_details = len(float_details)
        if not length_details:
            ret_val = False
        else:
            if length_details > 1 and float_details[0] == "%":
                # format
                format_value = True
                try:
                    digits = int(float_details[1:])
                except:
                    ret_val = False
            else:
                format_value = False
                try:
                    digits = int(float_details[0:])
                except:
                    ret_val = False

        if ret_val:
            # Casting of float(x) - x is the number of digits
            if isinstance(value, float):
                float_val = value
            else:
                try:
                    float_val = float(value)
                except:
                    ret_val = False

            if ret_val:
                try:
                    casted_value = round(float_val, digits)
                except:
                    ret_val = False
                else:
                    if format_value:
                        try:
                            casted_value = ("{:,.0%sf}" % digits).format(casted_value)
                        except:
                            ret_val = False
    else:
        ret_val = False

    if not ret_val:
        status.add_error(f"Casting failed with ::{casting_str} on: {value}")
        casted_value = None

    return casted_value

# =======================================================================================================================
# cast to left or right justified
# =======================================================================================================================
def cast_to_just(status, row, casting_str, value):

    if casting_str[:6] == "ljust(" or casting_str[:6] == "rjust(":
        # returns a left-justified string of a given width
        if casting_str[-1] == ')':
            try:
                length = int(casting_str[6:-1])
            except:
                status.add_error(f"Casting failed with ::{casting_str} - string length is not properly provided")
                casted_value = None
            else:
                current_str = str(value)
                current_len = len(current_str)

                if casting_str[0] == 'l':
                    if current_len < length:
                        casted_value = current_str.ljust(length)
                    elif current_len > length:
                        casted_value = current_str[:length]
                    else:
                        casted_value = current_str
                else:
                    if current_len < length:
                        casted_value = str(value).rjust(length)
                    elif current_len > length:
                        casted_value = current_str[-length:]
                    else:
                        casted_value = current_str
    else:
        casted_value = None

    return casted_value
# =======================================================================================================================
# cast with format
# =======================================================================================================================
def cast_with_format(status, row, casting_str, value):

    if casting_str[:7] == "format(" and casting_str[-1] == ')':
        casting_key = casting_str[7:-1]
        try:
            casted_value = ("{" + casting_key + "}").format(value)
        except:
            status.add_error(f"Casting failed with '{casting_key}'.format({value})")
            casted_value = None
    else:
        casted_value = None
    return casted_value

# =======================================================================================================================
# cast by function
# =======================================================================================================================
def cast_by_function(status, row, casting_str, value):

    if casting_str[:9] == "function(" and casting_str[-1] == ')':
        try:
            str_function = casting_str[9:-1]

            # Function to replace each match with the corresponding value from the dictionary
            def replace_match(match):
                key = match.group(1)
                return str(row.get(key, match.group(0)))  # Keep the placeholder if key not found

            new_string = pattern_func_.sub(replace_match, str_function)

            casted_value = eval(new_string)

        except:
            status.add_error(f"Casting using function '{casting_str}' failed")
            casted_value = None
    else:
        casted_value = None

    return casted_value
# =======================================================================================================================
# cast to date_time
# Return second or minute or hour or day or month or year
# =======================================================================================================================
def cast_to_date_time(status, row, casting_str, value):

    try:
        # Step 1: Parse the datetime string
        dt = datetime.strptime(value[:26], '%Y-%m-%d %H:%M:%S.%f')  # the microseconds can be with an extra char which leads to an error
    except:
        status.add_error(f"Casting failed to apply: datetime.strptime({value}, '%Y-%m-%d %H:%M:%S.%f') ")
        casted_value = None
    else:
       if casting_str[:9] != "datetime(" or  casting_str[-1] != ')':
           casted_value = None
           status.add_error(f"Casting of datetime is not well defined: {casting_str}")
       else:
            info_type = casting_str[9:-1]
            if len(info_type) > 2 and info_type[0] == "'" and info_type[-1] == "'":
                info_type = info_type[1:-1]     # The quotations were used to maintain capital letters -

            try:
                casted_value =  dt.strftime(info_type)      # for example info_type = %m-%Y' will return month and year
            except:
                casted_value = None
                status.add_error(f"Casting failed in extracting value using the string: {info_type}")

    return casted_value

casting_methods_ = {
    'in': cast_to_int,
    'st': cast_to_str,
    'fl' : cast_to_float,
    'lj' : cast_to_just,
    'rj' : cast_to_just,
    'fo' : cast_with_format,
    'da' : cast_to_date_time,
    'fu' : cast_by_function,
}
# =======================================================================================================================
# Apply casting on a column
# =======================================================================================================================
def cast_column(status, row, col_castings, column_val):
    '''
    col_custing - the list of casting to apply on the column: i.e. int::format(":,")
    '''
    global casting_methods_
    ret_val = process_status.SUCCESS

    casted_value = column_val

    for casting_str in col_castings:    # Can do multiple casting

        if not casted_value:
            # Deal with Null
           break

        method_index = casting_str[0:2]
        if method_index in casting_methods_:
            casted_value = casting_methods_[method_index](status, row, casting_str, casted_value)
            if casted_value == None:
                # Casting failed
                ret_val = process_status.CASTING_FAILURE
                break
        else:
            status.add_error(f"Casting failed with ::{casting_str} on: {casted_value}, casting type is not supported")
            ret_val = process_status.NON_supported_casting
            break


    return [ret_val,casted_value]

# =======================================================================================================================
# Return the data type by the cast string
# =======================================================================================================================
def cast_key_to_type(casting_per_column):

    global cast_to_type_        # a Mapping dict from casting to data type

    casting_str = casting_per_column[-1]       # Could be multiple casting for a column, take the last one

    if casting_str[-1] == ')':
        index = casting_str.rfind('(')
        if index > 0:
            key = casting_str[:index]
            if key in cast_to_type_:
                data_type = cast_to_type_[key]
            else:
                data_type = None
        else:
            data_type = None
    elif casting_str in cast_to_type_:
        data_type = cast_to_type_[casting_str]
    else:
        data_type = None

    return data_type

# =======================================================================================================================
# Validate a date string
# =======================================================================================================================
def validate_date_string(date_string, format_string=TIME_FORMAT_STR):
    date_len = len(date_string)
    if date_len == 28 and date_string[-1] == 'Z' and date_string[-2] >= '0' and  date_string[-2] <= '9':
        # datetime.strptime failed on '2019-10-11T17:05:08.0400085Z' , but does not fail if we remove last char '2019-10-11T17:05:08.040008Z'
        test_str = date_string[:-2] + 'Z'
    else:
        if date_len > 10 and date_string[-5] == '-' and date_string[-4:].isdigit():
            # Example: 2024-01-01 18:21:54.906182-0800' --> the -0800 indicates that the time is 8 hours behind UTC
            test_str = date_string[:-5]
        else:
            test_str = date_string
    try:
        datetime.strptime(test_str, format_string)
    except ValueError as error:
        ret_val = False  # wrong format
    except:
        ret_val = False  # wrong format
    else:
        ret_val = True
    return ret_val
# =======================================================================================================================
# FROM Format like: '2011-11-04T00:05:23+04:00' - https://docs.python.org/3/library/datetime.html
# To UTC string
# The UTC offset is the difference in hours and minutes between Coordinated Universal Time (UTC) and local solar time, at a particular place
# https://en.wikipedia.org/wiki/UTC_offset
# =======================================================================================================================
def time_iso_format(date_string):

    in_utc = False
    if date_string[-6] == '+':
        try:
            # Parse the input timestamp string using the original format
            timestamp = datetime.strptime(date_string, '%m/%d/%Y %I:%M:%S %p %z')
            utc_string = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        except:
            pass
        else:
            in_utc = True

    if not in_utc:
        try:
            local_time = datetime.fromisoformat(date_string)
            utc_time = local_time.astimezone(utc_zone)
            utc_str = utc_time.strftime('%Y-%m-%dT%H:%M:%S')

            if len(date_string[:-6]) > 19:
                utc_string = utc_str + date_string[19:-6] + 'Z'       # Add MS
            else:
                utc_string = utc_str
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Failed to change date-time with utc-offset value to UTC: [date-time: '%s'] [error-no: '%s'] [error-val: '%s']" % (date_string, errno, value)
            process_log.add("Error", err_msg)
            utc_string = None

    return utc_string
# =======================================================================================================================
# Transform strings provided by the user to date.
# Example -3d --> current - 3 days
# =======================================================================================================================
def input_to_date(status, value):

    if validate_date_string(value):
        time_str = value
        ret_val = True
    else:
        ret_val = False
        time_str = ""

        if len(value) >= 3:
            if value[0] == "-" or value[0] == "+":
                # test if days subtracted from current
                time_str = get_date_time_as_difference(value, None, True, "%Y-%m-%d %H:%M:%S.%f")
                if time_str:
                    ret_val = True
            elif validate_date_string(value, TIME_FORMAT_STR):
                time_str = local_to_utc(value)
                ret_val = True
            elif validate_date_string(value, UTC_TIME_FORMAT_STR):
                time_str = value
                ret_val = True
            elif value == "now()":
                time_str = get_current_time("%Y-%m-%d %H:%M:%S.%f")
            elif value[:9] == "start of ":
                if value[9:] == "year":
                    time_str = get_current_time("%Y-01-01 00:00:00.0")
                elif value[9:] == "month":
                    time_str = get_current_time("%Y-%m-01 00:00:00.0")
                elif value[9:] == "day":
                    time_str = get_current_time("%Y-%m-%d 00:00:00.0")
                elif value[9:] == "hour":
                    time_str = get_current_time("%Y-%m-%d %H:00:00.0")
                elif value[9:] == "minute":
                    time_str = get_current_time("%Y-%m-%d %H:%M:00.0")
                if time_str:
                    ret_val = True
        if not ret_val:
            status.add_error("Failed to retrieve date and time from the string provided: '%s'" % value)

    return [ret_val, time_str]

# =======================================================================================================================
# Get the date and time difference  format:
# + 1 day --> +1d
# =======================================================================================================================
def get_diff_str(date_words, offset_first):

    global time_units_char

    diff_str = ""
    offset = offset_first
    last_word = len(date_words)

    step = 1        # Need 3 steps - > 1 to get the +  or - sign, 2 to get the number, 3 to get the unit
    while offset < last_word:
        word = date_words[offset].strip()
        if step == 1:
            # Get the + or -
            sign = word[0]
            if sign != '-' and sign != '+':
                break
            step += 1       # get the number
            if len(word) == 1:
                offset += 1
                continue        # get the next word
            word = word[1:].strip()
        if step == 2:
            # get the value
            # find first char which is not a number
            number = ""
            for char in word:
                if char < '0' or char > '9':
                    break
                number += char
            if not number:
                break   # Not a number
            step += 1
            if len(number) >= len(word):           # Only number
                offset += 1
                continue       # get the next word - should be the unit
            word = word[len(number):].strip()
        if step == 3:
            # gt the unit
            unit = word
            if len(unit) == 1:      # one char representing day, month, etc.
                unit_key = unit
            else:
                if unit == "minute" or unit == "minutes":
                    unit_key = "t"
                else:
                    unit_key = unit[0]  # Other than minute, take first char

            if unit_key in time_units_char:
                if len(unit) > 1:
                    if unit[-1] == 's':
                        # remove the plural
                        singular = unit[:-1]
                    else:
                        singular = unit
                    if time_units_char[unit_key] != singular:
                        break           # Undefined unit

                diff_str = sign + number + unit_key
                offset += 1
                break
        break

    if diff_str:
        words_count = offset - offset_first + 1    # words considered to derive the string
    else:
        words_count = 0

    return [words_count, diff_str]

# =======================================================================================================================
# Get the date and time from a differece from the current date and time
# =======================================================================================================================
def get_date_time_as_difference(value, from_time, is_utc, date_format):

    time_str = ""
    if value[0] == "-" or value[0] == "+":
        # test if days subtracted from current
        time_unit_char = value[-1].lower()
        if time_unit_char in time_units_char.keys():  # Map d to days, m to month etc.
            time_unit = time_units_char[time_unit_char]
            units = value[1:-1].strip()
            if units.isdecimal():
                if from_time:
                    init_time = from_time
                else:
                    if is_utc:
                        init_time = get_current_utc_time(date_format)   # Use current UTC time
                    else:
                        init_time = get_current_time(date_format)  # Use current UTC time
                if value[0] == "-":
                    function = time_units_calculator[time_unit][0]  # function to subtract time from date
                else:
                    function = time_units_calculator[time_unit][1]  # function to add time from date
                time_str = function(init_time, int(units), date_format)

    return time_str
# =======================================================================================================================
# Test if date or date + time - return the string or None
# Example 'YYYY-MM-DD'
# =======================================================================================================================
def get_date_time_str( word, is_utc ):

    date_time = ""
    if len(word) > 11 and word[5] == '-' and word[8] == '-' and word[0] == '\'' and word[-1] == '\'':
        # test date:
        test_str = word[1:-1]

        if is_utc and (len(test_str)<=10 or test_str[10] == ' '):
            # Transform to UTC
            if validate_date_string(test_str[:19], TIME_FORMAT_STR_NO_MS):  # Milliseconds do not need to be providded
                date_time = local_to_utc(test_str) # get utc time
            else:
                date_time = test_str
        elif not is_utc and test_str[10] == 'T':
            # Transform from UTC to local
            if validate_date_string(test_str[:19],UTC_TIME_FORMAT_STR_NO_MS):      # Milliseconds do not need to be providded
                date_time = _utc_to_local(test_str)  # get utc time
            else:
                date_time = test_str
        else:
            # keep as is
            if is_utc:
                format_str = UTC_TIME_FORMAT_STR_NO_MS
            else:
                format_str = TIME_FORMAT_STR_NO_MS
            if validate_date_string(test_str[:19], format_str):
                date_time = test_str

    return date_time

# =======================================================================================================================
# Get delta to timezone (and considering daylight saving)
# =======================================================================================================================
def utc_delta():

    global utc_diff

    utc_date_time = pytz.utc.localize(datetime.utcnow())        # considers daylight savings

    utc_str = "%u-%02u-%02uT%02u:%02u:%02u.0Z"     % (utc_date_time.year, utc_date_time.month, utc_date_time.day, utc_date_time.hour, utc_date_time.minute, utc_date_time.second)

    utc_sec = get_time_in_sec(utc_str, False)     # Treat as local time

    time_local = _utc_to_local(utc_str)

    local_sec = get_time_in_sec(time_local, False)  # Treat as local time

    utc_diff = int(utc_sec - local_sec)


# =======================================================================================================================
# Get local time format as f(time string length)
# =======================================================================================================================
def get_local_time_format( date_val ):
    length = len(date_val)
    if length > 20:
        time_format = "%Y-%m-%d %H:%M:%S.%f"
    elif length == 19:
        time_format = "%Y-%m-%d %H:%M:%S"
    elif length == 10:
        time_format = "%Y-%m-%d"
    else:
        time_format = None
    return time_format
# =======================================================================================================================
# Get local time format as f(time string length)
# =======================================================================================================================
def get_utc_time_format(date_val):
    length = len(date_val)

    if length > 20:
        if date_val[-1] == 'Z':
            time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        else:
            time_format = "%Y-%m-%dT%H:%M:%S.%f"
    elif length == 20:
        time_format = "%Y-%m-%dT%H:%M:%SZ"
    elif length == 19:
        time_format = "%Y-%m-%dT%H:%M:%S"
    elif length == 11:
        time_format = "%Y-%m-%dT"
    else:
        time_format = None

    return time_format

# =======================================================================================================================
# Get timezone
# =======================================================================================================================
def get_timezone(status, io_buff_in, cmd_words, trace):

    try:
        tzone_info = str(datetime.now().astimezone().tzinfo)
    except:
        tzone_info = ""

    try:
        curr_time = time.localtime()
        curr_clock = time.strftime("%Y-%m-%d %H:%M:%S", curr_time)

        if tzone_info:
            tzone_info += " : " + curr_clock
        else:
            tzone_info = curr_clock
    except:
        if not tzone_info:
            tzone_info = "Failed to retrieve timezone info"

    return [0, tzone_info]

# =======================================================================================================================
# Is Valid timezone
# =======================================================================================================================
def is_valid_timezone(timezone):

    if get_timezone_object(timezone):
        is_valid = True
    else:
        is_valid = False
    return is_valid

# =======================================================================================================================
# map epoch to datetime
# =======================================================================================================================
def epoch_to_date_time(epoch_value):

    try:
        epoch_value = str(int(epoch_value))      # Remove fractions and make a string (if provided as an int)
        length = len(epoch_value)

        if length > 10:
            # epoch values with more than 10 digits typically represent millisecond precision,
            # dividing by 1000 is necessary to convert them to seconds.
            # Adjust the division factor accordingly if your epoch value has a different precision.
            hms_value =  int(epoch_value[:10]) # The value representing date + time without ms
            millisecond = int(epoch_value[10:17])
        else:
            # No milliseconds
            hms_value = int(epoch_value)
            millisecond = 0


        # Convert the epoch value to a datetime object
        epoch_datetime = datetime.utcfromtimestamp(hms_value)

        # Convert the datetime object to UTC time string
        utc_time = epoch_datetime.strftime('%Y-%m-%dT%H:%M:%S') + ".%sZ" % millisecond
    except:
        utc_time = None

    return utc_time

# =======================================================================================================================
# Syslog date to AnyLog date
# "Jan 18 14:40:16" -->  UTC_TIME_FORMAT_STR
# =======================================================================================================================
def format_syslog_date(syslog_format, dest_format = UTC_TIME_FORMAT_STR):

    try:
        # Get the current year
        current_year = datetime.now().year

        # Define the format string, assuming the time is in HH:MM:SS format
        format_string = '%b %d %H:%M:%S'

        # Parse the date string, adding the current year at the beginning
        date_obj = datetime.strptime(f"{current_year} {syslog_format}", f'%Y {format_string}')

        formatted_date = date_obj.strftime(f"%Y-%m-%dT%H:%M:%S.000000Z")

    except:
        formatted_date = None

    return formatted_date

# -----------------------------------------------------------------------------------------------
# String to timediff - Convert a string to a time diff
# Example: 'timestamp >= now() - 10 hours and timestamp < now() - 5 hours and id = 2'
# Example: 'timestamp >= '2024-07-09 01:59:55.856588' - 10 hours and timestamp < now() - 5 hours and id = 2'
'''
Use cases to test

where timestamp >= NOW() - 10 hours and timestamp < now()
where timestamp >= '2024-05-09'

timestamp >= '2024-05-09'
timestamp >= '2024-05-09 01:59:55.856588' - 10 hours
timestamp <= now()  and timestamp > now() - 5 hours
timestamp >= '2024-07-09 01:59:55.856588' - 10 hours and timestamp < '2024-07-09'
'''
# -----------------------------------------------------------------------------------------------
def str_to_timediff(query):
    '''
    query - the string to parse Given string
    '''

    global datetime_pattern_
    global interval_pattern_

    query_segments = query.split("and")

    datetime_matches = []       # Fixed dates - like 2024-07-06 01:59:55.856588
    interval_matches = []       # like - now() - 5 days
    for entry in query_segments:
        # Consider each part of the query to get the relevant date:

        # Extract the specific datetime - captures the datetime string in the format 'YYYY-MM-DD HH:MM:SS.ffffff'.
        datetime_match = re.search(datetime_pattern_, entry)

        # Extract intervals using regular expressions
        interval_match = re.findall(interval_pattern_, entry) # captures the interval value and the unit, allowing for plural forms.

        # Add in pairs:
        if datetime_match or interval_match:
            datetime_matches.append(datetime_match)
            interval_matches.append(interval_match[0] if interval_match else None)


    counter_date_values = len(datetime_matches)

    # Ensure intervals are extracted correctly
    if not counter_date_values:
        process_log.add("Error", "Failed to calculate time diff from: '%s'" % query)
        return None         # return an error - time was not identified

    if counter_date_values > 2:
        process_log.add("Error", "Failed to calculate time diff using more than 2 date/time references: '%s'" % query)
        return None

    if datetime_matches[0]:
        specific_datetime = get_unified_date_time(datetime_matches[0])
        if not specific_datetime:
            return None
    else:
        specific_datetime = datetime.now()

    if interval_matches[0]:
        time1 = specific_datetime - calculate_timedelta(*interval_matches[0])
    else:
        time1 = specific_datetime

    if counter_date_values >= 2:
        if datetime_matches[1]:
            specific_datetime = get_unified_date_time(datetime_matches[1])
            if not specific_datetime:
                return None
        else:
            specific_datetime = datetime.now()

        if interval_matches[1]:
            time2 = specific_datetime - calculate_timedelta(*interval_matches[1])
        else:
            time2 = specific_datetime
    else:
        time2 = datetime.now()

    time_difference = abs(time1 - time2)

    return time_difference


# -----------------------------------------------------------------------------------------------
# Calculate the time delta
# -----------------------------------------------------------------------------------------------
def calculate_timedelta(value_str, unit):
    """Helper function to calculate timedelta based on value and unit."""
    try:
        value = int(value_str)

        if unit in ['month', 'year']:
            days = value * (30 if unit == 'month' else 365)
            delta_time = timedelta(days=days)
        else:
            delta_time = timedelta(**{unit_to_timedelta_[unit]: value})
    except:
        process_log.add("Error", f"Failed to calculate time diff using '{value_str} and {unit}")
        delta_time = None

    return delta_time

# -----------------------------------------------------------------------------------------------
# Unify the date time to '%Y-%m-%d %H:%M:%S.%f'
# -----------------------------------------------------------------------------------------------
def get_unified_date_time(datetime_matches):

    try:
        date_str, hour_str, minute_str, seconds_str = datetime_matches.groups()
        if seconds_str:
            # all Info is available
            updated_datetime = f"{date_str} {hour_str}:{minute_str}:{seconds_str}"
            specific_datetime = datetime.strptime(updated_datetime, '%Y-%m-%d %H:%M:%S.%f')
        else:

            updated_datetime = date_str + " 00:00:00"

            specific_datetime = datetime.strptime(updated_datetime, '%Y-%m-%d %H:%M:%S')
    except:
        errno, value = sys.exc_info()[:2]
        process_log.add("Error", f"Failed to extract date: '{errno}' : '{value}'")
        specific_datetime = None

    return specific_datetime

# -----------------------------------------------------------------------------------------------
# seconds since the epoch for the provided date string.
# -----------------------------------------------------------------------------------------------
def string_to_seconds(date_string, date_format):

    try:
        if not date_format:
            # take the default:
            date_format = '%Y-%m-%dT%H:%M:%S.%fZ' if date_string[-1] == 'Z' else '%Y-%m-%d %H:%M:%S.%f'

        # Parse the date string into a datetime object
        dt_object = datetime.strptime(date_string, date_format)

        # Convert the datetime object to seconds since the epoch
        seconds_since_epoch = dt_object.timestamp()
    except:
        seconds_since_epoch = 0

    return seconds_since_epoch


# -----------------------------------------------------------------------------------------------
# Convert seconds of time to  seconds, minutes, hours, days, months and years
# -----------------------------------------------------------------------------------------------
def seconds_to_time(seconds):
    # Return time and unit (i.e. 3 days)

    if seconds < 60:
        return [seconds, "second"]

    for entry in second_conversions_:
        if seconds >= entry[1]:
            return [seconds / entry[1], entry[0]]
