#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

#TODO new docstring
""" This module defines functions for handling Deephaven date/time data. """

from __future__ import annotations

import datetime
from typing import Union, Optional

import jpy
import numpy as np

from deephaven import DHError
from deephaven.dtypes import Instant, LocalDate, LocalTime, ZonedDateTime, Duration, Period, TimeZone

#TODO: clean up type list
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")
_JLocalDate = jpy.get_type("java.time.LocalDate")
_JLocalTime = jpy.get_type("java.time.LocalTime")
_JInstant = jpy.get_type("java.time.Instant")
_JZonedDateTime = jpy.get_type("java.time.ZonedDateTime")
_JDuration = jpy.get_type("java.time.Duration")
_JPeriod = jpy.get_type("java.time.Period")
_epoch64 = np.datetime64('1970-01-01T00:00:00Z')


# region Clock

#TODO: rename these methods to system_<X>?

#TODO: what should these return?
def now(system: bool = False, resolution: str = 'ns') -> Instant:
    """ Provides the current datetime according to a clock.

    Args:
        system (bool): True to use the system clock; False to use the default clock.  Under most circumstances,
            the default clock will return the current system time, but during replay simulations, the default
            clock can return the replay time.

        resolution (str): The resolution of the returned time.  The default 'ns' will return nanosecond resolution times
            if possible. 'ms' will return millisecond resolution times.

    Returns:
        Instant

    Raises:
        DHError
    """
    try:
        if resolution == "ns":
            if system:
                return _JDateTimeUtils.nowSystem()
            else:
                return _JDateTimeUtils.now()
        elif resolution == "ms":
            if system:
                return _JDateTimeUtils.nowSystemMillisResolution()
            else:
                return _JDateTimeUtils.nowMillisResolution()
        else:
            raise ValueError("Unsupported time resolution: " + resolution)
    except Exception as e:
        raise DHError(e) from e

#TODO: what should these return?

def today(tz: TimeZone) -> str:
    """ Provides the current date string according to the current clock.
    Under most circumstances, this method will return the date according to current system time,
    but during replay simulations, this method can return the date according to replay time.

    Args:
        tz (TimeZone): Time zone to use when determining the date.

    Returns:
        Date string

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.today(tz)
    except Exception as e:
        raise DHError(e) from e


# endregion


# region Time Zone


##TODO: remove?
# def time_zone(tz: Optional[str]) -> TimeZone:
#     """ Gets the time zone for a time zone name.
#
#     Args:
#         tz (Optional[str]): Time zone name.  If None is provided, the system default time zone is returned.
#
#     Returns:
#         TimeZone
#
#     Raises:
#         DHError
#     """
#     try:
#         if tz is None:
#             return _JDateTimeUtils.timeZone()
#         else:
#             return _JDateTimeUtils.timeZone(tz)
#     except Exception as e:
#         raise DHError(e) from e


def time_zone_alias_add(alias: str, tz: str) -> None:
    """ Adds a new time zone alias.

    Args:
        alias (str): Alias name.
        tz (str): Time zone name.

    Returns:
        None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.timeZoneAliasAdd(alias, tz)
    except Exception as e:
        raise DHError(e) from e


def time_zone_alias_rm(alias: str) -> bool:
    """ Removes a time zone alias.

    Args:
        alias (str): Alias name.

    Returns:
        True if the alias was present; False if the alias was not present.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.timeZoneAliasRm(alias)
    except Exception as e:
        raise DHError(e) from e


# endregion

#TODO: Document

#TODO: numpy `astype(type)` syntax?

# region Conversions: To Java

#TODO: TZ input?
#TODO: rename as_j_time or asjtype
#TODO: rename everything
#TODO: consistently name functions
#TODO: have these methods parse strings?
#TODO: convert python time zones?

def to_j_time_zone(tz: Union[None, str]) -> Optional[TimeZone]:
    if not tz:
        return None
    else:
        return _JDateTimeUtils.timeZone(tz)
        #TODO: or _JDateTimeUtils.parseTimeZone(s)


def to_j_date(dt: Union[None, datetime.date, datetime.time, datetime.datetime, np.datetime64]) -> Optional[LocalDate]:
    if not dt:
        return None
    elif isinstance(dt, datetime.date) or isinstance(dt, datetime.datetime):
        return _JLocalDate.of(dt.year, dt.month, dt.day)
    elif isinstance(dt, np.datetime64):
        return to_j_date(dt.astype(datetime.date))
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> LocalDate")


def to_j_time(dt: Union[None, datetime.time, datetime.datetime, np.datetime64]) -> Optional[LocalTime]:
    if not dt:
        return None
    elif isinstance(dt, datetime.time) or isinstance(dt, datetime.datetime):
        return _JLocalTime.of(dt.hour, dt.minute, dt.second, dt.microsecond * 1000)
    elif isinstance(dt, np.datetime64):
        # Conversion only supports micros resolution
        return to_j_time(dt.astype(datetime.time))
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> LocalTime")


def to_j_instant(dt: Union[None, datetime.datetime, np.datetime64]) -> Optional[Instant]:
    if not dt:
        return None
    elif isinstance(dt, datetime.datetime):
        epoch_time = dt.timestamp()
        epoch_sec = int(epoch_time)
        nanos = (epoch_time - epoch_sec) * 1000000000
        return _JInstant.ofEpochSecond(epoch_sec, nanos)
    elif isinstance(dt, np.datetime64):
        epoch_nanos = (dt - _epoch64).astype('timedelta64[ns]').astype(np.int64)
        epoch_sec = epoch_nanos // 1000000000
        nanos = epoch_nanos % 1000000000
        return _JInstant.ofEpochSecond(epoch_sec, nanos)
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Instant")

#TODO: ZDT?

def to_j_duration(dt: Union[None, datetime.timedelta, np.timedelta64]) -> Optional[Duration]:
    if not dt:
        return None
    elif isinstance(dt, datetime.timedelta):
        nanos = (dt / datetime.timedelta(microseconds=1)) * 1000
        return _JDuration.ofNanos(nanos)
    elif isinstance(dt, np.timedelta64):
        nanos = dt.astype('timedelta64[ns]').astype(np.int64)
        return _JDuration.ofNanos(nanos)
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Duration")


def to_j_period(dt: Union[None, datetime.timedelta, np.timedelta64]) -> Optional[Period]:
    if not dt:
        return None
    elif isinstance(dt, datetime.timedelta):
        if dt.seconds or dt.microseconds:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Period: Periods must only be days or weeks")
        elif dt.days:
            return _JPeriod.ofDays(dt.days)
        else:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Period")
    elif isinstance(dt, np.timedelta64):
        data = np.datetime_data(dt)

        if data[0] == 'D':
            return _JPeriod.ofDays(data[1])
        elif data[0] == 'W':
            return _JPeriod.ofDays(data[1] * 7)
        elif data[0] == 'M':
            return _JPeriod.ofMonths(data[1])
        elif data[0] == 'Y':
            return _JPeriod.ofYears(data[1])
        else:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Period: numpy.datetime64 must have units of 'D', 'W', 'M', or 'Y'")
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> Period")



# endregion


# region Conversions: From Java


def to_date(dt: Union[None, LocalDate]) -> Optional[datetime.date]:
    if not dt:
        return None
    if isinstance(dt, LocalDate):
        return datetime.date(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth())
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.date")


def to_time(dt: Union[None, LocalTime]) -> Optional[datetime.time]:
    if not dt:
        return None
    elif isinstance(dt, LocalTime):
        return datetime.time(dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano() // 1000)
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.time")


def to_datetime(dt: Union[None, Instant, ZonedDateTime]) -> Optional[datetime.datetime]:
    if not dt:
        return None
    elif isinstance(dt, Instant):
        ts = dt.getEpochSecond() + (dt.getNano() / 1000000000)
        return datetime.datetime.fromtimestamp(ts)
    elif isinstance(dt, ZonedDateTime):
        ts = dt.toEpochSecond() + (dt.getNano() / 1000000000)
        return datetime.datetime.fromtimestamp(ts)
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")

def to_datetime64(dt: Union[None, Instant, ZonedDateTime]) -> Optional[np.datetime64]:
    if not dt:
        return None
    elif isinstance(dt, Instant):
        ts = dt.getEpochSecond() * 1000000000 + dt.getNano()
        return np.datetime64(ts, 'ns')
    elif isinstance(dt, ZonedDateTime):
        ts = dt.toEpochSecond() * 1000000000 + dt.getNano()
        return np.datetime64(ts, 'ns')
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")

def to_timedelta(dt: Union[None, Duration]) -> Optional[datetime.timedelta]:
    if not dt:
        return None
    elif isinstance(dt, Duration):
        return datetime.timedelta(seconds=dt.getSeconds(), microseconds=dt.getNano() // 1000)
    elif isinstance(dt, Period):
        #TODO: not sure what is right here
        y = dt.getYears()
        m = dt.getMonths()
        d = dt.getDays()
        w = dt.getDays() // 7

        if y or m:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta: Periods must only be days or weeks")

        return datetime.timedelta(days=d, weeks=w)
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta")


def to_timedelta64(dt: Union[None, Duration, Period]) -> Optional[np.timedelta64]:
    if not dt:
        return None
    elif isinstance(dt, Duration):
        return np.timedelta64(dt.toNanos(), 'ns')
    elif isinstance(dt, Period):
        d = dt.getDays()
        m = dt.getMonths()
        y = dt.getYears()

        count = (1 if d else 0) + (1 if m else 0) + (1 if y else 0)

        if count == 0:
            return np.timedelta64(0, 'D')
        elif count > 1:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64: Periods must be days, months, or years")
        elif y:
            return np.timedelta64(y, 'Y')
        elif m:
            return np.timedelta64(m, 'M')
        elif d:
            return np.timedelta64(d, 'D')
        else:
            raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64: (" + dt + ")")
    else:
        raise DHError(message="Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta")


# endregion

########################################################################################################################


# region XXXX

def to_time_zone(tz: Optional[str]**) -> TimeZone:
    """ Gets the time zone for a time zone name.

    Args:
        tz (Optional[str]): Time zone name.  If None is provided, the system default time zone is returned.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    ***
    try:
        if tz is None:
            return _JDateTimeUtils.timeZone()
        else:
            return _JDateTimeUtils.timeZone(tz)
    except Exception as e:
        raise DHError(e) from e




datetime.date
datetime.time
datetime.datetime
datetime.timedelta
datetime.tzinfo
datetime.timezone
np.datetime64
np.timedelta64



# endregion


# #TODO: Keep?
# region Parse

def parse_time_zone(s: str, quiet: bool = False) -> Optional[TimeZone]:
    """ Parses the string argument as a time zone.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        Time Zone

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseTimeZoneQuiet(s)
        else:
            return _JDateTimeUtils.parseTimeZone(s)
    except Exception as e:
        raise DHError(e) from e


#TODO: remove?
def parse_duration_nanos(s: str, quiet: bool = False) -> int:
    """ Parses the string argument as a time duration in nanoseconds.

    Time duration strings can be formatted as '[-]PT[-]hh:mm:[ss.nnnnnnnnn]' or as a duration string
    formatted as '[-]PnDTnHnMn.nS}'.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.
            False will cause NULL_LONG to be returned.

    Returns:
        number of nanoseconds represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseDurationNanosQuiet(s)
        else:
            return _JDateTimeUtils.parseDurationNanos(s)
    except Exception as e:
        raise DHError(e) from e


def parse_period(s: str, quiet: bool = False) -> Optional[Period]:
    """ Parses the string argument as a period, which is a unit of time in terms of calendar time
    (days, weeks, months, years, etc.).

    Period strings are formatted according to the ISO-8601 duration format as 'PnYnMnD' and 'PnW', where the
    coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    begin with a negative sign.

    Examples:
      "P2Y"             -- 2 Years
      "P3M"             -- 3 Months
      "P4W"             -- 4 Weeks
      "P5D"             -- 5 Days
      "P1Y2M3D"         -- 1 Year, 2 Months, 3 Days
      "P-1Y2M"          -- -1 Year, 2 Months
      "-P1Y2M"          -- -1 Year, -2 Months

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        Period represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parsePeriodQuiet(s)
        else:
            return _JDateTimeUtils.parsePeriod(s)
    except Exception as e:
        raise DHError(e) from e


def parse_duration(s: str, quiet: bool = False) -> Optional[Duration]:
    """ Parses the string argument as a duration, which is a unit of time in terms of clock time
    (24-hour days, hours, minutes, seconds, and nanoseconds).

    Duration strings are formatted according to the ISO-8601 duration format as '[-]PnDTnHnMn.nS', where the
    coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    begin with a negative sign.

    Examples:
       "PT20.345S" -- parses as "20.345 seconds"
       "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
       "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
       "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
       "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
       "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
       "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
       "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        Period represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseDurationQuiet(s)
        else:
            return _JDateTimeUtils.parseDuration(s)
    except Exception as e:
        raise DHError(e) from e


#TODO: remove?
def parse_epoch_nanos(s: str, quiet: bool = False) -> int:
    """ Parses the string argument as nanoseconds since the Epoch.

    Date time strings are formatted according to the ISO 8601 date time format
    'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch.  Expected date ranges are used to infer the units.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause NULL_LONG to be returned.

    Returns:
        Instant represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseEpochNanosQuiet(s)
        else:
            return _JDateTimeUtils.parseEpochNanos(s)
    except Exception as e:
        raise DHError(e) from e


def parse_instant(s: str, quiet: bool = False) -> Optional[Instant]:
    """ Parses the string argument as an Instant.

    Date time strings are formatted according to the ISO 8601 date time format
    'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch.  Expected date ranges are used to infer the units.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        Instant represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseInstantQuiet(s)
        else:
            return _JDateTimeUtils.parseInstant(s)
    except Exception as e:
        raise DHError(e) from e


def parse_zdt(s: str, quiet: bool = False) -> Optional[ZonedDateTime]:
    """ Parses the string argument as a ZonedDateTime.

    Date time strings are formatted according to the ISO 8601 date time format
    '{@code 'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        Instant represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseZonedDateTimeQuiet(s)
        else:
            return _JDateTimeUtils.parseZonedDateTime(s)
    except Exception as e:
        raise DHError(e) from e



def parse_local_date(s: str, quiet: bool = False) -> Optional[LocalTime]:
    """ Parses the string argument as a local date, which is a date without a time or time zone.

    Date strings are formatted according to the ISO 8601 date time format as 'YYYY-MM-DD}'.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  True will cause None to be returned.

    Returns:
        LocalDate represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseLocalDateQuiet(s)
        else:
            return _JDateTimeUtils.parseLocalDate(s)
    except Exception as e:
        raise DHError(e) from e


def parse_local_time(s: str, quiet: bool = False) -> Optional[LocalTime]:
    """ Parses the string argument as a local time, which is the time that would be read from a clock and
    does not have a date or timezone.

    Local time strings can be formatted as 'hh:mm:ss[.nnnnnnnnn]'.

    Args:
        s (str): String to be converted.
        quiet (bool): False will cause exceptions when strings can not be parsed.  True will cause None to be returned.

    Returns:
        LocalTime represented by the string.

    Raises:
        DHError
    """
    try:
        if quiet:
            return _JDateTimeUtils.parseLocalTimeQuiet(s)
        else:
            return _JDateTimeUtils.parseLocalTime(s)
    except Exception as e:
        raise DHError(e) from e

# endregion
