#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for handling Deephaven date/time data. """

from __future__ import annotations

import datetime
from typing import Union, Optional

import jpy
import numpy as np

from deephaven import DHError
from deephaven.dtypes import Instant, LocalDate, LocalTime, ZonedDateTime, Duration, Period, TimeZone

# TODO: clean up type list
_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")
_JLocalDate = jpy.get_type("java.time.LocalDate")
_JLocalTime = jpy.get_type("java.time.LocalTime")
_JInstant = jpy.get_type("java.time.Instant")
_JZonedDateTime = jpy.get_type("java.time.ZonedDateTime")
_JDuration = jpy.get_type("java.time.Duration")
_JPeriod = jpy.get_type("java.time.Period")
_epoch64 = np.datetime64('1970-01-01T00:00:00Z')


# TODO: Document


# region Clock

# TODO: rename these methods to system_<X>?

# TODO: what should these return?
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


# TODO: what should these return?

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

#TODO: Review all function names: to_j_<xyz>?  to_db_<xyz>?  to_dh_<xyz>?  as_j_<xyz>?  as_db_<xyz>?  as_dh_<xyz>?
#TODO: In doc strings use "Deephaven <type>" or "Java <type>"

#TODO "python type" in docs!!!!!

# region Conversions: Python To Java

def to_j_time_zone(tz: Union[None, str]) -> Optional[TimeZone]:
    """
    Converts a python type to a Deephaven time zone.

    Args:
        tz (Union[None, str]): A Python time zone or time zone string.  If None is provided, the system default
            time zone is returned.  If a string is provided, it is parsed as a time zone name.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    try:
        if not tz:
            return _JDateTimeUtils.timeZone()
            #TODO: return None
            #TODO: return system default? -> if return default, then the result is not optional
            # return _JDateTimeUtils.timeZone()

        # TODO: convert python time zones? datetime.tzinfo, datetime.timezone
        else:
            return _JDateTimeUtils.parseTimeZone(tz)
    except Exception as e:
        raise DHError(e) from e


def to_j_local_date(dt: Union[None, str, datetime.date, datetime.time, datetime.datetime, np.datetime64]) -> \
        Optional[LocalDate]:
    """
    Converts a python type to a Deephaven local date, which is a date without a time or time zone.

    Date strings can be formatted according to the ISO 8601 date time format as 'YYYY-MM-DD'.

    Args:
        dt (Union[None, str, datetime.date, datetime.time, datetime.datetime, np.datetime64]): A Python date, date time,
            or date string.  If None is provided, None is returned.

    Returns:
        LocalDate

    Raises:
        DHError
    """

    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseLocalDate(dt)
        elif isinstance(dt, datetime.date) or isinstance(dt, datetime.datetime):
            return _JLocalDate.of(dt.year, dt.month, dt.day)
        elif isinstance(dt, np.datetime64):
            return to_j_local_date(dt.astype(datetime.date))
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> LocalDate")
    except Exception as e:
        raise DHError(e) from e


def to_j_local_time(dt: Union[None, str, datetime.time, datetime.datetime, np.datetime64]) -> Optional[LocalTime]:
    """
    Converts a python type to a Deephaven local time, which is the time that would be read from a clock and does not
    have a date or timezone.

    Time strings can be formatted as 'hh:mm:ss[.nnnnnnnnn]'.

    Args:
        dt (Union[None, str, datetime.time, datetime.datetime, np.datetime64]): A Python time, date time, or
            time string.  If None is provided, None is returned.

    Returns:
        LocalTime

    Raises:
        DHError
    """

    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseLocalTime(dt)
        elif isinstance(dt, datetime.time) or isinstance(dt, datetime.datetime):
            return _JLocalTime.of(dt.hour, dt.minute, dt.second, dt.microsecond * 1000)
        elif isinstance(dt, np.datetime64):
            # Conversion only supports micros resolution
            return to_j_local_time(dt.astype(datetime.time))
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> LocalTime")
    except Exception as e:
        raise DHError(e) from e


def to_j_instant(dt: Union[None, str, datetime.datetime, np.datetime64]) -> Optional[Instant]:
    """
    Converts a python type to a Deephaven instant, which is a point in time on the time-line.

    Instant strings can be formatted according to the ISO 8601 date time format
    'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch.  Expected date ranges are used to infer the units.

    Args:
        dt (Union[None, str, datetime.datetime, np.datetime64]): A Python date time or date time string.  If None is
            provided, None is returned.

    Returns:
        Instant

    Raises:
        DHError
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseInstant(dt)
        elif isinstance(dt, datetime.datetime):
            epoch_time = dt.timestamp()
            epoch_sec = int(epoch_time)
            nanos = int((epoch_time - epoch_sec) * 1000000000)
            return _JInstant.ofEpochSecond(epoch_sec, nanos)
        elif isinstance(dt, np.datetime64):
            epoch_nanos = (dt - _epoch64).astype('timedelta64[ns]').astype(np.int64)
            epoch_sec = int(epoch_nanos // 1000000000)
            nanos = int(epoch_nanos % 1000000000)
            return _JInstant.ofEpochSecond(epoch_sec, nanos)
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> Instant")
    except Exception as e:
        raise DHError(e) from e


def to_j_zdt(dt: Union[None, str, datetime.datetime, np.datetime64]) -> Optional[ZonedDateTime]:
    """
    Converts a python type to a Deephaven zoned date time, which is a date time with a time zone.

    Date time strings can be formatted according to the ISO 8601 date time format
    '{@code 'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch.  Expected date ranges are used to infer the units.

    Args:
        dt (Union[None, str, datetime.datetime, np.datetime64]): A Python date time or date time string.  If None is
            provided, None is returned.

    Returns:
        ZonedDateTime

    Raises:
        DHError
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseZonedDateTime(dt)
        #TODO: support datetime types
        # elif isinstance(dt, datetime.datetime):
        #     epoch_time = dt.timestamp()
        #     epoch_sec = int(epoch_time)
        #     nanos = (epoch_time - epoch_sec) * 1000000000
        #     return _JZonedDateTime.ofInstant(_JInstant.ofEpochSecond(epoch_sec, nanos), ***_JDateTimeUtils.timeZone())
        # elif isinstance(dt, np.datetime64):
        #     epoch_nanos = (dt - _epoch64).astype('timedelta64[ns]').astype(np.int64)
        #     epoch_sec = epoch_nanos // 1000000000
        #     nanos = epoch_nanos % 1000000000
        #     return _JZonedDateTime.ofInstant(_JInstant.ofEpochSecond(epoch_sec, nanos), ***_JDateTimeUtils.timeZone())
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> ZonedDateTime")
    except Exception as e:
        raise DHError(e) from e


def to_j_duration(dt: Union[None, str, datetime.timedelta, np.timedelta64]) -> Optional[Duration]:
    """
    Converts a python type to a Deephaven duration, which is a unit of time in terms of clock time
    (24-hour days, hours, minutes, seconds, and nanoseconds).

    Duration strings can be formatted according to the ISO-8601 duration format as '[-]PnDTnHnMn.nS', where the
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
        dt (Union[None, str, datetime.timedelta, np.timedelta64]): A Python duration or duration string.  If None is
            provided, None is returned.

    Returns:
        Duration

    Raises:
        DHError
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseDuration(dt)
        elif isinstance(dt, datetime.timedelta):
            nanos = int((dt / datetime.timedelta(microseconds=1)) * 1000)
            return _JDuration.ofNanos(nanos)
        elif isinstance(dt, np.timedelta64):
            nanos = int(dt.astype('timedelta64[ns]').astype(np.int64))
            return _JDuration.ofNanos(nanos)
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> Duration")
    except Exception as e:
        raise DHError(e) from e


def to_j_period(dt: Union[None, str, datetime.timedelta, np.timedelta64]) -> Optional[Period]:
    """
    Converts a python type to a Deephaven period, which is a unit of time in terms of calendar time
    (days, weeks, months, years, etc.).

    Period strings can be formatted according to the ISO-8601 duration format as 'PnYnMnD' and 'PnW', where the
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
        dt (Union[None, str, datetime.timedelta, np.timedelta64]): A Python period or period string.  If None is
            provided, None is returned.

    Returns:
        Period
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, str):
            return _JDateTimeUtils.parsePeriod(dt)
        elif isinstance(dt, datetime.timedelta):
            if dt.seconds or dt.microseconds:
                raise Exception("Unsupported conversion: " + str(type(dt)) +
                                " -> Period: Periods must only be days or weeks")
            elif dt.days:
                return _JPeriod.ofDays(dt.days)
            else:
                raise Exception("Unsupported conversion: " + str(type(dt)) + " -> Period")
        elif isinstance(dt, np.timedelta64):
            data = np.datetime_data(dt)
            units = data[0]
            value = int(dt.astype(np.int64))

            if units == 'D':
                return _JPeriod.ofDays(value)
            elif units == 'W':
                return _JPeriod.ofDays(value * 7)
            elif units == 'M':
                return _JPeriod.ofMonths(value)
            elif units == 'Y':
                return _JPeriod.ofYears(value)
            else:
                raise Exception("Unsupported conversion: " + str(
                    type(dt)) + " -> Period: numpy.datetime64 must have units of 'D', 'W', 'M', or 'Y'")
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> Period")
    except Exception as e:
        raise DHError(e) from e


# endregion


# region Conversions: Java To Python


def to_date(dt: Union[None, LocalDate]) -> Optional[datetime.date]:
    """
    Converts a Deephaven local date to a python date.

    Args:
        dt (Union[None, LocalDate]): A Deephaven local date.  If None is provided, None is returned.

    Returns:
        datetime.date
    """
    try:
        if not dt:
            return None
        if isinstance(dt, LocalDate):
            return datetime.date(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth())
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.date")
    except Exception as e:
        raise DHError(e) from e


def to_time(dt: Union[None, LocalTime]) -> Optional[datetime.time]:
    """
    Converts a Deephaven local time to a python time.

    Args:
        dt (Union[None, LocalTime]): A Deephaven local time.  If None is provided, None is returned.

    Returns:
        datetime.time
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, LocalTime):
            return datetime.time(dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano() // 1000)
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.time")
    except Exception as e:
        raise DHError(e) from e


def to_datetime(dt: Union[None, Instant, ZonedDateTime]) -> Optional[datetime.datetime]:
    """
    Converts a Deephaven instant or zoned date time to a python date time.

    Args:
        dt (Union[None, Instant, ZonedDateTime]): A Deephaven instant or zoned date time.  If None is provided, None is
            returned.

    Returns:
        datetime.datetime
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, Instant):
            ts = dt.getEpochSecond() + (dt.getNano() / 1000000000)
            return datetime.datetime.fromtimestamp(ts)
        elif isinstance(dt, ZonedDateTime):
            ts = dt.toEpochSecond() + (dt.getNano() / 1000000000)
            return datetime.datetime.fromtimestamp(ts)
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")
    except Exception as e:
        raise DHError(e) from e


def to_datetime64(dt: Union[None, Instant, ZonedDateTime]) -> Optional[np.datetime64]:
    """
    Converts a Deephaven instant or zoned date time to a numpy datetime64.

    Args:
        dt (Union[None, Instant, ZonedDateTime]): A Deephaven instant or zoned date time.  If None is provided, None is
            returned.

    Returns:
        np.datetime64

    Raises:
        DHError
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, Instant):
            ts = dt.getEpochSecond() * 1000000000 + dt.getNano()
            return np.datetime64(ts, 'ns')
        elif isinstance(dt, ZonedDateTime):
            ts = dt.toEpochSecond() * 1000000000 + dt.getNano()
            return np.datetime64(ts, 'ns')
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")
    except Exception as e:
        raise DHError(e) from e


def to_timedelta(dt: Union[None, Duration]) -> Optional[datetime.timedelta]:
    """
    Converts a Deephaven duration to a python timedelta.

    Args:
        dt (Union[None, Duration]): A Deephaven duration.  If None is provided, None is returned.

    Returns:
        datetime.timedelta
    """
    try:
        if not dt:
            return None
        elif isinstance(dt, Duration):
            return datetime.timedelta(seconds=dt.getSeconds(), microseconds=dt.getNano() // 1000)
        elif isinstance(dt, Period):
            # TODO: not sure what is right here
            y = dt.getYears()
            m = dt.getMonths()
            d = dt.getDays()
            w = dt.getDays() // 7

            if y or m:
                raise Exception("Unsupported conversion: " + str(type(dt)) +
                                " -> datetime.timedelta: Periods must only be days or weeks")

            return datetime.timedelta(days=d, weeks=w)
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta")
    except Exception as e:
        raise DHError(e) from e


def to_timedelta64(dt: Union[None, Duration, Period]) -> Optional[np.timedelta64]:
    """
    Converts a Deephaven duration or period to a numpy timedelta64.

    Args:
        dt (Union[None, Duration, Period]): A Deephaven duration or period.  If None is provided, None is returned.

    Returns:
        np.timedelta64

    Raises:
        DHError
    """
    try:
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
                raise Exception("Unsupported conversion: " + str(type(dt)) +
                                " -> datetime.timedelta64: Periods must be days, months, or years")
            elif y:
                return np.timedelta64(y, 'Y')
            elif m:
                return np.timedelta64(m, 'M')
            elif d:
                return np.timedelta64(d, 'D')
            else:
                raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64: (" + dt + ")")
        else:
            raise Exception("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64")
    except Exception as e:
        raise DHError(e) from e


# endregion
