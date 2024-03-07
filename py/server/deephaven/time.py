#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for handling Deephaven date/time data. """

import datetime
from typing import Union, Optional, Literal

import jpy
import numpy
import pandas

from deephaven import DHError
from deephaven.dtypes import Instant, LocalDate, LocalTime, ZonedDateTime, Duration, Period, TimeZone

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")
_JPythonTimeComponents = jpy.get_type("io.deephaven.integrations.python.PythonTimeComponents")
_JLocalDate = jpy.get_type("java.time.LocalDate")
_JLocalTime = jpy.get_type("java.time.LocalTime")
_JInstant = jpy.get_type("java.time.Instant")
_JZonedDateTime = jpy.get_type("java.time.ZonedDateTime")
_JDuration = jpy.get_type("java.time.Duration")
_JPeriod = jpy.get_type("java.time.Period")

_NANOS_PER_SECOND = 1000000000
_NANOS_PER_MICRO = 1000


# region Clock


def dh_now(system: bool = False, resolution: Literal["ns", "ms"] = "ns") -> Instant:
    """ Provides the current datetime according to the current Deephaven clock.

    Query strings should use the built-in "now" function instead of this function.
    The build-in "now" function is pure Java and will be more efficient
    because fewer Java/Python boundary crossings will be needed.

    Args:
        system (bool): True to use the system clock; False to use the default clock.  Under most circumstances,
            the default clock will return the current system time, but during replay simulations, the default
            clock can return the replay time.
        resolution (str): The resolution of the returned time.  The default "ns" will return nanosecond resolution times
            if possible. "ms" will return millisecond resolution times.

    Returns:
        Instant

    Raises:
        DHError, TypeError
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
            raise TypeError("Unsupported time resolution: " + resolution)
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def dh_today(tz: Optional[TimeZone] = None) -> str:
    """ Provides the current date string according to the current Deephaven clock.
    Under most circumstances, this method will return the date according to current system time,
    but during replay simulations, this method can return the date according to replay time.

    Query strings should use the built-in "today" function instead of this function.
    The build-in "today" function is pure Java and will be more efficient
    because fewer Java/Python boundary crossings will be needed.

    Args:
        tz (TimeZone): Time zone to use when determining the date.
            If None is provided, the Deephaven system default time zone is used.

    Returns:
        Date string

    Raises:
        DHError
    """
    try:
        if tz is None:
            tz = _JDateTimeUtils.timeZone()

        return _JDateTimeUtils.today(tz)
    except Exception as e:
        raise DHError(e) from e


def dh_time_zone() -> TimeZone:
    """ Provides the current Deephaven system time zone.

    Query strings should use the built-in "timeZone" function instead of this function.
    The build-in "timeZone" function is pure Java and will be more efficient
    because fewer Java/Python boundary crossings will be needed.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.timeZone()
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


# region Conversions: Python To Java


def to_j_time_zone(tz: Union[None, TimeZone, str, datetime.tzinfo, datetime.datetime, pandas.Timestamp]) -> \
        Optional[TimeZone]:
    """
    Converts a time zone value to a Java TimeZone.
    Time zone values can be None, a Java TimeZone, a string, a datetime.tzinfo, a datetime.datetime,
    or a pandas.Timestamp.

    Args:
        tz (Union[None, TimeZone, str, datetime.tzinfo, datetime.datetime, pandas.Timestamp]): A time zone value.
            If None is provided, None is returned.
            If a string is provided, it is parsed as a time zone name.

    Returns:
        TimeZone

    Raises:
        DHError, TypeError
    """
    try:
        if tz is None or pandas.isnull(tz):
            return None
        elif isinstance(tz, TimeZone.j_type):
            return tz
        elif isinstance(tz, str):
            return _JDateTimeUtils.parseTimeZone(tz)
        elif isinstance(tz, datetime.tzinfo):
            return _JDateTimeUtils.parseTimeZone(str(tz))
        elif isinstance(tz, datetime.datetime):
            if not tz.tzname():
                return _JDateTimeUtils.parseTimeZone(tz.astimezone().tzname())

            return _JDateTimeUtils.parseTimeZone(tz.tzname())
        else:
            raise TypeError("Unsupported conversion: " + str(type(tz)) + " -> TimeZone")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_local_date(dt: Union[None, LocalDate, str, datetime.date, datetime.datetime,
                              numpy.datetime64, pandas.Timestamp]) -> Optional[LocalDate]:
    """
    Converts a date time value to a Java LocalDate.
    Date time values can be None, a Java LocalDate, a string, a datetime.date, a datetime.datetime,
    a numpy.datetime64, or a pandas.Timestamp.

    Date strings can be formatted according to the ISO 8601 date time format as 'YYYY-MM-DD'.

    Args:
        dt (Union[None, LocalDate, str, datetime.date, datetime.datetime, numpy.datetime64, pandas.Timestamp]): 
            A date time value. If None is provided, None is returned.

    Returns:
        LocalDate

    Raises:
        DHError, TypeError
    """

    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, LocalDate.j_type):
            return dt
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseLocalDate(dt)
        elif isinstance(dt, datetime.date) or isinstance(dt, datetime.datetime) or isinstance(dt, pandas.Timestamp):
            return _JLocalDate.of(dt.year, dt.month, dt.day)
        elif isinstance(dt, numpy.datetime64):
            return to_j_local_date(dt.astype(datetime.date))
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> LocalDate")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_local_time(dt: Union[None, LocalTime, int, str, datetime.time, datetime.datetime,
                              numpy.datetime64, pandas.Timestamp]) -> Optional[LocalTime]:
    """
    Converts a date time value to a Java LocalTime.
    Date time values can be None, a Java LocalTime, an int, a string, a datetime.time, a datetime.datetime,
    a numpy.datetime64, or a pandas.Timestamp.

    int values are the number of nanoseconds since the start of the day.

    Time strings can be formatted as 'hh:mm:ss[.nnnnnnnnn]'.

    Args:
        dt (Union[None, LocalTime, int, str, datetime.time, datetime.datetime, numpy.datetime64, pandas.Timestamp]):
            A date time value.  If None is provided, None is returned.

    Returns:
        LocalTime

    Raises:
        DHError, TypeError
    """

    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, LocalTime.j_type):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _JLocalTime.ofNanoOfDay(dt)
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseLocalTime(dt)
        elif isinstance(dt, pandas.Timestamp):
            return _JLocalTime.of(dt.hour, dt.minute, dt.second, dt.microsecond * _NANOS_PER_MICRO + dt.nanosecond)
        elif isinstance(dt, datetime.time) or isinstance(dt, datetime.datetime):
            return _JLocalTime.of(dt.hour, dt.minute, dt.second, dt.microsecond * _NANOS_PER_MICRO)
        elif isinstance(dt, numpy.datetime64):
            # Conversion only supports micros resolution
            return to_j_local_time(dt.astype(datetime.time))
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> LocalTime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_instant(dt: Union[None, Instant, int, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]) -> \
        Optional[Instant]:
    """
    Converts a date time value to a Java Instant.
    Date time values can be None, a Java Instant, an int, a string, a datetime.datetime,
    a numpy.datetime64, or a pandas.Timestamp.

    int values are the number of nanoseconds since the Epoch.

    Instant strings can be formatted according to the ISO 8601 date time format
    'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ' and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch.  Expected date ranges are used to infer the units.

    Args:
        dt (Union[None, Instant, int, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]): A date time value.
            If None is provided, None is returned.

    Returns:
        Instant, TypeError

    Raises:
        DHError
    """
    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, Instant.j_type):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _JDateTimeUtils.epochNanosToInstant(dt)
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseInstant(dt)
        elif isinstance(dt, datetime.datetime):
            epoch_time = dt.timestamp()
            epoch_sec = int(epoch_time)
            nanos = int((epoch_time - epoch_sec) * _NANOS_PER_SECOND)
            return _JInstant.ofEpochSecond(epoch_sec, nanos)
        elif isinstance(dt, numpy.datetime64):
            epoch_nanos = dt.astype('datetime64[ns]').astype(numpy.int64)
            epoch_sec, nanos = divmod(epoch_nanos, _NANOS_PER_SECOND)
            return _JInstant.ofEpochSecond(int(epoch_sec), int(nanos))
        elif isinstance(dt, pandas.Timestamp):
            return _JDateTimeUtils.epochNanosToInstant(dt.value)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Instant")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_zdt(dt: Union[None, ZonedDateTime, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]) -> \
        Optional[ZonedDateTime]:
    """
    Converts a date time value to a Java ZonedDateTime.
    Date time values can be None, a Java ZonedDateTime, a string, a datetime.datetime,
    a numpy.datetime64, or a pandas.Timestamp.

    Date time strings can be formatted according to the ISO 8601 date time format
    ``'yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ'`` and others.
    Additionally, date time strings can be integer values that are nanoseconds, milliseconds, or seconds
    from the Epoch. Expected date ranges are used to infer the units.

    Converting a datetime.datetime or pandas.Timestamp to a ZonedDateTime will use the datetime's timezone information.
    Converting a numpy.datetime64 to a ZonedDateTime will use the Deephaven default time zone.

    Args:
        dt (Union[None, ZonedDateTime, str, datetime.datetime, numpy.datetime64, pandas.Timestamp]):
            A date time value.  If None is provided, None is returned.

    Returns:
        ZonedDateTime

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, ZonedDateTime.j_type):
            return dt
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseZonedDateTime(dt)
        elif isinstance(dt, datetime.datetime) or isinstance(dt, pandas.Timestamp):
            instant = to_j_instant(dt)
            tz = to_j_time_zone(dt.tzinfo)

            if tz is None:
                tz = dh_time_zone()

            return _JZonedDateTime.ofInstant(instant, tz)
        elif isinstance(dt, numpy.datetime64):
            instant = to_j_instant(dt)
            tz = _JDateTimeUtils.timeZone()
            return _JZonedDateTime.ofInstant(instant, tz)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> ZonedDateTime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_duration(dt: Union[None, Duration, int, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]) -> \
        Optional[Duration]:
    """
    Converts a time duration value to a Java Duration,
    which is a unit of time in terms of clock time (24-hour days, hours, minutes, seconds, and nanoseconds).
    Time duration values can be None, a Java Duration, an int, a string, a datetime.timedelta, a numpy.timedelta64,
    or a pandas.Timedelta.

    int values are nanoseconds.

    Duration strings can be formatted according to the ISO-8601 duration format as '[-]PnDTnHnMn.nS', where the
    coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    begin with a negative sign.

    Examples:
        |    "PT20.345S" -- parses as "20.345 seconds"
        |    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
        |    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
        |    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
        |    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
        |    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
        |    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
        |    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"

    Args:
        dt (Union[None, Duration, int, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]):
            A time duration value.  If None is provided, None is returned.

    Returns:
        Duration

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, Duration.j_type):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _JDuration.ofNanos(dt)
        elif isinstance(dt, str):
            return _JDateTimeUtils.parseDuration(dt)
        elif isinstance(dt, pandas.Timedelta):
            nanos = int((dt / datetime.timedelta(microseconds=1)) * _NANOS_PER_MICRO) + dt.nanoseconds
            return _JDuration.ofNanos(nanos)
        elif isinstance(dt, datetime.timedelta):
            nanos = int((dt / datetime.timedelta(microseconds=1)) * _NANOS_PER_MICRO)
            return _JDuration.ofNanos(nanos)
        elif isinstance(dt, numpy.timedelta64):
            nanos = int(dt.astype('timedelta64[ns]').astype(numpy.int64))
            return _JDuration.ofNanos(nanos)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Duration")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_period(dt: Union[None, Period, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]) -> \
        Optional[Period]:
    """
    Converts a time duration value to a Java Period,
    which is a unit of time in terms of calendar time (days, weeks, months, years, etc.).
    Time duration values can be None, a Java Period, a string, a datetime.timedelta, a numpy.timedelta64,
    or a pandas.Timedelta.

    Period strings can be formatted according to the ISO-8601 duration format as 'PnYnMnD' and 'PnW', where the
    coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    begin with a negative sign.

    Examples:
        |    "P2Y"             -- 2 Years
        |    "P3M"             -- 3 Months
        |    "P4W"             -- 4 Weeks
        |    "P5D"             -- 5 Days
        |    "P1Y2M3D"         -- 1 Year, 2 Months, 3 Days
        |    "P-1Y2M"          -- -1 Year, 2 Months
        |    "-P1Y2M"          -- -1 Year, -2 Months

    Args:
        dt (Union[None, Period, str, datetime.timedelta, numpy.timedelta64, pandas.Timedelta]):
            A Python period or period string.  If None is provided, None is returned.

    Returns:
        Period

    Raises:
        DHError, TypeError, ValueError
    """
    try:
        if dt is None or pandas.isnull(dt):
            return None
        elif isinstance(dt, Period.j_type):
            return dt
        elif isinstance(dt, str):
            return _JDateTimeUtils.parsePeriod(dt)
        elif isinstance(dt, pandas.Timedelta):
            if dt.seconds or dt.microseconds or dt.nanoseconds:
                raise ValueError("Unsupported conversion: " + str(type(dt)) +
                                " -> Period: Periods must only be days or weeks")
            elif dt.days:
                return _JPeriod.ofDays(dt.days)
            else:
                raise ValueError("Unsupported conversion: " + str(type(dt)) + " -> Period")
        elif isinstance(dt, datetime.timedelta):
            if dt.seconds or dt.microseconds:
                raise ValueError("Unsupported conversion: " + str(type(dt)) +
                                " -> Period: Periods must only be days or weeks")
            elif dt.days:
                return _JPeriod.ofDays(dt.days)
            else:
                raise ValueError("Unsupported conversion: " + str(type(dt)) + " -> Period")
        elif isinstance(dt, numpy.timedelta64):
            data = numpy.datetime_data(dt)
            units = data[0]
            value = int(dt.astype(numpy.int64))

            if units == 'D':
                return _JPeriod.ofDays(value)
            elif units == 'W':
                return _JPeriod.ofDays(value * 7)
            elif units == 'M':
                return _JPeriod.ofMonths(value)
            elif units == 'Y':
                return _JPeriod.ofYears(value)
            else:
                raise ValueError("Unsupported conversion: " + str(
                    type(dt)) + " -> Period: numpy.datetime64 must have units of 'D', 'W', 'M', or 'Y'")
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Period")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


# endregion


# region Conversions: Java To Python


def to_date(dt: Union[None, LocalDate, ZonedDateTime]) -> Optional[datetime.date]:
    """
    Converts a Java date time to a datetime.date.

    Args:
        dt (Union[None, LocalDate, ZonedDateTime]): A Java date time.
            If None is provided, None is returned.

    Returns:
        datetime.date

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None:
            return None
        if isinstance(dt, LocalDate.j_type):
            year, month_value, day_of_month = _JPythonTimeComponents.getLocalDateComponents(dt)
            return datetime.date(year, month_value, day_of_month)
        if isinstance(dt, ZonedDateTime.j_type):
            year, month_value, day_of_month = _JPythonTimeComponents.getLocalDateComponents(dt)
            return datetime.date(year, month_value, day_of_month)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.date")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_time(dt: Union[None, LocalTime, ZonedDateTime]) -> Optional[datetime.time]:
    """
    Converts a Java date time to a datetime.time.

    Args:
        dt (Union[None, LocalTime, ZonedDateTime]): A Java date time.
            If None is provided, None is returned.

    Returns:
        datetime.time

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, LocalTime.j_type):
            hour, minute, second, nano = _JPythonTimeComponents.getLocalTimeComponents(dt)
            return datetime.time(hour, minute, second, nano // _NANOS_PER_MICRO)
        elif isinstance(dt, ZonedDateTime.j_type):
            hour, minute, second, nano = _JPythonTimeComponents.getLocalTimeComponents(dt)
            return datetime.time(hour, minute, second, nano // _NANOS_PER_MICRO)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.time")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_datetime(dt: Union[None, Instant, ZonedDateTime]) -> Optional[datetime.datetime]:
    """
    Converts a Java date time to a datetime.datetime.

    Args:
        dt (Union[None, Instant, ZonedDateTime]): A Java date time.
            If None is provided, None is returned.

    Returns:
        datetime.datetime

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Instant.j_type):
            epoch_second, nano = _JPythonTimeComponents.getInstantComponents(dt)
            ts = epoch_second + (nano / _NANOS_PER_SECOND)
            return datetime.datetime.fromtimestamp(ts)
        elif isinstance(dt, ZonedDateTime.j_type):
            epoch_second, nano = _JPythonTimeComponents.getInstantComponents(dt)
            ts = epoch_second + (nano / _NANOS_PER_SECOND)
            return datetime.datetime.fromtimestamp(ts)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_pd_timestamp(dt: Union[None, Instant, ZonedDateTime]) -> Optional[pandas.Timestamp]:
    """
    Converts a Java date time to a pandas.Timestamp.

    Args:
        dt (Union[None, Instant, ZonedDateTime]): A Java date time.
            If None is provided, None is returned.

    Returns:
        pandas.Timestamp

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Instant.j_type) or isinstance(dt, ZonedDateTime.j_type):
            ts = _JDateTimeUtils.epochNanos(dt)
            return pandas.Timestamp(ts_input=ts, unit='ns')
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> pandas.Timestamp")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_np_datetime64(dt: Union[None, Instant, ZonedDateTime]) -> Optional[numpy.datetime64]:
    """
    Converts a Java date time to a numpy.datetime64.

    Args:
        dt (Union[None, Instant, ZonedDateTime]): A Java date time.
            If None is provided, None is returned.

    Returns:
        numpy.datetime64

    Raises:
        DHError, TypeError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Instant.j_type):
            epoch_second, nano = _JPythonTimeComponents.getInstantComponents(dt)
            ts = epoch_second * _NANOS_PER_SECOND + nano
            return numpy.datetime64(ts, 'ns')
        elif isinstance(dt, ZonedDateTime.j_type):
            epoch_second, nano = _JPythonTimeComponents.getInstantComponents(dt)
            ts = epoch_second * _NANOS_PER_SECOND + nano
            return numpy.datetime64(ts, 'ns')
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.datetime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_timedelta(dt: Union[None, Duration]) -> Optional[datetime.timedelta]:
    """
    Converts a Java time duration to a datetime.timedelta.

    Args:
        dt (Union[None, Duration]): A Java time duration.  If None is provided, None is returned.

    Returns:
        datetime.timedelta

    Raises:
        DHError, TypeError, ValueError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Duration.j_type):
            seconds, nano = _JPythonTimeComponents.getDurationComponents(dt)
            return datetime.timedelta(seconds=seconds, microseconds=nano // _NANOS_PER_MICRO)
        elif isinstance(dt, Period.j_type):
            y, m, d = _JPythonTimeComponents.getPeriodComponents(dt)

            if y or m:
                raise ValueError("Unsupported conversion: " + str(type(dt)) +
                                " -> datetime.timedelta: Periods must only be days or weeks")

            return datetime.timedelta(days=d)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_pd_timedelta(dt: Union[None, Duration]) -> Optional[pandas.Timedelta]:
    """
    Converts a Java time duration to a pandas.Timedelta.

    Args:
        dt (Union[None, Duration]): A Java time duration.  If None is provided, None is returned.

    Returns:
        pandas.Timedelta

    Raises:
        DHError, TypeError, ValueError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Duration.j_type):
            seconds, nano = _JPythonTimeComponents.getDurationComponents(dt)
            micros, nanos = divmod(nano, _NANOS_PER_MICRO)
            return pandas.Timedelta(seconds=seconds, microseconds=micros, nanoseconds=nanos)
        elif isinstance(dt, Period.j_type):
            y, m, d = _JPythonTimeComponents.getPeriodComponents(dt)

            if y or m:
                raise ValueError("Unsupported conversion: " + str(type(dt)) +
                                " -> datetime.timedelta: Periods must only be days or weeks")

            return pandas.Timedelta(days=d)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> pandas.Timedelta")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_np_timedelta64(dt: Union[None, Duration, Period]) -> Optional[numpy.timedelta64]:
    """
    Converts a Java time durationto a numpy.timedelta64.

    Args:
        dt (Union[None, Duration, Period]): A Java time period.  If None is provided, None is returned.

    Returns:
        numpy.timedelta64

    Raises:
        DHError, TypeError, ValueError
    """
    try:
        if dt is None:
            return None
        elif isinstance(dt, Duration.j_type):
            return numpy.timedelta64(dt.toNanos(), 'ns')
        elif isinstance(dt, Period.j_type):
            y, m, d = _JPythonTimeComponents.getPeriodComponents(dt)

            count = (1 if d else 0) + (1 if m else 0) + (1 if y else 0)

            if count == 0:
                return numpy.timedelta64(0, 'D')
            elif count > 1:
                raise ValueError("Unsupported conversion: " + str(type(dt)) +
                                " -> datetime.timedelta64: Periods must be days, months, or years")
            elif y:
                return numpy.timedelta64(y, 'Y')
            elif m:
                return numpy.timedelta64(m, 'M')
            elif d:
                return numpy.timedelta64(d, 'D')
            else:
                raise ValueError("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64: (" + dt + ")")
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> datetime.timedelta64")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e

# endregion
