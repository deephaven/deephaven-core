#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for handling Deephaven date/time data. """

from __future__ import annotations
from typing import Union, Optional

import jpy

from deephaven import DHError
from deephaven.dtypes import Instant, LocalDate, LocalTime, ZonedDateTime, Duration, Period, TimeZone, from_jtype
from deephaven.constants import NULL_INT, NULL_LONG, NULL_DOUBLE

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

MICRO = 1000  #: One microsecond in nanoseconds.
MILLI = 1000000  #: One millisecond in nanosecondsl
SECOND = 1000000000  #: One second in nanoseconds.
MINUTE = 60 * SECOND  #: One minute in nanoseconds.
HOUR = 60 * MINUTE  #: One hour in nanoseconds.
DAY = 24 * HOUR  #: One day in nanoseconds.  This is one hour of wall time and does not take into account calendar adjustments.
WEEK = 7 * DAY  #: One week in nanoseconds.  This is 7 days of wall time and does not take into account calendar adjustments.
YEAR_365 = 365 * DAY  #: One 365 day year in nanoseconds.  This is 365 days of wall time and does not take into account calendar adjustments.
YEAR_AVG = 31556952000000000  #: One average year in nanoseconds.  This is 365.2425 days of wall time and does not take into account calendar adjustments.

SECONDS_PER_NANO = 1 / SECOND  #: Number of seconds per nanosecond.
MINUTES_PER_NANO = 1 / MINUTE  #: Number of minutes per nanosecond.
HOURS_PER_NANO = 1 / HOUR  #: Number of hours per nanosecond.
DAYS_PER_NANO = 1 / DAY  #: Number of days per nanosecond.
YEARS_PER_NANO_365 = 1 / YEAR_365  #: Number of 365 day years per nanosecond.
YEARS_PER_NANO_AVG = 1 / YEAR_AVG  #: Number of average (365.2425 day) years per nanosecond.


# region Clock


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


def time_zone(tz: Optional[str]) -> TimeZone:
    """ Gets the time zone for a time zone name.

    Args:
        tz (Optional[str]): Time zone name.  If None is provided, the system default time zone is returned.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    try:
        if tz is None:
            return _JDateTimeUtils.timeZone()
        else:
            return _JDateTimeUtils.timeZone(tz)
    except Exception as e:
        raise DHError(e) from e


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

# region Conversions: Time Units


def micros_to_nanos(micros: int) -> int:
    """ Converts microseconds to nanoseconds.

    Args:
        micros (int): Microseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input microseconds converted to nanoseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.microsToNanos(micros)
    except Exception as e:
        raise DHError(e) from e


def millis_to_nanos(millis: int) -> int:
    """ Converts milliseconds to nanoseconds.

    Args:
        millis (int): Milliseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input milliseconds converted to nanoseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisToNanos(millis)
    except Exception as e:
        raise DHError(e) from e


def seconds_to_nanos(seconds: int) -> int:
    """ Converts seconds to nanoseconds.

    Args:
        seconds (int): Seconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input seconds converted to nanoseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.secondsToNanos(seconds)
    except Exception as e:
        raise DHError(e) from e


def nanos_to_micros(nanos: int) -> int:
    """ Converts nanoseconds to microseconds.

    Args:
        nanos (int): nanoseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input nanoseconds converted to microseconds, rounded down.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosToMicros(nanos)
    except Exception as e:
        raise DHError(e) from e


def millis_to_micros(millis: int) -> int:
    """ Converts milliseconds to microseconds.

    Args:
        millis (int): milliseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input milliseconds converted to microseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisToMicros(millis)
    except Exception as e:
        raise DHError(e) from e


def seconds_to_micros(seconds: int) -> int:
    """ Converts seconds to microseconds.

    Args:
        seconds (int): Seconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input seconds converted to microseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.secondsToMicros(seconds)
    except Exception as e:
        raise DHError(e) from e


def nanos_to_millis(nanos: int) -> int:
    """ Converts nanoseconds to milliseconds.

    Args:
        nanos (int): Nanoseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input nanoseconds converted to milliseconds, rounded down.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosToMillis(nanos)
    except Exception as e:
        raise DHError(e) from e


def micros_to_millis(micros: int) -> int:
    """ Converts microseconds to milliseconds.

    Args:
        micros (int): Microseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input microseconds converted to milliseconds, rounded down.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.microsToMillis(micros)
    except Exception as e:
        raise DHError(e) from e


def seconds_to_millis(seconds: int) -> int:
    """ Converts seconds to milliseconds.

    Args:
        seconds (int): Seconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input seconds converted to milliseconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.secondsToMillis(seconds)
    except Exception as e:
        raise DHError(e) from e


def nanos_to_seconds(nanos: int) -> int:
    """ Converts nanoseconds to seconds.

    Args:
        nanos (int): Nanoseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input nanoseconds converted to seconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosToSeconds(nanos)
    except Exception as e:
        raise DHError(e) from e


def micros_to_seconds(micros: int) -> int:
    """ Converts microseconds to seconds.

    Args:
        micros (int): Microseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input microseconds converted to seconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.microsToSeconds(micros)
    except Exception as e:
        raise DHError(e) from e


def millis_to_seconds(millis: int) -> int:
    """ Converts milliseconds to seconds.

    Args:
        millis (int): Milliseconds to convert.

    Returns:
        NULL_LONG if the input is NULL_LONG; otherwise the input milliseconds converted to seconds.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisToSeconds(millis)
    except Exception as e:
        raise DHError(e) from e


# endregion

# region Conversions: Date Time Types

def to_instant(dt: ZonedDateTime) -> Instant:
    """ Converts a date time to an Instant.

    Args:
        dt (ZonedDateTime): Date time to convert.

    Returns:
        Instant or None if dt is None.

    Raises:
        DHError
    """
    if not dt:
        return None

    try:
        return _JDateTimeUtils.toInstant(dt)
    except Exception as e:
        raise DHError(e) from e


def to_zdt(dt: Instant, tz: TimeZone) -> ZonedDateTime:
    """ Converts a date time to a ZonedDateTime.

    Args:
        dt (Instant): Date time to convert.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if any input is None.

    Raises:
        DHError
    """
    if not dt or not tz:
        return None

    try:
        return _JDateTimeUtils.toZonedDateTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def make_instant(date: LocalDate, time: LocalTime, tz: TimeZone) -> Instant:
    """ Makes an Instant.

    Args:
        date (LocalDate): Local date.
        time (LocalTime): Local time.
        tz (TimeZone): Time zone.

    Returns:
        Instant or None if any input is None.

    Raises:
        DHError
    """
    if not date or not time or not tz:
        return None

    try:
        return _JDateTimeUtils.toInstant(date, time, tz)
    except Exception as e:
        raise DHError(e) from e


def make_zdt(date: LocalDate, time: LocalTime, tz: TimeZone) -> ZonedDateTime:
    """ Makes a ZonedDateTime.

    Args:
        date (LocalDate): Local date.
        time (LocalTime): Local time.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if any input is None.

    Raises:
        DHError
    """
    if not date or not time or not tz:
        return None

    try:
        return _JDateTimeUtils.toZonedDateTime(date, time, tz)
    except Exception as e:
        raise DHError(e) from e


def to_local_date(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> LocalDate:
    """ Converts a date time to a LocalDate.

    Args:
        dt (Instant): Date time to convert.
        tz (TimeZone): Time zone.

    Returns:
        LocalDate or None if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return None

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.toLocalDate(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def to_local_time(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> LocalTime:
    """ Converts a date time to a LocalTime.

    Args:
        dt (Instant): Date time to convert.
        tz (TimeZone): Time zone.

    Returns:
        LocalTime or None if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return None

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.toLocalTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e


# endregion

# region Conversions: Epoch

def epoch_nanos(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns nanoseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        nanoseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    if not dt:
        return NULL_LONG

    try:
        return _JDateTimeUtils.epochNanos(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_micros(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns microseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        microseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    if not dt:
        return NULL_LONG

    try:
        return _JDateTimeUtils.epochMicros(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_millis(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns milliseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        milliseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    if not dt:
        return NULL_LONG

    try:
        return _JDateTimeUtils.epochMillis(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_seconds(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns seconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        seconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    if not dt:
        return NULL_LONG

    try:
        return _JDateTimeUtils.epochSeconds(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_nanos_to_instant(nanos: int) -> Instant:
    """ Converts nanoseconds from the Epoch to an Instant.

    Args:
        nanos (int): Nanoseconds since Epoch.

    Returns:
        Instant or None if the input is NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochNanosToInstant(nanos)
    except Exception as e:
        raise DHError(e) from e


def epoch_micros_to_instant(micros: int) -> Instant:
    """ Converts microseconds from the Epoch to an Instant.

    Args:
        micros (int): Microseconds since Epoch.

    Returns:
        Instant or None if the input is NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMicrosToInstant(micros)
    except Exception as e:
        raise DHError(e) from e


def epoch_millis_to_instant(millis: int) -> Instant:
    """ Converts milliseconds from the Epoch to an Instant.

    Args:
        millis (int): Milliseconds since Epoch.

    Returns:
        Instant or None if the input is NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMillisToInstant(millis)
    except Exception as e:
        raise DHError(e) from e


def epoch_seconds_to_instant(seconds: int) -> Instant:
    """ Converts seconds from the Epoch to an Instant.

    Args:
        seconds (int): Seconds since Epoch.

    Returns:
        Instant or None if the input is NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochSecondsToInstant(seconds)
    except Exception as e:
        raise DHError(e) from e


def epoch_nanos_to_zdt(nanos: int, tz: TimeZone) -> ZonedDateTime:
    """ Converts nanoseconds from the Epoch to a ZonedDateTime.

    Args:
        nanos (int): Nanoseconds since Epoch.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if the input is NULL_LONG or None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochNanosToZonedDateTime(nanos, tz)
    except Exception as e:
        raise DHError(e) from e


def epoch_micros_to_zdt(micros: int, tz: TimeZone) -> ZonedDateTime:
    """ Converts microseconds from the Epoch to a ZonedDateTime.

    Args:
        micros (int): Microseconds since Epoch.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if the input is NULL_LONG or None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMicrosToZonedDateTime(micros, tz)
    except Exception as e:
        raise DHError(e) from e


def epoch_millis_to_zdt(millis: int, tz: TimeZone) -> ZonedDateTime:
    """ Converts milliseconds from the Epoch to a ZonedDateTime.

    Args:
        millis (int): Milliseconds since Epoch.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if the input is NULL_LONG or None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMillisToZonedDateTime(millis, tz)
    except Exception as e:
        raise DHError(e) from e


def epoch_seconds_to_zdt(seconds: int, tz: TimeZone) -> ZonedDateTime:
    """ Converts seconds from the Epoch to a ZonedDateTime.

    Args:
        seconds (int): Seconds since Epoch.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if the input is NULL_LONG or None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochSecondsToZonedDateTime(seconds, tz)
    except Exception as e:
        raise DHError(e) from e


def epoch_auto_to_epoch_nanos(epoch_offset: int) -> int:
    """ Converts an offset from the Epoch to a nanoseconds from the Epoch.
    The offset can be in milliseconds, microseconds, or nanoseconds.
    Expected date ranges are used to infer the units for the offset.

    Args:
        epoch_offset (int): Time offset from the Epoch.

    Returns:
        the input offset from the Epoch converted to nanoseconds from the Epoch, or NULL_LONG if the input is NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochAutoToEpochNanos(epoch_offset)
    except Exception as e:
        raise DHError(e) from e


def epoch_auto_to_instant(epoch_offset: int) -> Instant:
    """ Converts an offset from the Epoch to an Instant.
    The offset can be in milliseconds, microseconds, or nanoseconds.
    Expected date ranges are used to infer the units for the offset.

    Args:
        epoch_offset (int): Time offset from the Epoch.

    Returns:
        Instant or None if the input is NULL_LONG

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochAutoToInstant(epoch_offset)
    except Exception as e:
        raise DHError(e) from e


def epoch_auto_to_zdt(epoch_offset: int, tz: TimeZone) -> ZonedDateTime:
    """ Converts an offset from the Epoch to a ZonedDateTime.
    The offset can be in milliseconds, microseconds, or nanoseconds.
    Expected date ranges are used to infer the units for the offset.

    Args:
        epoch_offset (int): Time offset from the Epoch.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if the input is NULL_LONG

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochAutoToZonedDateTime(epoch_offset, tz)
    except Exception as e:
        raise DHError(e) from e


# endregion

# region Conversions: Excel

def to_excel_time(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> float:
    """ Converts a date time to an Excel time represented as a double.

    Args:
        dt (Instant): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Excel time as a double or NULL_DOUBLE if any input is None

    Raises:
        DHError
    """
    if not dt or not tz:
        return NULL_DOUBLE

    try:
        if not dt or not tz:
            return NULL_DOUBLE

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.toExcelTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def excel_to_instant(excel: float, tz: TimeZone) -> Instant:
    """ Converts an Excel time represented as a double to an Instant.

    Args:
        excel (float): Excel time.
        tz (TimeZone): Time zone.

    Returns:
        Instant or None if any input is None or NULL_DOUBLE.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.excelToInstant(excel, tz)
    except Exception as e:
        raise DHError(e) from e


def excel_to_zdt(excel: float, tz: TimeZone) -> ZonedDateTime:
    """ Converts an Excel time represented as a double to a ZonedDateTime.

    Args:
        excel (float): Excel time.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if any input is None or NULL_DOUBLE.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.excelToZonedDateTime(excel, tz)
    except Exception as e:
        raise DHError(e) from e


# endregion

# region Arithmetic

def plus_period(dt: Union[Instant, ZonedDateTime], period: Union[int, Duration, Period]) -> \
        Union[Instant, ZonedDateTime]:
    """ Adds a time period to a date time.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        period (Union[int, Duration, Period]): Time period to add.  Integer inputs are nanoseconds.

    Returns:
        Date time, or None if any input is None or NULL_LONG.

    Raises:
        DHError
    """
    if not dt or not period:
        return None

    try:
        return _JDateTimeUtils.plus(dt, period)
    except Exception as e:
        raise DHError(e) from e


def minus_period(dt: Union[Instant, ZonedDateTime], period: Union[int, Duration, Period]) -> \
        Union[Instant, ZonedDateTime]:
    """ Subtracts a time period from a date time.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        period (Union[int, Duration, Period]): Time period to subtract.  Integer inputs are nanoseconds.

    Returns:
        Date time, or None if any input is None or NULL_LONG.

    Raises:
        DHError
    """
    if not dt or not period:
        return None

    try:
        return _JDateTimeUtils.minus(dt, period)
    except Exception as e:
        raise DHError(e) from e


def diff_nanos(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> int:
    """ Returns the difference in nanoseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in nanoseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_LONG

    try:
        return _JDateTimeUtils.diffNanos(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_micros(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> int:
    """ Returns the difference in microseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in microseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_LONG

    try:
        return _JDateTimeUtils.diffMicros(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_millis(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> int:
    """ Returns the difference in milliseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in milliseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_LONG

    try:
        return _JDateTimeUtils.diffMillis(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_seconds(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> float:
    """ Returns the difference in seconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in seconds or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_DOUBLE

    try:
        return _JDateTimeUtils.diffSeconds(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_minutes(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> float:
    """ Returns the difference in minutes between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in minutes or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_DOUBLE

    try:
        return _JDateTimeUtils.diffMinutes(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_days(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> float:
    """ Returns the difference in days between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in days or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_DOUBLE

    try:
        return _JDateTimeUtils.diffDays(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_years_365(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> float:
    """ Returns the difference in years between two date time values.  Both values must be of the same type.

    Years are defined in terms of 365 day years.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in years or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_DOUBLE

    try:
        return _JDateTimeUtils.diffYears365(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_years_avg(start: Union[Instant, ZonedDateTime], end: Union[Instant, ZonedDateTime]) -> float:
    """ Returns the difference in years between two date time values.  Both values must be of the same type.

    Years are defined in terms of 365.2425 day years.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in years or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    if not start or not end:
        return NULL_DOUBLE

    try:
        return _JDateTimeUtils.diffYearsAvg(start, end)
    except Exception as e:
        raise DHError(e) from e

# endregion

# region Comparisons

def is_before(dt1: Union[Instant, ZonedDateTime], dt2: Union[Instant, ZonedDateTime]) -> bool:
    """ Evaluates whether one date time value is before a second date time value.
    Both values must be of the same type.

    Args:
        dt1 (Union[Instant,ZonedDateTime]): First date time.
        dt2 (Union[Instant,ZonedDateTime]): Second date time.

    Returns:
        True if dt1 is before dt2; otherwise, False if either value is null or if dt2 is equal to or before dt1.

    Raises:
        DHError
    """
    if not dt1 or not dt2:
        return False

    try:
        return _JDateTimeUtils.isBefore(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_before_or_equal(dt1: Union[Instant, ZonedDateTime], dt2: Union[Instant, ZonedDateTime]) -> bool:
    """ Evaluates whether one date time value is before or equal to a second date time value.
    Both values must be of the same type.

    Args:
        dt1 (Union[Instant,ZonedDateTime]): First date time.
        dt2 (Union[Instant,ZonedDateTime]): Second date time.

    Returns:
        True if dt1 is before or equal to dt2; otherwise, False if either value is null or if dt2 is before dt1.

    Raises:
        DHError
    """
    if not dt1 or not dt2:
        return False

    try:
        return _JDateTimeUtils.isBeforeOrEqual(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_after(dt1: Union[Instant, ZonedDateTime], dt2: Union[Instant, ZonedDateTime]) -> bool:
    """ Evaluates whether one date time value is after a second date time value.
    Both values must be of the same type.

    Args:
        dt1 (Union[Instant,ZonedDateTime]): First date time.
        dt2 (Union[Instant,ZonedDateTime]): Second date time.

    Returns:
        True if dt1 is after dt2; otherwise, False if either value is null or if dt2 is equal to or after dt1.

    Raises:
        DHError
    """
    if not dt1 or not dt2:
        return False

    try:
        return _JDateTimeUtils.isAfter(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_after_or_equal(dt1: Union[Instant, ZonedDateTime], dt2: Union[Instant, ZonedDateTime]) -> bool:
    """ Evaluates whether one date time value is after or equal to a second date time value.
    Both values must be of the same type.

    Args:
        dt1 (Union[Instant,ZonedDateTime]): First date time.
        dt2 (Union[Instant,ZonedDateTime]): Second date time.

    Returns:
        True if dt1 is after or equal to dt2; otherwise, False if either value is null or if dt2 is after dt1.

    Raises:
        DHError
    """
    if not dt1 or not dt2:
        return False

    try:
        return _JDateTimeUtils.isAfterOrEqual(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


# endregion

# region Chronology

def nanos_of_milli(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns the number of nanoseconds that have elapsed since the top of the millisecond.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        Number of nanoseconds that have elapsed since the top of the millisecond, or NULL_INT if the input is None.

    Raises:
        DHError
    """
    if not dt:
        return NULL_INT

    try:
        return _JDateTimeUtils.nanosOfMilli(dt)
    except Exception as e:
        raise DHError(e) from e


def micros_of_milli(dt: Union[Instant, ZonedDateTime]) -> int:
    """ Returns the number of microseconds that have elapsed since the top of the millisecond.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        Number of microseconds that have elapsed since the top of the millisecond, or NULL_INT if the input is None.

    Raises:
        DHError
    """
    if not dt:
        return NULL_INT

    try:
        return _JDateTimeUtils.microsOfMilli(dt)
    except Exception as e:
        raise DHError(e) from e


def nanos_of_second(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of nanoseconds that have elapsed since the top of the second.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of nanoseconds that have elapsed since the top of the second, or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_LONG

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.nanosOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def micros_of_second(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of microseconds that have elapsed since the top of the second.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of microseconds that have elapsed since the top of the second, or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_LONG

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.microsOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def millis_of_second(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of milliseconds that have elapsed since the top of the second.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of milliseconds that have elapsed since the top of the second, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.millisOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def second_of_minute(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of seconds that have elapsed since the top of the minute.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of seconds that have elapsed since the top of the minute, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.secondOfMinute(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def minute_of_hour(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of minutes that have elapsed since the top of the hour.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of minutes that have elapsed since the top of the hour, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.minuteOfHour(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def nanos_of_day(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of nanoseconds that have elapsed since the top of the day.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of nanoseconds that have elapsed since the top of the day, or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_LONG

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.nanosOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def millis_of_day(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of milliseconds that have elapsed since the top of the day.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of milliseconds that have elapsed since the top of the day, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.millisOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def second_of_day(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of seconds that have elapsed since the top of the day.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of seconds that have elapsed since the top of the day, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.secondOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def minute_of_day(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of minutes that have elapsed since the top of the day.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of minutes that have elapsed since the top of the day, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.minuteOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def hour_of_day(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the number of hours that have elapsed since the top of the day.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Number of hours that have elapsed since the top of the day, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.hourOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def day_of_week(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the week for a date time in the specified time zone, with 1 being
    Monday and 7 being Sunday.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Day of the week, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.dayOfWeek(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def day_of_month(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the month for a date time and specified time zone.
    The first day of the month returns 1, the second day returns 2, etc.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Day of the month, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.dayOfMonth(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def day_of_year(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the year (Julian date) for a date time in the specified time zone.
    The first day of the year returns 1, the second day returns 2, etc.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Day of the year, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.dayOfYear(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def month_of_year(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns a 1-based int value of the month of the year (Julian date) for a date time in the specified time zone.
    January is 1, February is 2, etc.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Month of the year, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.monthOfYear(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def year(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the year for a date time in the specified time zone.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Year, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.year(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def year_of_century(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> int:
    """ Returns the year of the century (two-digit year) for a date time in the specified time zone.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Year of the century, or NULL_INT if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return NULL_INT

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.yearOfCentury(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def at_midnight(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> Union[Instant, ZonedDateTime]:
    """ Returns a date time for the prior midnight in the specified time zone.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        date time for the prior midnight in the specified time zone, or None if any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return None

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.atMidnight(dt, tz)
    except Exception as e:
        raise DHError(e) from e


# endregion


# region Format

def format_duration_nanos(nanos: int) -> str:
    """ Returns a nanosecond duration formatted as a "[-]PThhh:mm:ss.nnnnnnnnn" string.

    Args:
        nanos (int): Nanosecond.

    Returns:
        Formatted string, or None if the input is NULL_INT.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.formatDurationNanos(nanos)
    except Exception as e:
        raise DHError(e) from e


def format_datetime(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> str:
    """ Returns a date time formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Formatted string, or None if the any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return None

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.formatDateTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e


def format_date(dt: Union[Instant, ZonedDateTime], tz: TimeZone) -> str:
    """ Returns a date time formatted as a "yyyy-MM-dd" string.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Formatted string, or None if the any input is None.

    Raises:
        DHError
    """
    try:
        if not dt or not tz:
            return None

        if from_jtype(dt.getClass()) == ZonedDateTime:
            dt = to_instant(dt)

        return _JDateTimeUtils.formatDate(dt, tz)
    except Exception as e:
        raise DHError(e) from e


# endregion

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


def parse_time_precision(s: str, quiet: bool = False) -> Optional[str]:
    """ Returns a string indicating the level of precision in a time, datetime, or period nanos string. (e.g. 'SecondOfMinute').

    Args:
        s (str): Time string.
        quiet (bool): False will cause exceptions when strings can not be parsed.  False will cause None to be returned.

    Returns:
        String indicating the level of precision in a time or datetime string (e.g. 'SecondOfMinute').

    Raises:
        DHError
    """
    try:
        if quiet:
            p = _JDateTimeUtils.parseTimePrecisionQuiet(s)

            if p:
                return p.toString()
            else:
                return None
        else:
            return _JDateTimeUtils.parseTimePrecision(s).toString()
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
