#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for handling Deephaven date/time data. """

from __future__ import annotations
from typing import Union

import jpy

from deephaven import DHError
from deephaven.dtypes import Instant, LocalDate, LocalTime, ZonedDateTime, Duration, Period, TimeZone #TODO, DateTime, Period

MICRO = 1000 #: One microsecond in nanoseconds.
MILLI = 1000000 #: One millisecond in nanosecondsl
SECOND = 1000000000  #: One second in nanoseconds.
MINUTE = 60 * SECOND  #: One minute in nanoseconds.
HOUR = 60 * MINUTE  #: One hour in nanoseconds.
DAY = 24 * HOUR  #: One day in nanoseconds.
WEEK = 7 * DAY  #: One week in nanoseconds.
YEAR = 52 * WEEK  #: One year in nanoseconds.

SECONDS_PER_NANO = 1 / SECOND #: Number of seconds per nanosecond.
MINUTES_PER_NANO = 1/MINUTE #: Number of minutes per nanosecond.
HOURS_PER_NANO = 1/HOUR #: Number of hours per nanosecond.
DAYS_PER_NANO = 1/DAY #: Number of days per nanosecond.
YEARS_PER_NANO = 1/YEAR #: Number of years per nanosecond.

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

# region Clock

def now(system: bool=False, resolution: str='ns') -> Instant:
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
        return _JDateTimeUtils.today(tz) #TODO: tz wrapped?
    except Exception as e:
        raise DHError(e) from e

# endregion

# region Time Zone

def tz(time_zone: str) -> TimeZone:
    """ Gets the time zone for a time zone name.

    Args:
        time_zone (str): Time zone name.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.tz(time_zone) #TODO: wrap the result?
    except Exception as e:
        raise DHError(e) from e


def tz_default() -> TimeZone:
    """ Gets the default time zone.

    Returns:
        TimeZone

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.tz() #TODO: wrap the result?
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
    try:
        return _JDateTimeUtils.toInstant(dt)
    except Exception as e:
        raise DHError(e) from e


    #TODO: ?
    # /**
    #  * Converts a date, time, and time zone to an {@link Instant}.
    #  *
    #  * @param date date.
    #  * @param time local time.
    #  * @param timeZone time zone.
    #  * @return {@link Instant}, or null if any input is null.
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant toInstant(@Nullable final LocalDate date, @Nullable final LocalTime time, @Nullable ZoneId timeZone) {


def to_zoned_date_time(dt: Instant, tz: TimeZone) -> ZonedDateTime:
    """ Converts a date time to a ZonedDateTime.

    Args:
        dt (Instant): Date time to convert.
        tz (TimeZone): Time zone.

    Returns:
        ZonedDateTime or None if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.toZonedDateTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e

    #TODO:
    # /**
    #  * Converts a local date, local time, and time zone to a {@link ZonedDateTime}.
    #  *
    #  * @param date date.
    #  * @param time local time.
    #  * @param timeZone time zone.
    #  * @return {@link ZonedDateTime}, or null if any input is null.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime toZonedDateTime(@Nullable final LocalDate date, @Nullable final LocalTime time, @Nullable ZoneId timeZone) {

def to_local_date(dt: Instant, tz: TimeZone) -> LocalDate:
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
        return _JDateTimeUtils.toLocalDate(dt, tz)
    except Exception as e:
        raise DHError(e) from e

    #TODO?
    # /**
    #  * Converts a date time to a {@link LocalDate} with the time zone in the {@link ZonedDateTime}.
    #  *
    #  * @param dateTime date time to convert.
    #  * @return {@link LocalDate}, or null if any input is null.
    #  */
    # @Nullable
    # public static LocalDate toLocalDate(@Nullable final ZonedDateTime dateTime) {


def to_local_time(dt: Instant, tz: TimeZone) -> LocalTime:
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
        return _JDateTimeUtils.toLocalTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e

    #TODO: ?
    # /**
    #  * Converts a date time to a {@link LocalTime} with the time zone in the {@link ZonedDateTime}.
    #  *
    #  * @param dateTime date time to convert.
    #  * @return {@link LocalTime}, or null if any input is null.
    #  */
    # @Nullable
    # public static LocalTime toLocalTime(@Nullable final ZonedDateTime dateTime) {


# endregion

# region Conversions: Epoch

def epoch_nanos(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns nanoseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        nanoseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochNanos(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_micros(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns microseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        microseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMicros(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_millis(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns milliseconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        milliseconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.epochMillis(dt)
    except Exception as e:
        raise DHError(e) from e


def epoch_seconds(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns seconds from the Epoch for a date time value.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        seconds since Epoch, or a NULL_LONG value if the date time is null.

    Raises:
        DHError
    """
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


#TODO: shorten zoned_date_time to zdt?
def epoch_nanos_to_zoned_date_time(nanos: int, tz: TimeZone) -> ZonedDateTime:
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


def epoch_micros_to_zoned_date_time(micros: int, tz: TimeZone) -> ZonedDateTime:
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


def epoch_millis_to_zoned_date_time(millis: int, tz: TimeZone) -> ZonedDateTime:
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


def epoch_seconds_to_zoned_date_time(seconds: int, tz: TimeZone) -> ZonedDateTime:
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


def epoch_auto_to_zoned_date_time(epoch_offset: int, tz: TimeZone) -> ZonedDateTime:
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

def to_excel_time(dt: Instant, tz: TimeZone) -> float:
    """ Converts a date time to an Excel time represented as a double.

    Args:
        dt (Instant): Date time.
        tz (TimeZone): Time zone.

    Returns:
        Excel time as a double or NULL_DOUBLE if any input is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.toExcelTime(dt, tz)
    except Exception as e:
        raise DHError(e) from e

    #TODO: ?
    # /**
    #  * Converts a date time to an Excel time represented as a double.
    #  *
    #  * @param dateTime date time to convert.
    #  * @return 0.0 if either input is null; otherwise, the input date time converted to an Excel time represented as a double.
    #  */
    # @ScriptApi
    # public static double toExcelTime(@Nullable final ZonedDateTime dateTime) {


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


def excel_to_zoned_date_time(excel: float, tz: TimeZone) -> ZonedDateTime:
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

def plus_period(dt: Union[Instant,ZonedDateTime], period: Union[int, Duration, Period]) -> Union[Instant,ZonedDateTime]:
    """ Adds a time period to a date time.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        period (Union[int, Duration, Period]): Time period to add.  Integer inputs are nanoseconds.

    Returns:
        Date time, or None if any input is None or NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.plus(dt, period)
    except Exception as e:
        raise DHError(e) from e


def minus_period(dt: Union[Instant,ZonedDateTime], period: Union[int, Duration, Period]) -> Union[Instant,ZonedDateTime]:
    """ Subtracts a time period from a date time.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.
        period (Union[int, Duration, Period]): Time period to subtract.  Integer inputs are nanoseconds.

    Returns:
        Date time, or None if any input is None or NULL_LONG.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.minus(dt, period)
    except Exception as e:
        raise DHError(e) from e


    #TODO: ?
    # /**
    #  * Subtract one date time from another and return the difference in nanoseconds.
    #  *
    #  * @param dateTime1 first date time.
    #  * @param dateTime2 second date time.
    #  * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in dateTime1 and dateTime2 in nanoseconds.
    #  * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
    #  */
    # @ScriptApi
    # public static long minus(@Nullable final Instant dateTime1, @Nullable final Instant dateTime2) {
    #
    # /**
    #  * Subtract one date time from another and return the difference in nanoseconds.
    #  *
    #  * @param dateTime1 first date time.
    #  * @param dateTime2 second date time.
    #  * @return {@link QueryConstants#NULL_LONG} if either input is null; otherwise the difference in dateTime1 and dateTime2 in nanoseconds.
    #  * @throws DateTimeOverflowException if the datetime arithmetic overflows or underflows.
    #  */
    # @ScriptApi
    # public static long minus(@Nullable final ZonedDateTime dateTime1, @Nullable final ZonedDateTime dateTime2) {


def diff_nanos(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> int:
    """ Returns the difference in nanoseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in nanoseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffNanos(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_micros(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> int:
    """ Returns the difference in microseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in microseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffMicros(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_millis(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> int:
    """ Returns the difference in milliseconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in milliseconds or NULL_LONG if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffMillis(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_seconds(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> float:
    """ Returns the difference in seconds between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in seconds or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffSeconds(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_minutes(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> float:
    """ Returns the difference in minutes between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in minutes or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffMinutes(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_days(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> float:
    """ Returns the difference in days between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in days or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffDays(start, end)
    except Exception as e:
        raise DHError(e) from e


def diff_years(start: Union[Instant,ZonedDateTime], end: Union[Instant,ZonedDateTime]) -> float:
    """ Returns the difference in years between two date time values.  Both values must be of the same type.

    Args:
        start (Union[Instant,ZonedDateTime]): Start time.
        end (Union[Instant,ZonedDateTime]): End time.

    Returns:
        the difference in start and end in years or NULL_DOUBLE if any input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffYears(start, end)
    except Exception as e:
        raise DHError(e) from e

# endregion

# region Comparisons

def is_before(dt1: Union[Instant,ZonedDateTime], dt2: Union[Instant,ZonedDateTime]) -> bool:
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
    try:
        return _JDateTimeUtils.isBefore(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_before_or_equal(dt1: Union[Instant,ZonedDateTime], dt2: Union[Instant,ZonedDateTime]) -> bool:
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
    try:
        return _JDateTimeUtils.isBeforeOrEqual(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_after(dt1: Union[Instant,ZonedDateTime], dt2: Union[Instant,ZonedDateTime]) -> bool:
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
    try:
        return _JDateTimeUtils.isAfter(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_after_or_equal(dt1: Union[Instant,ZonedDateTime], dt2: Union[Instant,ZonedDateTime]) -> bool:
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
    try:
        return _JDateTimeUtils.isAfterOrEqual(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e

# endregion


# region Chronology

def nanos_of_milli(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns the number of nanoseconds that have elapsed since the top of the millisecond.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        Number of nanoseconds that have elapsed since the top of the millisecond, or NULL_INT if the input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosOfMilli(dt)
    except Exception as e:
        raise DHError(e) from e


def micros_of_milli(dt: Union[Instant,ZonedDateTime]) -> int:
    """ Returns the number of microseconds that have elapsed since the top of the millisecond.

    Args:
        dt (Union[Instant,ZonedDateTime]): Date time.

    Returns:
        Number of microseconds that have elapsed since the top of the millisecond, or NULL_INT if the input is None.

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.microsOfMilli(dt)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def nanos_of_second(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.nanosOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def micros_of_second(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.microsOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def millis_of_second(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.millisOfSecond(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def second_of_minute(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.secondOfMinute(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def minute_of_hour(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.minuteOfHour(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def nanos_of_day(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.nanosOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def millis_of_day(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.millisOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def second_of_day(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.secondOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def minute_of_day(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.minuteOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def hour_of_day(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.hourOfDay(dt, tz)
    except Exception as e:
        raise DHError(e) from e

#TODO: ZonedDateTime -- no tz
def day_of_week(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.dayOfWeek(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def day_of_month(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.dayOfMonth(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def day_of_year(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.dayOfYear(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def month_of_year(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.monthOfYear(dt, tz)
    except Exception as e:
        raise DHError(e) from e

#TODO: ZonedDateTime -- no tz
def year(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.monthOfYear(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def year_of_century(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.yearOfCentury(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def year_of_century(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> int:
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
        return _JDateTimeUtils.yearOfCentury(dt, tz)
    except Exception as e:
        raise DHError(e) from e


#TODO: ZonedDateTime -- no tz
def at_midnight(dt: Union[Instant,ZonedDateTime], tz: TimeZone) -> Union[Instant,ZonedDateTime]:
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
        return _JDateTimeUtils.atMidnight(dt, tz)
    except Exception as e:
        raise DHError(e) from e

# endregion

##############################################
##############################################
##############################################
##############################################
##############################################
##############################################




#TODO: string_to_datetime?
def to_datetime(s: str, quiet: bool = False) -> DateTime:
    """ Converts a datetime string to a DateTime object.

    Supports ISO 8601 format and others.

    Args:
        s (str): in the form of ISO 8601 or "yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ"
        quiet (bool): when True, if the datetime string can't be parsed, this function returns None, otherwise
            it raises an exception. The default is False

    Returns:
        a DateTime

    Raises:
        DHError
    """
    if quiet:
        return _JDateTimeUtils.toDateTimeQuiet(s)

    try:
        return _JDateTimeUtils.toDateTime(s)
    except Exception as e:
        raise DHError(e) from e


#TODO: string_to_period?
def to_period(s: str, quiet: bool = False) -> Period:
    """ Converts a period string into a Period object.

    Args:
        s (str): a string in the form of nYnMnWnDTnHnMnS, with n being numeric values, e.g. 1W for one week, T1M for
            one minute, 1WT1H for one week plus one hour
        quiet (bool): when True, if the period string can't be parsed, this function returns None, otherwise
            it raises an exception. The default is False

    Returns:
        a Period

    Raises:
        DHError
    """
    if quiet:
        return _JDateTimeUtils.toPeriodQuiet(s)

    try:
        return _JDateTimeUtils.toPeriod(s)
    except Exception as e:
        raise DHError(e) from e


#TODO: string_to_nanos?
def to_nanos(s, quiet: bool = False) -> int:
    """ Converts a time string to nanoseconds.

    Args:
        s (str): in the format of: hh:mm:ss[.SSSSSSSSS]
        quiet (bool): to return None or raise an exception if the string can't be parsed, default is False

    Returns:
        int

    Raises:
        DHError
    """
    if quiet:
        return _JDateTimeUtils.convertTimeQuiet(s)

    try:
        return _JDateTimeUtils.convertTime(s)
    except Exception as e:
        raise DHError(e) from e




def datetime_at_midnight(dt: DateTime, tz: TimeZone) -> DateTime:
    """ Returns a DateTime for the requested DateTime at midnight in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which the new value at midnight should be calculated
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        DateTime

    Raises:
        DHError
    """
    try:

        return _JDateTimeUtils.dateTimeAtMidnight(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def day_of_month(dt: DateTime, tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the month for a DateTime and specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the day of the month
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.dayOfMonth(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def day_of_week(dt: DateTime, tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the week for a DateTime in the specified time zone, with 1 being
     Monday and 7 being Sunday.

    Args:
        dt (DateTime): the DateTime for which to find the day of the week.
        tz (TimeZone): the TimeZone to use when interpreting the DateTime.

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.dayOfWeek(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def day_of_year(dt: DateTime, tz: TimeZone) -> int:
    """ Returns a 1-based int value of the day of the year (Julian date) for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the day of the year
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.dayOfYear(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def diff_nanos(dt1: DateTime, dt2: DateTime) -> int:
    """ Returns the difference in nanoseconds between two DateTime values.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTime): the 2nd DateTime

    Returns:
        int: NULL_LONG if either dt1 or dt2 is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffNanos(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def diff_days(dt1: DateTime, dt2: DateTime) -> float:
    """ Returns the difference in days between two DateTime values.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTime): the 2nd DateTime

    Returns:
        float: NULL_DOUBLE if either dt1 or dt2 is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffDays(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def diff_years(dt1: DateTime, dt2: DateTime) -> float:
    """ Returns the difference in years between two DateTime values.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTime): the 2nd DateTime

    Returns:
        float: NULL_DOUBLE if either dt1 or dt2 is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.diffYears(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def format_datetime(dt: DateTime, tz: TimeZone) -> str:
    """ Returns a string DateTime representation formatted as "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ".

    Args:
        dt (DateTime): the DateTime to format as a string
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        str

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.formatDateTime(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def format_nanos(ns: int) -> str:
    """ Returns a string representation of a number of nanoseconds formatted as "dddThh:mm:ss.nnnnnnnnn".
    For periods less than one day, "dddT" is not included.

    Args:
        ns (int): the number of nanoseconds

    Returns:
        str

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.formatNanos(ns)
    except Exception as e:
        raise DHError(e) from e


def format_date(dt: DateTime, tz: TimeZone) -> str:
    """ Returns a string date representation for a specified DateTime and time zone formatted as "yyy-MM-dd".

    Args:
        dt (DateTime): the DateTime to format
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        str

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.formatDate(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def hour_of_day(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the hour of the day for a DateTime in the specified time zone. The hour is on a 24 hour clock (0 - 23).

    Args:
        dt (DateTime): the DateTime for which to find the hour of the day
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.hourOfDay(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def is_after(dt1: DateTime, dt2: DateTime) -> bool:
    """ Evaluates whether one DateTime value is later than a second DateTime value.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTime): the 2nd DateTime

    Returns:
        bool

    Raises:
        DHError
    """
    try:
        dt1 = dt1 if dt1 else None
        dt2 = dt2 if dt2 else None
        return _JDateTimeUtils.isAfter(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def is_before(dt1: DateTime, dt2: DateTime) -> bool:
    """ Evaluates whether one DateTime value is before a second DateTime value.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTime): the 2nd DateTime

    Returns:
        bool

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.isBefore(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def lower_bin(dt: DateTime, interval: int, offset: int = 0) -> DateTime:
    """ Returns a DateTime value, which is at the starting (lower) end of a time range defined by the interval
     nanoseconds. For example, a 5*MINUTE intervalNanos value would return the DateTime value for the start of the
     five minute window that contains the input date time.

    Args:
        dt (DateTime): the DateTime for which to evaluate the start of the containing window
        interval (int): the size of the window in nanoseconds
        offset (int): the window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
              one minute. Default is 0

    Returns:
        DateTime

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.lowerBin(dt, interval, offset)
    except Exception as e:
        raise DHError(e) from e


def millis(dt: DateTime) -> int:
    """ Returns milliseconds since Epoch for a DateTime value.

    Args:
        dt (DateTime): the DateTime for which the milliseconds offset should be returned

    Returns:
        int: NULL_LONG if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millis(dt)
    except Exception as e:
        raise DHError(e) from e


def millis_of_day(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of milliseconds since midnight for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the milliseconds since midnight
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisOfDay(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def millis_of_second(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of milliseconds since the top of the second for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the milliseconds
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisOfSecond(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def millis_to_nanos(ms: int) -> int:
    """ Converts milliseconds to nanoseconds.

    Args:
        ms (int): the milliseconds value to convert

    Returns:
        int: NULL_LONG if ms is NULL_LONG

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisToNanos(ms)
    except Exception as e:
        raise DHError(e) from e


def millis_to_datetime(ms: int) -> DateTime:
    """ Converts a value of milliseconds from Epoch in the UTC time zone to a DateTime.

    Args:
        ms (int): the milliseconds value to convert

    returns:
        DateTime

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.millisToDateTime(ms)
    except Exception as e:
        raise DHError(e) from e


def minus(dt1: DateTime, dt2: DateTime) -> int:
    """ Subtracts one time from another, returns the difference in nanos.

    Args:
        dt1 (DateTime): the 1st DateTime
        dt2 (DateTiem): the 2nd DateTime

    Returns:
        int: NULL_LONG if either dt1 or dt2 is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.minus(dt1, dt2)
    except Exception as e:
        raise DHError(e) from e


def minus_nanos(dt: DateTime, ns: int) -> DateTime:
    """ Subtracts nanoseconds from a DateTime.

    Args:
        dt (DateTime): the starting DateTime value
        ns (int): the number of nanoseconds to subtract from dateTime

    Returns:
        DateTime

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.minus(dt, ns)
    except Exception as e:
        raise DHError(e) from e


def minus_period(dt: DateTime, period) -> DateTime:
    """ Subtracts a period from a DateTime.

    Args:
        dt (DateTime): the starting DateTime value
        period (Period): the Period to subtract from dateTime

    Returns:
        DateTime

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.minus(dt, period)
    except Exception as e:
        raise DHError(e) from e


def minute_of_day(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of minutes since midnight for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the minutes
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.minuteOfDay(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def minute_of_hour(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of minutes since the top of the hour for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the minutes
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:

        return _JDateTimeUtils.minuteOfHour(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def month_of_year(dt: DateTime, tz: TimeZone) -> int:
    """ Returns an 1-based int value for the month of a DateTime in the specified time zone. January is 1,
    and December is 12.

    Args:
        dt (DateTime): the DateTime for which to find the month
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.monthOfYear(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def nanos(dt: DateTime) -> int:
    """ Returns nanoseconds since Epoch for a DateTime value.

    Args:
        dt (DateTime): the DateTime for which the nanoseconds offset should be returned

    Returns:
        int: NULL_LONG if dt is None

    Raises:
        DHError
    """
    try:

        return _JDateTimeUtils.nanos(dt)
    except Exception as e:
        raise DHError(e) from e


def nanos_of_day(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of nanoseconds since midnight for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the nanoseconds since midnight
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_LONG if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosOfDay(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def nanos_of_second(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of nanoseconds since the top of the second for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the nanoseconds
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_LONG if dt is None

    Raises:
        DHError
    """
    try:

        return _JDateTimeUtils.nanosOfSecond(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def nanos_to_millis(ns: int) -> int:
    """ Converts nanoseconds to milliseconds.

    Args:
        ns (int): the value of nanoseconds to convert

    Returns:
        int: NULL_LONG if ns is NULL_LONG

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.nanosToMillis(ns)
    except Exception as e:
        raise DHError(e) from e


def nanos_to_datetime(ns: int) -> DateTime:
    """ Converts a value of nanoseconds from Epoch to a DateTime.

    Args:
        ns (long): the long nanoseconds since Epoch value to convert

    Returns:
        DateTime
    """
    try:
        return _JDateTimeUtils.nanosToDateTime(ns)
    except Exception as e:
        raise DHError(e) from e


def plus_period(dt: DateTime, period: Period) -> DateTime:
    """ Adds a period to a DateTime.

    Args:
        dt (DateTime): the starting DateTime value
        period (Period): the Period to add to the DateTime

    Returns:
        DateTime: None if either dt or period is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.plus(dt, period)
    except Exception as e:
        raise DHError(e) from e


def plus_nanos(dt: DateTime, ns: int) -> DateTime:
    """ Adds nanoseconds to a DateTime.

    Args:
        dt (DateTime): the starting DateTime value
        ns (int): the number of nanoseconds to add to DateTime

    Returns:
        DateTime: None if dt is None or ns is NULL_LONG

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.plus(dt, ns)
    except Exception as e:
        raise DHError(e) from e


def second_of_day(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of seconds since midnight for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the seconds
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.secondOfDay(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def second_of_minute(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the number of seconds since the top of the minute for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the seconds
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.secondOfMinute(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def upper_bin(dt, interval: int, offset: int = 0):
    """ Returns a DateTime value, which is at the ending (upper) end of a time range defined by the interval
     nanoseconds. For example, a 5*MINUTE intervalNanos value would return the DateTime value for the end of the five
     minute window that contains the input date time.

    Args:
        dt (DateTime): the DateTime for which to evaluate the end of the containing window
        interval (int): the size of the window in nanoseconds
        offset (int): the window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
              one minute. Default is 0

    Returns:
        DateTime

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.upperBin(dt, interval, offset)
    except Exception as e:
        raise DHError(e) from e


def year(dt: DateTime, tz: TimeZone) -> int:
    """ Returns an int value of the year for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the year
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int: NULL_INT if dt is None

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.year(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e


def year_of_century(dt: DateTime, tz: TimeZone) -> int:
    """ Returns the two-digit year for a DateTime in the specified time zone.

    Args:
        dt (DateTime): the DateTime for which to find the year
        tz (TimeZone): the TimeZone to use when interpreting the DateTime

    Returns:
        int

    Raises:
        DHError
    """
    try:
        return _JDateTimeUtils.yearOfCentury(dt, tz.value)
    except Exception as e:
        raise DHError(e) from e




#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:
#TODO:









    #
    # // region Binning
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static DateTime lowerBin(@Nullable final DateTime dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant lowerBin(@Nullable final Instant dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime lowerBin(@Nullable final ZonedDateTime dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static DateTime lowerBin(final @Nullable DateTime dateTime, long intervalNanos, long offset) {
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant lowerBin(final @Nullable Instant dateTime, long intervalNanos, long offset) {
    #
    # /**
    #  * Returns a date time value, which is at the starting (lower) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the start of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the start of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime lowerBin(final @Nullable ZonedDateTime dateTime, long intervalNanos, long offset) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static DateTime upperBin(final @Nullable DateTime dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant upperBin(final @Nullable Instant dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime upperBin(final @Nullable ZonedDateTime dateTime, long intervalNanos) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static DateTime upperBin(@Nullable final DateTime dateTime, long intervalNanos, long offset) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant upperBin(@Nullable final Instant dateTime, long intervalNanos, long offset) {
    #
    # /**
    #  * Returns a date time value, which is at the ending (upper) end of a time range defined by the interval
    #  * nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date time value for the end of the
    #  * five-minute window that contains the input date time.
    #  *
    #  * @param dateTime date time for which to evaluate the start of the containing window.
    #  * @param intervalNanos size of the window in nanoseconds.
    #  * @param offset The window start offset in nanoseconds. For example, a value of MINUTE would offset all windows by
    #  *        one minute.
    #  * @return null if either input is null; otherwise, a date time representing the end of the window.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime upperBin(@Nullable final ZonedDateTime dateTime, long intervalNanos, long offset) {
    #
    # // endregion
    #
    # // region Format
    #
    # /**
    #  * Returns a nanosecond duration formatted as a "hhh:mm:ss.nnnnnnnnn" string.
    #  *
    #  * @param nanos nanoseconds, or null if the input is {@link QueryConstants#NULL_LONG}.
    #  * @return the nanosecond duration formatted as a "hhh:mm:ss.nnnnnnnnn" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatNanos(long nanos) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @param timeZone time zone to use when formatting the string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDateTime(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @param timeZone time zone to use when formatting the string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDateTime(@Nullable final DateTime dateTime, @Nullable final ZoneId timeZone) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-ddThh:mm:ss.SSSSSSSSS TZ" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDateTime(@Nullable final ZonedDateTime dateTime) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-dd" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @param timeZone time zone to use when formatting the string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-dd" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDate(@Nullable final DateTime dateTime, @Nullable final ZoneId timeZone) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-dd" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @param timeZone time zone to use when formatting the string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-dd" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDate(@Nullable final Instant dateTime, @Nullable final ZoneId timeZone) {
    #
    # /**
    #  * Returns a DateTime formatted as a "yyyy-MM-dd" string.
    #  *
    #  * @param dateTime time to format as a string.
    #  * @return null if either input is null; otherwise, the time formatted as a "yyyy-MM-dd" string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static String formatDate(@Nullable final ZonedDateTime dateTime) {
    #
    # // endregion
    #
    # // region Parse
    #
    # /**
    #  * Parses the string argument as a time zone.
    #  *
    #  * @param s string to be converted
    #  * @return a {@link ZoneId} represented by the input string.
    #  * @throws RuntimeException if the string cannot be converted.
    #  * @see ZoneId
    #  * @see TimeZoneAliases
    #  */
    # @ScriptApi
    # @NotNull
    # public static ZoneId parseTimeZone(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a time zone.
    #  *
    #  * @param s string to be converted
    #  * @return a {@link ZoneId} represented by the input string, or null if the string can not be parsed.
    #  * @see ZoneId
    #  * @see TimeZoneAliases
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZoneId parseTimeZoneQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a time duration in nanoseconds.
    #  *
    #  * Time duration strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]} or as a duration string formatted as {@code [-]PnDTnHnMn.nS}.
    #  *
    #  * @param s string to be converted.
    #  * @return the number of nanoseconds represented by the string.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see #parseDuration(String)
    #  * @see #parseDurationQuiet(String)
    #  */
    # @ScriptApi
    # public static long parseNanos(@NotNull String s) {
    #
    # /**
    #  * Parses the string argument as a time duration in nanoseconds.
    #  *
    #  * Time duration strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]} or as a duration string formatted as {@code [-]PnDTnHnMn.nS}.
    #  *
    #  * @param s string to be converted.
    #  * @return the number of nanoseconds represented by the string, or {@link QueryConstants#NULL_LONG} if the string cannot be parsed.
    #  * @see #parseDuration(String)
    #  * @see #parseDurationQuiet(String)
    #  */
    # @ScriptApi
    # public static long parseNanosQuiet(@Nullable String s) {
    #
    # /**
    #  * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months, years, etc.).
    #  *
    #  * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where the
    #  * coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    #  * begin with a negative sign.
    #  *
    #  * Examples:
    #  * <pre>
    #  *   "P2Y"             -- Period.ofYears(2)
    #  *   "P3M"             -- Period.ofMonths(3)
    #  *   "P4W"             -- Period.ofWeeks(4)
    #  *   "P5D"             -- Period.ofDays(5)
    #  *   "P1Y2M3D"         -- Period.of(1, 2, 3)
    #  *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
    #  *   "P-1Y2M"          -- Period.of(-1, 2, 0)
    #  *   "-P1Y2M"          -- Period.of(-1, -2, 0)
    #  * </pre>
    #  *
    #  * @param s period string.
    #  * @return the period.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see Period#parse(CharSequence)
    #  */
    # @ScriptApi
    # @NotNull
    # public static Period parsePeriod(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a period, which is a unit of time in terms of calendar time (days, weeks, months, years, etc.).
    #  *
    #  * Period strings are formatted according to the ISO-8601 duration format as {@code PnYnMnD} and {@code PnW}, where the
    #  * coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    #  * begin with a negative sign.
    #  *
    #  * Examples:
    #  * <pre>
    #  *   "P2Y"             -- Period.ofYears(2)
    #  *   "P3M"             -- Period.ofMonths(3)
    #  *   "P4W"             -- Period.ofWeeks(4)
    #  *   "P5D"             -- Period.ofDays(5)
    #  *   "P1Y2M3D"         -- Period.of(1, 2, 3)
    #  *   "P1Y2M3W4D"       -- Period.of(1, 2, 25)
    #  *   "P-1Y2M"          -- Period.of(-1, 2, 0)
    #  *   "-P1Y2M"          -- Period.of(-1, -2, 0)
    #  * </pre>
    #  *
    #  * @param s period string.
    #  * @return the period, or null if the string can not be parsed.
    #  * @see Period#parse(CharSequence)
    #  */
    # @ScriptApi
    # @Nullable
    # public static Period parsePeriodQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
    #  * minutes, seconds, and nanoseconds).
    #  *
    #  * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
    #  * coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    #  * begin with a negative sign.
    #  *
    #  * Examples:
    #  * <pre>
    #  *    "PT20.345S" -- parses as "20.345 seconds"
    #  *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
    #  *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
    #  *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
    #  *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
    #  *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
    #  *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
    #  *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
    #  * </pre>
    #  *
    #  * @param s duration string.
    #  * @return the duration.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see Duration#parse(CharSequence)
    #  */
    # @ScriptApi
    # @NotNull
    # public static Duration parseDuration(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a duration, which is a unit of time in terms of clock time (24-hour days, hours,
    #  * minutes, seconds, and nanoseconds).
    #  *
    #  * Duration strings are formatted according to the ISO-8601 duration format as {@code [-]PnDTnHnMn.nS}, where the
    #  * coefficients can be positive or negative.  Zero coefficients can be omitted.  Optionally, the string can
    #  * begin with a negative sign.
    #  *
    #  * Examples:
    #  * <pre>
    #  *    "PT20.345S" -- parses as "20.345 seconds"
    #  *    "PT15M"     -- parses as "15 minutes" (where a minute is 60 seconds)
    #  *    "PT10H"     -- parses as "10 hours" (where an hour is 3600 seconds)
    #  *    "P2D"       -- parses as "2 days" (where a day is 24 hours or 86400 seconds)
    #  *    "P2DT3H4M"  -- parses as "2 days, 3 hours and 4 minutes"
    #  *    "PT-6H3M"    -- parses as "-6 hours and +3 minutes"
    #  *    "-PT6H3M"    -- parses as "-6 hours and -3 minutes"
    #  *    "-PT-6H+3M"  -- parses as "+6 hours and -3 minutes"
    #  * </pre>
    #  *
    #  * @param s duration string.
    #  * @return the duration, or null if the string can not be parsed.
    #  * @see Duration#parse(CharSequence)
    #  */
    # @ScriptApi
    # @Nullable
    # public static Duration parseDurationQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @NotNull
    # public static DateTime parseDateTime(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string, or null if the string can not be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @Nullable
    # public static DateTime parseDateTimeQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @NotNull
    # public static Instant parseInstant(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string, or null if the string can not be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @Nullable
    # public static Instant parseInstantQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @NotNull
    # public static ZonedDateTime parseZonedDateTime(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a date time.
    #  *
    #  * Date time strings are formatted according to the ISO 8601 date time format {@code yyyy-MM-ddThh:mm:ss[.SSSSSSSSS] TZ} and others.
    #  *
    #  * @param s date time string.
    #  * @return a date time represented by the input string, or null if the string can not be parsed.
    #  * @see DateTimeFormatter#ISO_INSTANT
    #  */
    # @ScriptApi
    # @Nullable
    # public static ZonedDateTime parseZonedDateTimeQuiet(@Nullable final String s) {
    #
    # /**
    #  * Returns a {@link ChronoField} indicating the level of precision in a time or datetime string.
    #  *
    #  * @param s time string.
    #  * @return {@link ChronoField} for the finest units in the string (e.g. "10:00:00" would yield SecondOfMinute).
    #  * @throws RuntimeException if the string cannot be parsed.
    #  */
    # @ScriptApi
    # @NotNull
    # public static ChronoField parseTimePrecision(@NotNull final String s) {
    #
    # /**
    #  * Returns a {@link ChronoField} indicating the level of precision in a time or datetime string.
    #  *
    #  * @param s time string.
    #  * @return null if the time string cannot be parsed; otherwise, a {@link ChronoField} for the finest units in the
    #  *      string (e.g. "10:00:00" would yield SecondOfMinute).
    #  * @throws RuntimeException if the string cannot be converted, otherwise a {@link DateTime} from the parsed string.
    #  */
    # @ScriptApi
    # @Nullable
    # public static ChronoField parseTimePrecisionQuiet(@Nullable final String s) {
    #
    # /**
    #  * Format style for a date string.
    #  */
    # @ScriptApi
    # public enum DateStyle {
    #     /**
    #      * Month, day, year date format.
    #      */
    #     MDY,
    #     /**
    #      * Day, month, year date format.
    #      */
    #     DMY,
    #     /**
    #      * Year, month, day date format.
    #      */
    #     YMD
    # }
    #
    # // see if we can match one of the slash-delimited styles, the interpretation of which requires knowing the
    # // system date style setting (for example Europeans often write dates as d/m/y).
    # @NotNull
    # private static LocalDate matchLocalDate(final Matcher matcher, final DateStyle dateStyle) {
    #
    # /**
    #  * Converts a string into a local date.
    #  * A local date is a date without a time or time zone.
    #  *
    #  * The ideal date format is YYYY-MM-DD since it's the least ambiguous, but other formats are supported.
    #  *
    #  * Supported formats:
    #  * - YYYY-MM-DD
    #  * - YYYYMMDD
    #  * - YYYY/MM/DD
    #  * - MM/DD/YYYY
    #  * - MM-DD-YYYY
    #  * - DD/MM/YYYY
    #  * - DD-MM-YYYY
    #  * - YY/MM/DD
    #  * - YY-MM-DD
    #  * - MM/DD/YY
    #  * - MM-DD-YY
    #  * - DD/MM/YY
    #  * - DD-MM-YY
    #  *
    #  * If the format matches the ISO YYYY-MM-DD or YYYYMMDD formats, the date style is ignored.
    #  *
    #  * @param s date string.
    #  * @param dateStyle style the date string is formatted in.
    #  * @return local date.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  */
    # @ScriptApi
    # @NotNull
    # public static LocalDate parseDate(@NotNull final String s, @Nullable final DateStyle dateStyle) {
    #
    # /**
    #  * Parses the string argument as a local date, which is a date without a time or time zone.
    #  *
    #  * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
    #  *
    #  * Supported formats:
    #  * - {@code YYYY-MM-DD}
    #  * - {@code YYYYMMDD}
    #  * - {@code YYYY/MM/DD}
    #  * - {@code MM/DD/YYYY}
    #  * - {@code MM-DD-YYYY}
    #  * - {@code DD/MM/YYYY}
    #  * - {@code DD-MM-YYYY}
    #  * - {@code YY/MM/DD}
    #  * - {@code YY-MM-DD}
    #  * - {@code MM/DD/YY}
    #  * - {@code MM-DD-YY}
    #  * - {@code DD/MM/YY}
    #  * - {@code DD-MM-YY}
    #  *
    #  * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
    #  *
    #  *
    #  * @param s date string.
    #  * @param dateStyle style the date string is formatted in.
    #  * @return local date, or null if the string can not be parsed.
    #  */
    # @ScriptApi
    # @Nullable
    # public static LocalDate parseDateQuiet(@Nullable final String s, @Nullable final DateStyle dateStyle) {
    #
    # /**
    #  * Parses the string argument as a local date, which is a date without a time or time zone.
    #  *
    #  * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
    #  *
    #  * Supported formats:
    #  * - {@code YYYY-MM-DD}
    #  * - {@code YYYYMMDD}
    #  * - {@code YYYY/MM/DD}
    #  * - {@code MM/DD/YYYY}
    #  * - {@code MM-DD-YYYY}
    #  * - {@code DD/MM/YYYY}
    #  * - {@code DD-MM-YYYY}
    #  * - {@code YY/MM/DD}
    #  * - {@code YY-MM-DD}
    #  * - {@code MM/DD/YY}
    #  * - {@code MM-DD-YY}
    #  * - {@code DD/MM/YY}
    #  * - {@code DD-MM-YY}
    #  *
    #  * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
    #  *
    #  *
    #  * @param s date string.
    #  * @return local date parsed according to the default date style.
    #  * @throws RuntimeException if the string cannot be parsed.
    #  */
    # @ScriptApi
    # @NotNull
    # public static LocalDate parseDate(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a local date, which is a date without a time or time zone.
    #  *
    #  * The ideal date format is {@code YYYY-MM-DD} since it's the least ambiguous, but other formats are supported.
    #  *
    #  * Supported formats:
    #  * - {@code YYYY-MM-DD}
    #  * - {@code YYYYMMDD}
    #  * - {@code YYYY/MM/DD}
    #  * - {@code MM/DD/YYYY}
    #  * - {@code MM-DD-YYYY}
    #  * - {@code DD/MM/YYYY}
    #  * - {@code DD-MM-YYYY}
    #  * - {@code YY/MM/DD}
    #  * - {@code YY-MM-DD}
    #  * - {@code MM/DD/YY}
    #  * - {@code MM-DD-YY}
    #  * - {@code DD/MM/YY}
    #  * - {@code DD-MM-YY}
    #  *
    #  * If the format matches the ISO {@code YYYY-MM-DD} or {@code YYYYMMDD} formats, the date style is ignored.
    #  *
    #  *
    #  * @param s date string.
    #  * @return local date parsed according to the default date style, or null if the string can not be parsed.
    #  */
    # @ScriptApi
    # @Nullable
    # public static LocalDate parseDateQuiet(@Nullable final String s) {
    #
    # /**
    #  * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a date or timezone.
    #  *
    #  * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
    #  *
    #  * @param s string to be converted
    #  * @return a {@link LocalTime} represented by the input string.
    #  * @throws RuntimeException if the string cannot be converted, otherwise a {@link LocalTime} from the parsed string.
    #  */
    # @ScriptApi
    # @NotNull
    # public static LocalTime parseLocalTime(@NotNull final String s) {
    #
    # /**
    #  * Parses the string argument as a local time, which is the time that would be read from a clock and does not have a date or timezone.
    #  *
    #  * Local time strings can be formatted as {@code hh:mm:ss[.nnnnnnnnn]}.
    #  *
    #  * @param s string to be converted
    #  * @return a {@link LocalTime} represented by the input string, or null if the string can not be parsed.
    #  */
    # @ScriptApi
    # @Nullable
    # public static LocalTime parseLocalTimeQuiet(@Nullable final String s) {
    #
    # // endregion