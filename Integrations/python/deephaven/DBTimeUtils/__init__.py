
"""
Utilities for Deephaven date/time storage and manipulation.
"""



"""
Utilities for creating plots.
"""

#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

#############################################################################
#               This code is auto generated. DO NOT EDIT FILE!
# Run generatePythonIntegrationStaticMethods or
# "./gradlew :Generators:generatePythonIntegrationStaticMethods" to generate
#############################################################################


import jpy
import wrapt


_java_type_ = None  # None until the first define_symbols() call
DBTimeZone = None  #: Deephaven timezone class (io.deephaven.db.tables.utils.DBTimeZone).
DBDateTime = None  #: Deephaven date-time class (io.deephaven.db.tables.utils.DBDateTime).

SECOND = 1000000000  #: One second in nanoseconds.
MINUTE = 60*SECOND   #: One minute in nanoseconds.
HOUR = 60*MINUTE     #: One hour in nanoseconds.
DAY = 24*HOUR        #: One day in nanoseconds.
WEEK = 7*DAY         #: One week in nanoseconds.
YEAR = 52*WEEK       #: One year in nanoseconds.


def _defineSymbols():
    """
    Defines appropriate java symbol, which requires that the jvm has been initialized through the :class:`jpy` module,
    for use throughout the module AT RUNTIME. This is versus static definition upon first import, which would lead to an
    exception if the jvm wasn't initialized BEFORE importing the module.
    """

    if not jpy.has_jvm():
        raise SystemError("No java functionality can be used until the JVM has been initialized through the jpy module")

    global _java_type_, DBTimeZone
    if _java_type_ is not None:
        return
    # This will raise an exception if the desired object is not the classpath
    _java_type_ = jpy.get_type("io.deephaven.db.tables.utils.DBTimeUtils")
    DBTimeZone = jpy.get_type("io.deephaven.db.tables.utils.DBTimeZone")
    DBDateTime = jpy.get_type("io.deephaven.db.tables.utils.DBDateTime")


# every module method should be decorated with @_passThrough
@wrapt.decorator
def _passThrough(wrapped, instance, args, kwargs):
    """
    For decoration of module methods, to define necessary symbols at runtime

    :param wrapped: the method to be decorated
    :param instance: the object to which the wrapped function was bound when it was called
    :param args: the argument list for `wrapped`
    :param kwargs: the keyword argument dictionary for `wrapped`
    :return: the decorated version of the method
    """

    _defineSymbols()
    return wrapped(*args, **kwargs)


# Define all of our functionality, if currently possible
try:
    _defineSymbols()
except Exception as e:
    pass


@_passThrough
def autoEpochToTime(epoch):
    """
    Converts a long offset from Epoch value to a DBDateTime. This method uses expected date ranges to
     infer whether the passed value is in milliseconds, microseconds, or nanoseconds. Thresholds used are
     TimeConstants.MICROTIME_THRESHOLD divided by 1000 for milliseconds, as-is for microseconds, and
     multiplied by 1000 for nanoseconds. The value is tested to see if its ABS exceeds the threshold. E.g. a value
     whose ABS is greater than 1000 * TimeConstants.MICROTIME_THRESHOLD will be treated as nanoseconds.
    
    :param epoch: (long) - The long Epoch offset value to convert.
    :return: (io.deephaven.db.tables.utils.DBDateTime) null, if the input is equal to QueryConstants.NULL_LONG, otherwise a DBDateTime based
     on the inferred conversion.
    """
    
    return _java_type_.autoEpochToTime(epoch)


@_passThrough
def cappedTimeOffset(original, period, cap):
    """
    Returns a DBDateTime value based on a starting value and a DBPeriod to add to it,
     but with a cap max value which is returned in case the starting value plus period exceeds the cap.
    
    :param original: (io.deephaven.db.tables.utils.DBDateTime) - The starting DBDateTime value.
    :param period: (io.deephaven.db.tables.utils.DBPeriod) - The DBPeriod to add to dateTime.
    :param cap: (io.deephaven.db.tables.utils.DBDateTime) - A DBDateTime value to use as the maximum return value.
    :return: (io.deephaven.db.tables.utils.DBDateTime) a null DBDateTime if either original or period are null;
     the starting DBDateTime plus the specified period, if the result is not too large for
     a DBDateTime and does not exceed the cap value; the cap value if this is less than offset plus period.
     Throws a DBDateTimeOverflowException if the
     resultant value is more than max long nanoseconds from Epoch.
    """
    
    return _java_type_.cappedTimeOffset(original, period, cap)


@_passThrough
def convertDate(s):
    """
    Attempt to convert the given string to a LocalDate. This should not accept dates with times, as we want those
     to be interpreted as DBDateTime values. The ideal date format is YYYY-MM-DD since it's the least ambiguous, but
     this method also parses slash-delimited dates according to the system "date style".
    
    :param s: (java.lang.String) - the date string to convert
    :return: java.time.LocalDate
    """
    
    return _java_type_.convertDate(s)


@_passThrough
def convertDateQuiet(*args):
    """
    Attempt to convert the given string to a LocalDate. This should not accept dates with times, as we want those
     to be interpreted as DBDateTime values. The ideal date format is YYYY-MM-DD since it's the least ambiguous.
    
    *Overload 1*  
      :param s: (java.lang.String) - the date string to convert
      :return: (java.time.LocalDate) the LocalDate formatted using the default date style.
      
    *Overload 2*  
      :param s: (java.lang.String) - the date string
      :param dateStyle: (io.deephaven.db.tables.utils.DBTimeUtils.DateStyle) - indicates how to interpret slash-delimited dates
      :return: (java.time.LocalDate) the LocalDate
    """
    
    return _java_type_.convertDateQuiet(*args)


@_passThrough
def convertDateTime(s):
    """
    Converts a DateTime String from a few specific zoned formats to a DBDateTime
    
    :param s: (java.lang.String) - String to be converted, usually in the form yyyy-MM-ddThh:mm:ss and with optional sub-seconds
              after an optional decimal point, followed by a mandatory time zone character code
    :return: io.deephaven.db.tables.utils.DBDateTime
    """
    
    return _java_type_.convertDateTime(s)


@_passThrough
def convertDateTimeQuiet(s):
    """
    Converts a DateTime String from a few specific zoned formats to a DBDateTime
    
    :param s: (java.lang.String) - String to be converted, usually in the form yyyy-MM-ddThh:mm:ss and with optional sub-seconds
              after an optional decimal point, followed by a mandatory time zone character code
    :return: (io.deephaven.db.tables.utils.DBDateTime) A DBDateTime from the parsed String, or null if the format is not recognized or an exception occurs
    """
    
    return _java_type_.convertDateTimeQuiet(s)


@_passThrough
def convertExpression(formula):
    """
    Converts an expression, replacing DBDateTime and DBPeriod literals with references
     to constant DBDateTime/DBPeriod instances.
    
    :param formula: (java.lang.String) - The formula to convert.
    :return: (io.deephaven.db.tables.utils.DBTimeUtils.Result) A DBTimeUtils.Result object, which includes the converted formula string,
     a string of instance variable declarations, and a map describing the names
     and types of these instance variables.
    """
    
    return _java_type_.convertExpression(formula)


@_passThrough
def convertLocalTimeQuiet(s):
    """
    Converts a time String in the form hh:mm:ss[.nnnnnnnnn] to a LocalTime.
    
    :param s: (java.lang.String) - The String to convert.
    :return: (java.time.LocalTime) null if the String cannot be parsed, otherwise a LocalTime.
    """
    
    return _java_type_.convertLocalTimeQuiet(s)


@_passThrough
def convertPeriod(s):
    """
    Converts a String into a DBPeriod object.
    
    :param s: (java.lang.String) - The String to convert in the form of numbertype, e.g. 1W for one week, and Tnumbertype for times, e.g.
     T1M for one minute.
    :return: io.deephaven.db.tables.utils.DBPeriod
    """
    
    return _java_type_.convertPeriod(s)


@_passThrough
def convertPeriodQuiet(s):
    """
    Converts a String into a DBPeriod object.
    
    :param s: (java.lang.String) - The String to convert in the form of numbertype, e.g. 1W for one week, and Tnumbertype for times, e.g.
     T1M for one minute.
    :return: (io.deephaven.db.tables.utils.DBPeriod) null if the String cannot be parsed, otherwise a DBPeriod object.
    """
    
    return _java_type_.convertPeriodQuiet(s)


@_passThrough
def convertTime(s):
    """
    Converts a String time to nanoseconds from Epoch. The format for the String is: hh:mm:ss[.nnnnnnnnn].
    
    :param s: (java.lang.String) - The String to be evaluated and converted.
    :return: (long) A long value representing an Epoch offset in nanoseconds. Throws RuntimeException if the
     String cannot be parsed.
    """
    
    return _java_type_.convertTime(s)


@_passThrough
def convertTimeQuiet(s):
    """
    Converts a time String in the form hh:mm:ss[.nnnnnnnnn] to a long nanoseconds offset from Epoch.
    
    :param s: (java.lang.String) - The String to convert.
    :return: (long) QueryConstants.NULL_LONG if the String cannot be parsed, otherwise long nanoseconds offset from Epoch.
    """
    
    return _java_type_.convertTimeQuiet(s)


@_passThrough
def createFormatter(timeZoneName):
    """
    Create a DateTimeFormatter formatter with the specified time zone name using the standard yyyy-MM-dd format.
    
    :param timeZoneName: (java.lang.String) - the time zone name
    :return: (java.time.format.DateTimeFormatter) a formatter set for the specified time zone
    """
    
    return _java_type_.createFormatter(timeZoneName)


@_passThrough
def currentDate(timeZone):
    """
    Returns a String of the current date in the specified DBTimeZone.
    
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to reference when evaluating the current date for "now".
    :return: (java.lang.String) A String in format yyyy-MM-dd.
    """
    
    return _java_type_.currentDate(timeZone)


@_passThrough
def currentDateNy():
    """
    Provides a String representing the current date in the New York time zone or, if a custom currentDateNyOverride
     has been set, the date provided by that override.
    
    :return: (java.lang.String) A String in yyyy-MM-dd format.
    """
    
    return _java_type_.currentDateNy()


@_passThrough
def currentTime():
    """
    Provides the current date/time, or, if a custom timeProvider has been configured, provides the
     current time according to the custom provider.
    
    :return: (io.deephaven.db.tables.utils.DBDateTime) A DBDateTime of the current date and time from the system or from the configured alternate
     time provider.
    """
    
    return _java_type_.currentTime()


@_passThrough
def dateAtMidnight(dateTime, timeZone):
    """
    Returns a DBDateTime for the requested DBDateTime at midnight in the specified
     time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - DBDateTime for which the new value at midnight should be calculated.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - DBTimeZone for which the new value at midnight should be calculated.
    :return: (io.deephaven.db.tables.utils.DBDateTime) A null DBDateTime if either input is null, otherwise a DBDateTime representing
     midnight for the date and time zone of the inputs.
    """
    
    return _java_type_.dateAtMidnight(dateTime, timeZone)


@_passThrough
def dayOfMonth(dateTime, timeZone):
    """
    Returns an int value of the day of the month for a DBDateTime and specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the month.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the day of the month represented by the DBDateTime when interpreted in the specified
     time zone.
    """
    
    return _java_type_.dayOfMonth(dateTime, timeZone)


@_passThrough
def dayOfMonthNy(dateTime):
    """
    Returns an int value of the day of the month for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the month.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of the day of the month represented by the DBDateTime when interpreted in the New York
     time zone.
    """
    
    return _java_type_.dayOfMonthNy(dateTime)


@_passThrough
def dayOfWeek(dateTime, timeZone):
    """
    Returns an int value of the day of the week for a DBDateTime in the specified time zone,
     with 1 being Monday and 7 being Sunday.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the week.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the day of the week represented by the DBDateTime when interpreted in the specified
     time zone.
    """
    
    return _java_type_.dayOfWeek(dateTime, timeZone)


@_passThrough
def dayOfWeekNy(dateTime):
    """
    Returns an int value of the day of the week for a DBDateTime in the New York time zone,
     with 1 being Monday and 7 being Sunday.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the week.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of the day of the week represented by the DBDateTime when interpreted in the New York
     time zone.
    """
    
    return _java_type_.dayOfWeekNy(dateTime)


@_passThrough
def dayOfYear(dateTime, timeZone):
    """
    Returns an int value of the day of the year (Julian date) for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the year.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the day of the year represented by the DBDateTime when interpreted in the specified
     time zone.
    """
    
    return _java_type_.dayOfYear(dateTime, timeZone)


@_passThrough
def dayOfYearNy(dateTime):
    """
    Returns an int value of the day of the year (Julian date) for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the day of the year.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of the day of the year represented by the DBDateTime when interpreted in the New York
     time zone.
    """
    
    return _java_type_.dayOfYearNy(dateTime)


@_passThrough
def diffDay(start, end):
    """
    Returns a double value of the number of days difference between two DBDateTime values.
    
    :param start: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime.
    :param end: (io.deephaven.db.tables.utils.DBDateTime) - The second DBDateTime.
    :return: (double) QueryConstants.NULL_LONG if either input is null;
     a double value of the number of days obtained from the first DBDateTime value minus d2,
     if the intermediate value of nanoseconds difference between the two dates
     is not out of range for a long value; or throws a DBDateTimeOverflowException if the
     intermediate value would be more than min long or max long nanoseconds from Epoch. 
     Note that the subtraction is done based the nanosecond offsets of the two dates from Epoch, so,
     if either date is before Epoch (negative offset), the result may be unexpected.
     If the second value is greater than the first value, the result will be negative.
    """
    
    return _java_type_.diffDay(start, end)


@_passThrough
def diffNanos(d1, d2):
    """
    Returns the difference in nanoseconds between two DBDateTime values.
    
    :param d1: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime.
    :param d2: (io.deephaven.db.tables.utils.DBDateTime) - The second DBDateTime.
    :return: (long) QueryConstants.NULL_LONG if either input is null;
     the long nanoseconds from Epoch value of the first DBDateTime minus d2, if the result is not out of range
     for a long value; or throws a DBDateTimeOverflowException if the
     resultant value would be more than min long or max long nanoseconds from Epoch. 
     Note that the subtraction is done based the nanosecond offsets of the two dates from Epoch, so,
     if either date is before Epoch (negative offset), the result may be unexpected.
     If the second value is greater than the first value, the result will be negative.
    """
    
    return _java_type_.diffNanos(d1, d2)


@_passThrough
def diffYear(start, end):
    """
    Returns a double value of the number of 365 day units difference between two DBDateTime values.
    
    :param start: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime.
    :param end: (io.deephaven.db.tables.utils.DBDateTime) - The second DBDateTime.
    :return: (double) QueryConstants.NULL_LONG if either input is null;
     a double value of the number of 365 day periods obtained from the first DBDateTime value minus d2,
     if the intermediate value of nanoseconds difference between the two dates
     is not out of range for a long value; or throws a DBTimeUtils.DBDateTimeOverflowException if the
     intermediate value would be more than min long or max long nanoseconds from Epoch. 
     Note that the subtraction is done based the nanosecond offsets of the two dates from Epoch, so,
     if either date is before Epoch (negative offset), the result may be unexpected.
     If the second value is greater than the first value, the result will be negative.
    """
    
    return _java_type_.diffYear(start, end)


@_passThrough
def expressionToNanos(formula):
    """
    Converts a String date/time to nanoseconds from Epoch or a nanoseconds period. Three patterns are supported:
     yyyy-MM-ddThh:mm:ss[.nnnnnnnnn] TZ for date/time values
    hh:mm:ss[.nnnnnnnnn] for time values
    Period Strings in the form of numbertype, e.g. 1W for one week, and Tnumbertype for times, e.g. T1M for one
     minute
    
    :param formula: (java.lang.String) - The String to be evaluated and converted. Optionally, but preferred, enclosed in straight single
                    ticks.
    :return: (long) A long value representing an Epoch offset in nanoseconds for a time or date/time, or a duration
     in nanoseconds for a period. Throws DBTimeUtils.DBDateTimeOverflowException if the resultant value would be
     longer than max long, or IllegalArgumentException if expression cannot be evaluated.
    """
    
    return _java_type_.expressionToNanos(formula)


@_passThrough
def format(*args):
    """
    Returns a String date/time representation.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to format as a String.
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when formatting the String.
      :return: (java.lang.String) A null String if either input is null, otherwise a String formatted as yyyy-MM-ddThh:mm:ss.nnnnnnnnn TZ.
      
    *Overload 2*  
      :param nanos: (long) - The long number of nanoseconds offset from Epoch.
      :return: (java.lang.String) A String of varying format depending on the offset.
       For values greater than one day, the output will start with dddT
      For values with fractional seconds, the output will be trailed by .nnnnnnnnn
      e.g. output may be dddThh:mm:ss.nnnnnnnnn or subsets of this.
    """
    
    return _java_type_.format(*args)


@_passThrough
def formatDate(dateTime, timeZone):
    """
    Returns a String date representation of a DBDateTime interpreted for a specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to format as a String.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when formatting the String.
    :return: (java.lang.String) A null String if either input is null, otherwise a String formatted as yyyy-MM-dd.
    """
    
    return _java_type_.formatDate(dateTime, timeZone)


@_passThrough
def formatDateNy(dateTime):
    """
    Returns a String date representation of a DBDateTime interpreted for the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to format as a String.
    :return: (java.lang.String) A null String if the input is null, otherwise a String formatted as yyyy-MM-dd.
    """
    
    return _java_type_.formatDateNy(dateTime)


@_passThrough
def formatNy(dateTime):
    """
    Returns a String date/time representation of a DBDateTime interpreted for the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to format as a String.
    :return: (java.lang.String) A null String if the input is null, otherwise a String formatted as yyyy-MM-ddThh:mm:ss.nnnnnnnnn NY.
    """
    
    return _java_type_.formatNy(dateTime)


@_passThrough
def getExcelDateTime(*args):
    """
    Returns the Excel double time format representation of a DBDateTime.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to convert.
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
      :return: (double) 0.0 if either input is null, otherwise, a double value
       containing the Excel double format representation of a DBDateTime in the specified time zone.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to convert.
      :param timeZone: (java.util.TimeZone) - The TimeZone to use when interpreting the date/time.
      :return: (double) 0.0 if either input is null, otherwise, a double value
       containing the Excel double format representation of a DBDateTime in the specified time zone.
      
    *Overload 3*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to convert.
      :return: (double) 0.0 if the input is null, otherwise, a double value
       containing the Excel double format representation of a DBDateTime in the New York time zone.
    """
    
    return _java_type_.getExcelDateTime(*args)


@_passThrough
def getFinestDefinedUnit(timeDef):
    """
    Returns a ChronoField indicating the level of precision in a String time value.
    
    :param timeDef: (java.lang.String) - The time String to evaluate.
    :return: (java.time.temporal.ChronoField) null if the time String cannot be parsed, otherwise a ChronoField for the
     finest units in the String (e.g. "10:00:00" would yield SecondOfMinute).
    """
    
    return _java_type_.getFinestDefinedUnit(timeDef)


@_passThrough
def getPartitionFromTimestampMicros(dateTimeFormatter, timestampMicros):
    """
    Given a DateTimeFormatter and a timestamp in micros from epoch, return the date as a String in standard column-partition format of yyyy-MM-dd.
     A timestamp of NULL_LONG means use the system current time.
    
    :param dateTimeFormatter: (java.time.format.DateTimeFormatter) - the date formatter
    :param timestampMicros: (long) - the timestamp in micros
    :return: (java.lang.String) the formatted date
    """
    
    return _java_type_.getPartitionFromTimestampMicros(dateTimeFormatter, timestampMicros)


@_passThrough
def getPartitionFromTimestampMillis(dateTimeFormatter, timestampMillis):
    """
    Given a DateTimeFormatter and a timestamp in millis, return the date as a String in standard column-partition format of yyyy-MM-dd.
     A timestamp of NULL_LONG means use the system current time.
    
    :param dateTimeFormatter: (java.time.format.DateTimeFormatter) - the date formatter
    :param timestampMillis: (long) - the timestamp in millis
    :return: (java.lang.String) the formatted date
    """
    
    return _java_type_.getPartitionFromTimestampMillis(dateTimeFormatter, timestampMillis)


@_passThrough
def getPartitionFromTimestampNanos(dateTimeFormatter, timestampNanos):
    """
    Given a DateTimeFormatter and a timestamp in nanos from epoch, return the date as a String in standard column-partition format of yyyy-MM-dd.
     A timestamp of NULL_LONG means use the system current time.
    
    :param dateTimeFormatter: (java.time.format.DateTimeFormatter) - the date formatter
    :param timestampNanos: (long) - the timestamp in nanos
    :return: (java.lang.String) the formatted date
    """
    
    return _java_type_.getPartitionFromTimestampNanos(dateTimeFormatter, timestampNanos)


@_passThrough
def getPartitionFromTimestampSeconds(dateTimeFormatter, timestampSeconds):
    """
    Given a DateTimeFormatter and a timestamp in seconds from epoch, return the date as a String in standard column-partition format of yyyy-MM-dd.
     A timestamp of NULL_LONG means use the system current time.
    
    :param dateTimeFormatter: (java.time.format.DateTimeFormatter) - the date formatter
    :param timestampSeconds: (long) - the timestamp in seconds
    :return: (java.lang.String) the formatted date
    """
    
    return _java_type_.getPartitionFromTimestampSeconds(dateTimeFormatter, timestampSeconds)


@_passThrough
def getZonedDateTime(*args):
    """
    Converts a DBDateTime to a ZonedDateTime.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The a DBDateTime to convert.
      :return: (java.time.ZonedDateTime) A ZonedDateTime using the default time zone for the session
       as indicated by DBTimeZone.TZ_DEFAULT.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The a DBDateTime to convert.
      :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use for the conversion.
      :return: (java.time.ZonedDateTime) A ZonedDateTime using the specified time zone.
    """
    
    return _java_type_.getZonedDateTime(*args)


@_passThrough
def hourOfDay(dateTime, timeZone):
    """
    Returns an int value of the hour of the day for a DBDateTime in the specified time zone.
     The hour is on a 24 hour clock (0 - 23).
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the hour of the day.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the hour of the day represented by the DBDateTime when interpreted in the specified
     time zone.
    """
    
    return _java_type_.hourOfDay(dateTime, timeZone)


@_passThrough
def hourOfDayNy(dateTime):
    """
    Returns an int value of the hour of the day for a DBDateTime in the New York time zone.
     The hour is on a 24 hour clock (0 - 23).
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the hour of the day.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of the hour of the day represented by the DBDateTime when interpreted in the New York
     time zone.
    """
    
    return _java_type_.hourOfDayNy(dateTime)


@_passThrough
def isAfter(d1, d2):
    """
    Evaluates whether one DBDateTime value is later than a second DBDateTime value.
    
    :param d1: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime value to compare.
    :param d2: (io.deephaven.db.tables.utils.DBDateTime) - The second DBDateTime value to compare.
    :return: (boolean) Boolean true if d1 is later than d2, false if either value is null, or if
     d2 is equal to or later than d1.
    """
    
    return _java_type_.isAfter(d1, d2)


@_passThrough
def isBefore(d1, d2):
    """
    Evaluates whether one DBDateTime value is earlier than a second DBDateTime value.
    
    :param d1: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime value to compare.
    :param d2: (io.deephaven.db.tables.utils.DBDateTime) - The second DBDateTime value to compare.
    :return: (boolean) Boolean true if d1 is earlier than d2, false if either value is null, or if
     d2 is equal to or earlier than d1.
    """
    
    return _java_type_.isBefore(d1, d2)


@_passThrough
def lastBusinessDateNy(*args):
    """
    Provides a String representing the previous business date in the New York time zone using the NYSE calendar, or,
     if a custom lastBusinessDayNyOverride has been set, the date provided by that override.
    
    *Overload 1*  
      :return: (java.lang.String) A String in yyyy-MM-dd format.
      
    *Overload 2*  
      :param currentTimeMillis: (long) - The current date/time in milliseconds from Epoch to be used when determining
                                the previous business date. Typically this is System.currentTimeMillis() and is
                                passed in by calling the niladic variant of this method.
      :return: (java.lang.String) A String in yyyy-MM-dd format.
    """
    
    return _java_type_.lastBusinessDateNy(*args)


@_passThrough
def lowerBin(*args):
    """
    Returns a DBDateTime value, which is at the starting (lower) end of a time range defined
     by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date/time value
     for the start of the five minute window that contains the input date time.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to evaluate the start of the containing window.
      :param intervalNanos: (long) - The size of the window in nanoseconds.
      :return: (io.deephaven.db.tables.utils.DBDateTime) Null if either input is null, otherwise a DBDateTime representing the start of the
       window.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to evaluate the start of the containing window.
      :param intervalNanos: (long) - The size of the window in nanoseconds.
      :param offset: (long) - The window start offset in nanoseconds.  For example, a value of MINUTE would offset all windows by one minute.
      :return: (io.deephaven.db.tables.utils.DBDateTime) Null if either input is null, otherwise a DBDateTime representing the start of the
       window.
    """
    
    return _java_type_.lowerBin(*args)


@_passThrough
def microsOfMilli(dateTime, timeZone):
    """
    Returns the number of microseconds that have elapsed since the start of the millisecond represented by the
     provided dateTime in the specified time zone.
     Nanoseconds are rounded, not dropped -- '20:41:39.123456700' has 457 micros, not 456.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the microseconds.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of microseconds since the top of the millisecond for the date/time represented by the DBDateTime
     when interpreted in the specified time zone.
    """
    
    return _java_type_.microsOfMilli(dateTime, timeZone)


@_passThrough
def microsOfMilliNy(dateTime):
    """
    Returns the number of microseconds that have elapsed since the start of the millisecond represented by the
     provided dateTime in the New York time zone.
     Nanoseconds are rounded, not dropped -- '20:41:39.123456700' has 457 micros, not 456.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the microseconds.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of microseconds since the top of the millisecond for the date/time represented by the DBDateTime
     when interpreted in the New York time zone.
    """
    
    return _java_type_.microsOfMilliNy(dateTime)


@_passThrough
def microsToNanos(micros):
    """
    Converts microseconds to nanoseconds.
    
    :param micros: (long) - The long value of microseconds to convert.
    :return: (long) A QueryConstants.NULL_LONG if the input is null. Throws a DBTimeUtils.DBDateTimeOverflowException
     if the resultant value would exceed the range that can be stored in a long. Otherwise, returns a long
     containing the equivalent number of nanoseconds for the input in microseconds.
    """
    
    return _java_type_.microsToNanos(micros)


@_passThrough
def microsToTime(micros):
    """
    Converts a value of microseconds from Epoch in the UTC time zone to a DBDateTime.
    
    :param micros: (long) - The long microseconds value to convert.
    :return: (io.deephaven.db.tables.utils.DBDateTime) QueryConstants.NULL_LONG if the input is null, otherwise, a DBDateTime
     representation of the input.
    """
    
    return _java_type_.microsToTime(micros)


@_passThrough
def millis(dateTime):
    """
    Returns milliseconds since Epoch for a DBDateTime value.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which the milliseconds offset should be returned.
    :return: (long) A long value of milliseconds since Epoch, or a QueryConstants.NULL_LONG value if the DBDateTime is null.
    """
    
    return _java_type_.millis(dateTime)


@_passThrough
def millisOfDay(dateTime, timeZone):
    """
    Returns an int value of milliseconds since midnight for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the milliseconds since midnight.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of milliseconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.millisOfDay(dateTime, timeZone)


@_passThrough
def millisOfDayNy(dateTime):
    """
    Returns an int value of milliseconds since midnight for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the milliseconds since midnight.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of milliseconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.millisOfDayNy(dateTime)


@_passThrough
def millisOfSecond(dateTime, timeZone):
    """
    Returns an int value of milliseconds since the top of the second for a DBDateTime
     in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the milliseconds.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of milliseconds since the top of the second for the date/time represented by the DBDateTime
     when interpreted in the specified time zone.
    """
    
    return _java_type_.millisOfSecond(dateTime, timeZone)


@_passThrough
def millisOfSecondNy(dateTime):
    """
    Returns an int value of milliseconds since the top of the second for a DBDateTime
     in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the milliseconds.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of milliseconds since the top of the second for the date/time represented by the DBDateTime
     when interpreted in the New York time zone.
    """
    
    return _java_type_.millisOfSecondNy(dateTime)


@_passThrough
def millisToDateAtMidnight(millis, timeZone):
    """
    Returns a DBDateTime representing midnight in a selected time zone on the date specified by the a number of
     milliseconds from Epoch.
    
    :param millis: (long) - A long value of the number of milliseconds from Epoch for which the DBDateTime is to be calculated.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - DBTimeZone for which the new value at midnight should be calculated.
    :return: (io.deephaven.db.tables.utils.DBDateTime) A DBDateTime rounded down to midnight in the selected time zone for the specified number of milliseconds
     from Epoch.
    """
    
    return _java_type_.millisToDateAtMidnight(millis, timeZone)


@_passThrough
def millisToDateAtMidnightNy(millis):
    """
    Returns a DBDateTime representing midnight in New York time zone on the date specified by the a number of
     milliseconds from Epoch.
    
    :param millis: (long) - A long value of the number of milliseconds from Epoch for which the DBDateTime is to be calculated.
    :return: (io.deephaven.db.tables.utils.DBDateTime) A DBDateTime rounded down to midnight in the New York time zone for the specified number of milliseconds
     from Epoch.
    """
    
    return _java_type_.millisToDateAtMidnightNy(millis)


@_passThrough
def millisToNanos(millis):
    """
    Converts milliseconds to nanoseconds.
    
    :param millis: (long) - The long milliseconds value to convert.
    :return: (long) QueryConstants.NULL_LONG if the input is equal to QueryConstants.NULL_LONG. Throws
     DBTimeUtils.DBDateTimeOverflowException if the input is too large for conversion. Otherwise returns a long of
     the equivalent number of nanoseconds to the input.
    """
    
    return _java_type_.millisToNanos(millis)


@_passThrough
def millisToTime(millis):
    """
    Converts a value of milliseconds from Epoch in the UTC time zone to a DBDateTime.
    
    :param millis: (long) - The long milliseconds value to convert.
    :return: (io.deephaven.db.tables.utils.DBDateTime) QueryConstants.NULL_LONG if the input is null, otherwise, a DBDateTime
     representation of the input.
    """
    
    return _java_type_.millisToTime(millis)


@_passThrough
def minus(*args):
    """
    Subtracts one time from another.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The starting DBDateTime value.
      :param nanos: (long) - The long number of nanoseconds to subtract from dateTime.
      :return: (io.deephaven.db.tables.utils.DBDateTime) a null DBDateTime if either input is null;
       the starting DBDateTime minus the specified number of nanoseconds, if the result is not too negative for
       a DBDateTime; or throws a DBDateTimeOverflowException if the
       resultant value is more than min long nanoseconds from Epoch.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The starting DBDateTime value.
      :param period: (io.deephaven.db.tables.utils.DBPeriod) - The DBPeriod to subtract from dateTime.
      :return: (io.deephaven.db.tables.utils.DBDateTime) a null DBDateTime if either input is null;
       the starting DBDateTime minus the specified period, if the result is not too negative for
       a DBDateTime; or throws a DBDateTimeOverflowException if the
       resultant value is more than min long nanoseconds from Epoch.
      
    *Overload 3*  
      :param d1: (io.deephaven.db.tables.utils.DBDateTime) - The first DBDateTime.
      :param d2: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime to subtract from d1.
      :return: (long) QueryConstants.NULL_LONG if either input is null;
       the long nanoseconds from Epoch value of the first DBDateTime minus d2, if the result is not out of range
       for a long value; or throws a DBDateTimeOverflowException if the
       resultant value would be more than min long or max long nanoseconds from Epoch. 
       Note that the subtraction is done based the nanosecond offsets of the two dates from Epoch, so,
       if either date is before Epoch (negative offset), the result may be unexpected.
    """
    
    return _java_type_.minus(*args)


@_passThrough
def minuteOfDay(dateTime, timeZone):
    """
    Returns an int value of minutes since midnight for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the minutes.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of minutes since midnight for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.minuteOfDay(dateTime, timeZone)


@_passThrough
def minuteOfDayNy(dateTime):
    """
    Returns an int value of minutes since midnight for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the milliseconds since midnight.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of minutes since midnight for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.minuteOfDayNy(dateTime)


@_passThrough
def minuteOfHour(dateTime, timeZone):
    """
    Returns an int value of minutes since the top of the hour for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the minutes.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of minutes since the top of the hour for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.minuteOfHour(dateTime, timeZone)


@_passThrough
def minuteOfHourNy(dateTime):
    """
    Returns an int value of minutes since the top of the hour for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the minutes.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of minutes since the top of the hour for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.minuteOfHourNy(dateTime)


@_passThrough
def monthOfYear(dateTime, timeZone):
    """
    Returns an int value for the month of a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the month.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the month for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.  January is 1, February is 2, etc.
    """
    
    return _java_type_.monthOfYear(dateTime, timeZone)


@_passThrough
def monthOfYearNy(dateTime):
    """
    Returns an int value for the month of a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the month.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of the month for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.monthOfYearNy(dateTime)


@_passThrough
def nanos(*args):
    """
    Returns nanoseconds since Epoch for a DBDateTime value.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which the nanoseconds offset should be returned.
      :return: (long) A long value of nanoseconds since Epoch, or a NULL_LONG value if the DBDateTime is null.
      
    *Overload 2*  
      :param instant: java.time.Instant
      :return: long
    """
    
    return _java_type_.nanos(*args)


@_passThrough
def nanosOfDay(dateTime, timeZone):
    """
    Returns a long value of nanoseconds since midnight for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the nanoseconds since midnight.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (long) A QueryConstants.NULL_LONG if either input is null, otherwise, a long value
     of nanoseconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.nanosOfDay(dateTime, timeZone)


@_passThrough
def nanosOfDayNy(dateTime):
    """
    Returns a long value of nanoseconds since midnight for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the nanoseconds since midnight.
    :return: (long) A QueryConstants.NULL_LONG if the input is null, otherwise, a long value
     of nanoseconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.nanosOfDayNy(dateTime)


@_passThrough
def nanosOfSecond(dateTime, timeZone):
    """
    Returns a long value of nanoseconds since the top of the second for a DBDateTime
     in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the nanoseconds.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (long) A QueryConstants.NULL_LONG if either input is null, otherwise, a long value
     of nanoseconds since the top of the second for the date/time represented by the DBDateTime
     when interpreted in the specified time zone.
    """
    
    return _java_type_.nanosOfSecond(dateTime, timeZone)


@_passThrough
def nanosOfSecondNy(dateTime):
    """
    Returns a long value of nanoseconds since the top of the second for a DBDateTime
     in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the nanoseconds.
    :return: (long) A QueryConstants.NULL_LONG if the input is null, otherwise, a long value
     of nanoseconds since the top of the second for the date/time represented by the DBDateTime
     when interpreted in the New York time zone.
    """
    
    return _java_type_.nanosOfSecondNy(dateTime)


@_passThrough
def nanosToMicros(nanos):
    """
    Converts nanoseconds to microseconds.
    
    :param nanos: (long) - The long value of nanoseconds to convert.
    :return: (long) A QueryConstants.NULL_LONG if the input is null. Otherwise, returns a long
     containing the equivalent number of microseconds for the input in nanoseconds.
    """
    
    return _java_type_.nanosToMicros(nanos)


@_passThrough
def nanosToMillis(nanos):
    """
    Converts nanoseconds to milliseconds.
    
    :param nanos: (long) - The long value of nanoseconds to convert.
    :return: (long) A QueryConstants.NULL_LONG if the input is null. Otherwise, returns a long
     containing the equivalent number of milliseconds for the input in nanoseconds.
    """
    
    return _java_type_.nanosToMillis(nanos)


@_passThrough
def nanosToTime(nanos):
    """
    Converts a value of nanoseconds from Epoch to a DBDateTime.
    
    :param nanos: (long) - The long nanoseconds since Epoch value to convert.
    :return: (io.deephaven.db.tables.utils.DBDateTime) A DBDateTime for nanos, or null if nanos
     is equal to NULL_LONG.
    """
    
    return _java_type_.nanosToTime(nanos)


@_passThrough
def overrideLastBusinessDateNyFromCurrentDateNy():
    """
    Sets the lastBusinessDayNyOverride to the previous business day from a currently set
     currentDateNyOverride value. If currentDateNyOverride has not been set, this
     method has no effect.
    """
    
    return _java_type_.overrideLastBusinessDateNyFromCurrentDateNy()


@_passThrough
def plus(*args):
    """
    Adds one time from another.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The starting DBDateTime value.
      :param nanos: (long) - The long number of nanoseconds to add to dateTime.
      :return: (io.deephaven.db.tables.utils.DBDateTime) a null DBDateTime if either input is null;
       the starting DBDateTime plus the specified number of nanoseconds, if the result is not too large for
       a DBDateTime; or throws a DBDateTimeOverflowException if the
       resultant value is more than max long nanoseconds from Epoch.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The starting DBDateTime value.
      :param period: (io.deephaven.db.tables.utils.DBPeriod) - The DBPeriod to add to dateTime.
      :return: (io.deephaven.db.tables.utils.DBDateTime) a null DBDateTime if either input is null;
       the starting DBDateTime plus the specified period, if the result is not too large for
       a DBDateTime; or throws a DBDateTimeOverflowException if the
       resultant value is more than max long nanoseconds from Epoch.
    """
    
    return _java_type_.plus(*args)


@_passThrough
def secondOfDay(dateTime, timeZone):
    """
    Returns an int value of seconds since midnight for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the seconds.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of seconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.secondOfDay(dateTime, timeZone)


@_passThrough
def secondOfDayNy(dateTime):
    """
    Returns an int value of seconds since midnight for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the seconds.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of seconds since midnight for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.secondOfDayNy(dateTime)


@_passThrough
def secondOfMinute(dateTime, timeZone):
    """
    Returns an int value of seconds since the top of the minute for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the seconds.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of seconds since the top of the minute for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.secondOfMinute(dateTime, timeZone)


@_passThrough
def secondOfMinuteNy(dateTime):
    """
    Returns an int value of seconds since the top of the minute for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the seconds.
    :return: (int) A QueryConstants.NULL_INT if the input is null, otherwise, an int value
     of seconds since the top of the minute for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.secondOfMinuteNy(dateTime)


@_passThrough
def secondsToNanos(seconds):
    """
    Converts seconds to nanoseconds.
    
    :param seconds: (long) - The long value of seconds to convert.
    :return: (long) A QueryConstants.NULL_LONG if the input is null. Throws a DBTimeUtils.DBDateTimeOverflowException
     if the resultant value would exceed the range that can be stored in a long. Otherwise, returns a long
     containing the equivalent number of nanoseconds for the input in seconds.
    """
    
    return _java_type_.secondsToNanos(seconds)


@_passThrough
def secondsToTime(seconds):
    """
    Converts a value of seconds from Epoch in the UTC time zone to a DBDateTime.
    
    :param seconds: (long) - The long seconds value to convert.
    :return: (io.deephaven.db.tables.utils.DBDateTime) QueryConstants.NULL_LONG if the input is null, otherwise, a DBDateTime
     representation of the input.
    """
    
    return _java_type_.secondsToTime(seconds)


@_passThrough
def toDateTime(zonedDateTime):
    """
    Converts a ZonedDateTime to a DBDateTime.
    
    :param zonedDateTime: (java.time.ZonedDateTime) - The a ZonedDateTime to convert.
    :return: io.deephaven.db.tables.utils.DBDateTime
    """
    
    return _java_type_.toDateTime(zonedDateTime)


@_passThrough
def upperBin(*args):
    """
    Returns a DBDateTime value, which is at the ending (upper) end of a time range defined
     by the interval nanoseconds. For example, a 5*MINUTE intervalNanos value would return the date/time value
     for the end of the five minute window that contains the input date time.
    
    *Overload 1*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to evaluate the end of the containing window.
      :param intervalNanos: (long) - The size of the window in nanoseconds.
      :return: (io.deephaven.db.tables.utils.DBDateTime) Null if either input is null, otherwise a DBDateTime representing the end of the
       window.
      
    *Overload 2*  
      :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to evaluate the end of the containing window.
      :param intervalNanos: (long) - The size of the window in nanoseconds.
      :param offset: (long) - The window start offset in nanoseconds.  For example, a value of MINUTE would offset all windows by one minute.
      :return: (io.deephaven.db.tables.utils.DBDateTime) Null if either input is null, otherwise a DBDateTime representing the end of the
       window.
    """
    
    return _java_type_.upperBin(*args)


@_passThrough
def year(dateTime, timeZone):
    """
    Returns an int value of the year for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the year.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the year for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.year(dateTime, timeZone)


@_passThrough
def yearNy(dateTime):
    """
    Returns an int value of the year for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the year.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the year for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.yearNy(dateTime)


@_passThrough
def yearOfCentury(dateTime, timeZone):
    """
    Returns an int value of the two-digit year for a DBDateTime in the specified time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the year.
    :param timeZone: (io.deephaven.db.tables.utils.DBTimeZone) - The DBTimeZone to use when interpreting the date/time.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the two-digit year for the date/time represented by the DBDateTime when
     interpreted in the specified time zone.
    """
    
    return _java_type_.yearOfCentury(dateTime, timeZone)


@_passThrough
def yearOfCenturyNy(dateTime):
    """
    Returns an int value of the two-digit year for a DBDateTime in the New York time zone.
    
    :param dateTime: (io.deephaven.db.tables.utils.DBDateTime) - The DBDateTime for which to find the year.
    :return: (int) A QueryConstants.NULL_INT if either input is null, otherwise, an int value
     of the two-digit year for the date/time represented by the DBDateTime when
     interpreted in the New York time zone.
    """
    
    return _java_type_.yearOfCenturyNy(dateTime)
