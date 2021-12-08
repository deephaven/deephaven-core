#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#


##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import time

from deephaven import DateTimeUtils

if sys.version_info[0] < 3:
    int = long  # cheap python2/3 compliance - probably only required on a 32 bit system
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestDateTimeUtil(unittest.TestCase):
    """
    Test cases for the deephaven.DateTimeUtils module (performed locally) -
    """

    def testCurrent(self):
        """
        Test suite for getting DateTime object representing the current time
        """

        tz = jpy.get_type('io.deephaven.time.TimeZone').TZ_NY
        with self.subTest(msg="currentTime"):
            junk = DateTimeUtils.currentTime()
        with self.subTest(msg="currentDate"):
            junk = DateTimeUtils.currentDate(tz)
        with self.subTest(msg="currentTimeNy"):
            junk = DateTimeUtils.currentDateNy()

    def testDatetimeFromTimestamp(self):
        """
        Test suite for converting timestamp to DateTime
        """

        sampleTimestamp = time.time()  # seconds of type double
        with self.subTest(msg="autoEpochToTime"):
            autoDate = DateTimeUtils.autoEpochToTime(int(sampleTimestamp))
        with self.subTest(msg="secondsToTime"):
            secondDate = DateTimeUtils.secondsToTime(int(sampleTimestamp))
            self.assertTrue(autoDate.equals(secondDate))
        with self.subTest(msg="millisToTime"):
            millisDate = DateTimeUtils.millisToTime(1000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(millisDate))
        with self.subTest(msg="nanosToTime"):
            microsDate = DateTimeUtils.microsToTime(1000000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(microsDate))
        with self.subTest(msg="nanosToTime"):
            nanosDate = DateTimeUtils.nanosToTime(1000000000*int(sampleTimestamp))
            # NB: underscore literals work in Python 3.5 and after
            self.assertTrue(autoDate.equals(nanosDate))

    def testConversionFromString(self):
        """
        Coverage tests for converting strings of various forms to DateTime, Period, or nanosecond time (period)
        """

        # TODO: Methods which almost certainly have no business being wrapped...
        #   convertExpression(string), expressionToNanos(string), getFinestDefinedUnit(string)

        with self.subTest(msg="convertDateTime"):
            junk = DateTimeUtils.convertDateTime("2018-11-13T15:00:00.000000 NY")
        with self.subTest(msg="convertDateTimeQuiet"):
            junk = DateTimeUtils.convertDateTimeQuiet("2018-11-13T15:00:00.000000 NY")

        # appear to operate on string formatted as <days>T<hh>:<mm>:<ss>.<fractional seconds up to nanos>
        # NB: the documentation sucks, and I'm just guessing
        with self.subTest(msg="convertTime"):
            junk = DateTimeUtils.convertTime("1T02:04:16.3264128")
        with self.subTest(msg="convertTimeQuiet"):
            junk = DateTimeUtils.convertTimeQuiet("1T02:04:16.3264128")

        # appear to operate on string formatted as <years>Y<months>M<weeks>W<days>D<hours>H<minutes>M<seconds>S
        #   all literals could be upper or lower case
        # NB: the documentation sucks, and I'm mostly guessing
        with self.subTest(msg="convertPeriod"):
            junk = DateTimeUtils.convertPeriod("1Y1M1W1DT1H1M1S")
        with self.subTest(msg="convertPeriodQuiet"):
            junk = DateTimeUtils.convertPeriodQuiet("1Y1M1W1DT1H1M1S")

    def testTimestampFromDatetime(self):
        """
        Coverage tests for converting DateTime object to timestamp
        """

        # get current time
        now = DateTimeUtils.currentTime()  # unit tested above...

        with self.subTest(msg="millis"):
            junk = DateTimeUtils.millis(now)
        with self.subTest(msg="nanos"):
            junk = DateTimeUtils.nanos(now)
        with self.subTest(msg="getExcelDateTime(DateTime)"):
            junk = DateTimeUtils.getExcelDateTime(now)

        tz = jpy.get_type('io.deephaven.time.TimeZone').TZ_NY
        with self.subTest(msg="getExcelDateTime(DateTime, TimeZone)"):
            junk = DateTimeUtils.getExcelDateTime(now, tz)
        with self.subTest(msg="getExcelDateTime(DateTime, TimeZone)"):
            # NB: converting from TimeZone to TimeZone - should work...
            # should we even test this?
            junk = DateTimeUtils.getExcelDateTime(now, tz.getTimeZone().toTimeZone())

    def testZonedMethods(self):
        """
        Test suite for DateTime <-> ZonedDateTime - not clear this is ever to be used
        """

        # get current time
        now = DateTimeUtils.currentTime()  # unit tested above...
        with self.subTest(msg="getZonedDateTime(DateTime)"):
            junk = DateTimeUtils.getZonedDateTime(now)  # will have TZ_DEFAULT applied

        tz = jpy.get_type('io.deephaven.time.TimeZone').TZ_NY
        with self.subTest(msg="getZonedDateTime(DateTime, TimeZone)"):
            zoned = DateTimeUtils.getZonedDateTime(now, tz)
        with self.subTest(msg="toDateTime(ZonedDateTime)"):
            junk = DateTimeUtils.toDateTime(zoned)

    def testDateParts(self):
        """
        Coverage test suite for various date part extractions
        """

        tz = jpy.get_type('io.deephaven.time.TimeZone').TZ_NY
        # get current time
        now = DateTimeUtils.currentTime()

        # year parts
        with self.subTest(msg="year"):
            junk = DateTimeUtils.year(now, tz)
        with self.subTest(msg="yearOfCentury"):
            junk = DateTimeUtils.yearOfCentury(now, tz)
        with self.subTest(msg="yearNy"):
            junk = DateTimeUtils.yearNy(now)
        with self.subTest(msg="yearOfCenturyNy"):
            junk = DateTimeUtils.yearOfCenturyNy(now)

        # month parts
        with self.subTest(msg="monthOfYear"):
            junk = DateTimeUtils.monthOfYear(now, tz)
        with self.subTest(msg="monthOfYearNy"):
            junk = DateTimeUtils.monthOfYearNy(now)

        # day parts
        with self.subTest(msg="dayOfYear"):
            junk = DateTimeUtils.dayOfYear(now, tz)
        with self.subTest(msg="dayOfMonth"):
            junk = DateTimeUtils.dayOfMonth(now, tz)
        with self.subTest(msg="dayOfWeek"):
            junk = DateTimeUtils.dayOfWeek(now, tz)
        with self.subTest(msg="dayOfYearNy"):
            junk = DateTimeUtils.dayOfYearNy(now)
        with self.subTest(msg="dayOfMonthNy"):
            junk = DateTimeUtils.dayOfMonthNy(now)
        with self.subTest(msg="dayOfWeekNy"):
            junk = DateTimeUtils.dayOfWeekNy(now)

        # hour parts
        with self.subTest(msg="hourOfDay"):
            junk = DateTimeUtils.hourOfDay(now, tz)
        with self.subTest(msg="hourOfDayNy"):
            junk = DateTimeUtils.hourOfDayNy(now)

        # minute parts
        with self.subTest(msg="minuteOfDay"):
            junk = DateTimeUtils.minuteOfDay(now, tz)
        with self.subTest(msg="minuteOfHour"):
            junk = DateTimeUtils.minuteOfHour(now, tz)
        with self.subTest(msg="minuteOfDayNy"):
            junk = DateTimeUtils.minuteOfDayNy(now)
        with self.subTest(msg="minuteOfHourNy"):
            junk = DateTimeUtils.minuteOfHourNy(now)

        # second parts
        with self.subTest(msg="secondOfDay"):
            junk = DateTimeUtils.secondOfDay(now, tz)
        with self.subTest(msg="secondOfMinute"):
            junk = DateTimeUtils.secondOfMinute(now, tz)
        with self.subTest(msg="secondOfDayNy"):
            junk = DateTimeUtils.secondOfDayNy(now)
        with self.subTest(msg="secondOfMinuteNy"):
            junk = DateTimeUtils.secondOfMinuteNy(now)

        # millisecond parts
        with self.subTest(msg="millisOfDay"):
            junk = DateTimeUtils.millisOfDay(now, tz)
        with self.subTest(msg="millisOfSecond"):
            junk = DateTimeUtils.millisOfSecond(now, tz)
        with self.subTest(msg="millisOfDayNy"):
            junk = DateTimeUtils.millisOfDayNy(now)
        with self.subTest(msg="millisOfSecondNy"):
            junk = DateTimeUtils.millisOfSecondNy(now)

        # microsecond parts
        with self.subTest(msg="microsOfMilli"):
            junk = DateTimeUtils.microsOfMilli(now, tz)
        with self.subTest(msg="microsOfMilliNy"):
            junk = DateTimeUtils.microsOfMilliNy(now)

        # nanosecond parts
        with self.subTest(msg="nanosOfDay"):
            junk = DateTimeUtils.nanosOfDay(now, tz)
        with self.subTest(msg="nanosOfSecond"):
            junk = DateTimeUtils.nanosOfSecond(now, tz)
        with self.subTest(msg="nanosOfDayNy"):
            junk = DateTimeUtils.nanosOfDayNy(now)
        with self.subTest(msg="nanosOfSecondNy"):
            junk = DateTimeUtils.nanosOfSecondNy(now)

    def testArithmetic(self):
        """
        Coverage test of DateTime arithmetic helper methods
        """

        with self.subTest(msg="secondsToNanos"):
            junk = DateTimeUtils.secondsToNanos(1)
        with self.subTest(msg="millisToNanos"):
            junk = DateTimeUtils.millisToNanos(1)
        with self.subTest(msg="microsToNanos"):
            junk = DateTimeUtils.microsToNanos(1)
        with self.subTest(msg="nanosToMicros"):
            junk = DateTimeUtils.nanosToMicros(1000)
        with self.subTest(msg="nanosToMillis"):
            junk = DateTimeUtils.nanosToMillis(1000000)

        # get current time
        now = DateTimeUtils.currentTime()
        period = DateTimeUtils.convertPeriod("1D")  # returns a Period object
        # various plus/minus signatures
        with self.subTest(msg="plus(DateTime, long"):
            now2 = DateTimeUtils.plus(now, 6000000000)  # shift forward by an hour
        with self.subTest(msg="plus(DateTime, Period"):
            junk = DateTimeUtils.plus(now, period)  # shift forward by a day
        with self.subTest(msg="minus(DateTime, long"):
            junk = DateTimeUtils.minus(now, 6000000000)  # shift back by an hour
        with self.subTest(msg="minus(DateTime, DateTime"):
            junk = DateTimeUtils.minus(now2, now)
        with self.subTest(msg="minus(DateTime, Period)"):
            junk = DateTimeUtils.minus(now2, period)

        # other style date subtractions
        with self.subTest(msg="diffYear"):
            junk = DateTimeUtils.diffYear(now2, now)
        with self.subTest(msg="diffDay"):
            junk = DateTimeUtils.diffDay(now2, now)
        with self.subTest(msg="diffNanos"):
            junk = DateTimeUtils.diffNanos(now2, now)

        with self.subTest(msg="isAfter"):
            junk = DateTimeUtils.isAfter(now2, now)
        with self.subTest(msg="isBefore"):
            junk = DateTimeUtils.isBefore(now2, now)

        with self.subTest(msg="lowerBin"):
            junk = DateTimeUtils.lowerBin(now, 1000000000)
        with self.subTest(msg="upperBin"):
            junk = DateTimeUtils.upperBin(now, 1000000000)

    def testFormatting(self):
        """
        Test suite for date formatting
        """

        tz = jpy.get_type('io.deephaven.time.TimeZone').TZ_NY
        # get current time
        now = DateTimeUtils.currentTime()

        with self.subTest(msg="formatNy(DateTime)"):
            junk = DateTimeUtils.formatNy(now)  # Eastern timezone format
        with self.subTest(msg="format(DateTime, TimeZone)"):
            junk = DateTimeUtils.format(now, tz)
        # Unrelated method - formats long as period <days>T<hours>:....
        with self.subTest(msg="format(long)"):
            junk = DateTimeUtils.format(86400000000000 + 2*3600000000000 + 4*60000000000 + 8*1000000000 + 16)

        # Basically aliases of the above - why both?
        with self.subTest(msg="formatDate(DateTime, TimeZone)"):
            junk = DateTimeUtils.formatDate(now, tz)
        with self.subTest(msg="formatDateNy(DateTime)"):
            junk = DateTimeUtils.formatDateNy(now)

        formatter = None
        with self.subTest(msg="createFormatter"):
            formatter = DateTimeUtils.createFormatter("America/Denver")
        if formatter is not None:
            # get timestamp
            tsNow = time.time()
            tsSeconds = int(tsNow)
            tsMillis = 1000*tsSeconds
            tsMicros = 1000*tsMillis
            tsNanos = 1000*tsMicros
            with self.subTest(msg="getPartitionFromTimestampNanos"):
                junk = DateTimeUtils.getPartitionFromTimestampNanos(formatter, tsNanos)
            with self.subTest(msg="getPartitionFromTimestampMicros"):
                junk = DateTimeUtils.getPartitionFromTimestampMicros(formatter, tsMicros)
            with self.subTest(msg="getPartitionFromTimestampMillis"):
                junk = DateTimeUtils.getPartitionFromTimestampMillis(formatter, tsMillis)
            with self.subTest(msg="getPartitionFromTimestampSeconds"):
                junk = DateTimeUtils.getPartitionFromTimestampSeconds(formatter, tsSeconds)

    def testBusinessDay(self):
        """
        Test last business day functionality - why is this so limited?
        """

        with self.subTest(msg="lastBusinessDateNy()"):
            junk = DateTimeUtils.lastBusinessDateNy()

        tsMillis = 1000*int(time.time())
        with self.subTest(msg="lastBusinessDateNy(millis)"):
            junk = DateTimeUtils.lastBusinessDateNy(tsMillis)

        with self.subTest(msg="overrideLastBusinessDateNyFromCurrentDateNy"):
            DateTimeUtils.overrideLastBusinessDateNyFromCurrentDateNy()

    @unittest.skip("what to do?")
    def testDeprecated(self):
        """
        Test suite of deprecated methods?
        """

        # TODO: should I bother with these?
        # deephaven deprecated
        #   diff,dayDiff,yearDiff

        # deprecated joda call
        #   dateAtMidnight,millisToDateAtMidnight/Ny
        pass

