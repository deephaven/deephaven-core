#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#


##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import time

from deephaven import DateTimeUtil

if sys.version_info[0] < 3:
    int = long  # cheap python2/3 compliance - probably only required on a 32 bit system
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestDateTimeUtil(unittest.TestCase):
    """
    Test cases for the deephaven.DateTimeUtil module (performed locally) -
    """

    def testCurrent(self):
        """
        Test suite for getting DateTime object representing the current time
        """

        tz = jpy.get_type('io.deephaven.engine.time.TimeZone').TZ_NY
        with self.subTest(msg="currentTime"):
            junk = DateTimeUtil.currentTime()
        with self.subTest(msg="currentDate"):
            junk = DateTimeUtil.currentDate(tz)
        with self.subTest(msg="currentTimeNy"):
            junk = DateTimeUtil.currentDateNy()

    def testDatetimeFromTimestamp(self):
        """
        Test suite for converting timestamp to DateTime
        """

        sampleTimestamp = time.time()  # seconds of type double
        with self.subTest(msg="autoEpochToTime"):
            autoDate = DateTimeUtil.autoEpochToTime(int(sampleTimestamp))
        with self.subTest(msg="secondsToTime"):
            secondDate = DateTimeUtil.secondsToTime(int(sampleTimestamp))
            self.assertTrue(autoDate.equals(secondDate))
        with self.subTest(msg="millisToTime"):
            millisDate = DateTimeUtil.millisToTime(1000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(millisDate))
        with self.subTest(msg="nanosToTime"):
            microsDate = DateTimeUtil.microsToTime(1000000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(microsDate))
        with self.subTest(msg="nanosToTime"):
            nanosDate = DateTimeUtil.nanosToTime(1000000000*int(sampleTimestamp))
            # NB: underscore literals work in Python 3.5 and after
            self.assertTrue(autoDate.equals(nanosDate))

    def testConversionFromString(self):
        """
        Coverage tests for converting strings of various forms to DateTime, Period, or nanosecond time (period)
        """

        # TODO: Methods which almost certainly have no business being wrapped...
        #   convertExpression(string), expressionToNanos(string), getFinestDefinedUnit(string)

        with self.subTest(msg="convertDateTime"):
            junk = DateTimeUtil.convertDateTime("2018-11-13T15:00:00.000000 NY")
        with self.subTest(msg="convertDateTimeQuiet"):
            junk = DateTimeUtil.convertDateTimeQuiet("2018-11-13T15:00:00.000000 NY")

        # appear to operate on string formatted as <days>T<hh>:<mm>:<ss>.<fractional seconds up to nanos>
        # NB: the documentation sucks, and I'm just guessing
        with self.subTest(msg="convertTime"):
            junk = DateTimeUtil.convertTime("1T02:04:16.3264128")
        with self.subTest(msg="convertTimeQuiet"):
            junk = DateTimeUtil.convertTimeQuiet("1T02:04:16.3264128")

        # appear to operate on string formatted as <years>Y<months>M<weeks>W<days>D<hours>H<minutes>M<seconds>S
        #   all literals could be upper or lower case
        # NB: the documentation sucks, and I'm mostly guessing
        with self.subTest(msg="convertPeriod"):
            junk = DateTimeUtil.convertPeriod("1Y1M1W1DT1H1M1S")
        with self.subTest(msg="convertPeriodQuiet"):
            junk = DateTimeUtil.convertPeriodQuiet("1Y1M1W1DT1H1M1S")

    def testTimestampFromDatetime(self):
        """
        Coverage tests for converting DateTime object to timestamp
        """

        # get current time
        now = DateTimeUtil.currentTime()  # unit tested above...

        with self.subTest(msg="millis"):
            junk = DateTimeUtil.millis(now)
        with self.subTest(msg="nanos"):
            junk = DateTimeUtil.nanos(now)
        with self.subTest(msg="getExcelDateTime(DateTime)"):
            junk = DateTimeUtil.getExcelDateTime(now)

        tz = jpy.get_type('io.deephaven.engine.time.TimeZone').TZ_NY
        with self.subTest(msg="getExcelDateTime(DateTime, DbTimeZone)"):
            junk = DateTimeUtil.getExcelDateTime(now, tz)
        with self.subTest(msg="getExcelDateTime(DateTime, TimeZone)"):
            # NB: converting from DbTimeZone to TimeZone - should work...
            # should we even test this?
            junk = DateTimeUtil.getExcelDateTime(now, tz.getTimeZone().toTimeZone())

    def testZonedMethods(self):
        """
        Test suite for DateTime <-> ZonedDateTime - not clear this is ever to be used
        """

        # get current time
        now = DateTimeUtil.currentTime()  # unit tested above...
        with self.subTest(msg="getZonedDateTime(DateTime)"):
            junk = DateTimeUtil.getZonedDateTime(now)  # will have TZ_DEFAULT applied

        tz = jpy.get_type('io.deephaven.engine.time.TimeZone').TZ_NY
        with self.subTest(msg="getZonedDateTime(DateTime, DbTimeZone)"):
            zoned = DateTimeUtil.getZonedDateTime(now, tz)
        with self.subTest(msg="toDateTime(ZonedDateTime)"):
            junk = DateTimeUtil.toDateTime(zoned)

    def testDateParts(self):
        """
        Coverage test suite for various date part extractions
        """

        tz = jpy.get_type('io.deephaven.engine.time.TimeZone').TZ_NY
        # get current time
        now = DateTimeUtil.currentTime()

        # year parts
        with self.subTest(msg="year"):
            junk = DateTimeUtil.year(now, tz)
        with self.subTest(msg="yearOfCentury"):
            junk = DateTimeUtil.yearOfCentury(now, tz)
        with self.subTest(msg="yearNy"):
            junk = DateTimeUtil.yearNy(now)
        with self.subTest(msg="yearOfCenturyNy"):
            junk = DateTimeUtil.yearOfCenturyNy(now)

        # month parts
        with self.subTest(msg="monthOfYear"):
            junk = DateTimeUtil.monthOfYear(now, tz)
        with self.subTest(msg="monthOfYearNy"):
            junk = DateTimeUtil.monthOfYearNy(now)

        # day parts
        with self.subTest(msg="dayOfYear"):
            junk = DateTimeUtil.dayOfYear(now, tz)
        with self.subTest(msg="dayOfMonth"):
            junk = DateTimeUtil.dayOfMonth(now, tz)
        with self.subTest(msg="dayOfWeek"):
            junk = DateTimeUtil.dayOfWeek(now, tz)
        with self.subTest(msg="dayOfYearNy"):
            junk = DateTimeUtil.dayOfYearNy(now)
        with self.subTest(msg="dayOfMonthNy"):
            junk = DateTimeUtil.dayOfMonthNy(now)
        with self.subTest(msg="dayOfWeekNy"):
            junk = DateTimeUtil.dayOfWeekNy(now)

        # hour parts
        with self.subTest(msg="hourOfDay"):
            junk = DateTimeUtil.hourOfDay(now, tz)
        with self.subTest(msg="hourOfDayNy"):
            junk = DateTimeUtil.hourOfDayNy(now)

        # minute parts
        with self.subTest(msg="minuteOfDay"):
            junk = DateTimeUtil.minuteOfDay(now, tz)
        with self.subTest(msg="minuteOfHour"):
            junk = DateTimeUtil.minuteOfHour(now, tz)
        with self.subTest(msg="minuteOfDayNy"):
            junk = DateTimeUtil.minuteOfDayNy(now)
        with self.subTest(msg="minuteOfHourNy"):
            junk = DateTimeUtil.minuteOfHourNy(now)

        # second parts
        with self.subTest(msg="secondOfDay"):
            junk = DateTimeUtil.secondOfDay(now, tz)
        with self.subTest(msg="secondOfMinute"):
            junk = DateTimeUtil.secondOfMinute(now, tz)
        with self.subTest(msg="secondOfDayNy"):
            junk = DateTimeUtil.secondOfDayNy(now)
        with self.subTest(msg="secondOfMinuteNy"):
            junk = DateTimeUtil.secondOfMinuteNy(now)

        # millisecond parts
        with self.subTest(msg="millisOfDay"):
            junk = DateTimeUtil.millisOfDay(now, tz)
        with self.subTest(msg="millisOfSecond"):
            junk = DateTimeUtil.millisOfSecond(now, tz)
        with self.subTest(msg="millisOfDayNy"):
            junk = DateTimeUtil.millisOfDayNy(now)
        with self.subTest(msg="millisOfSecondNy"):
            junk = DateTimeUtil.millisOfSecondNy(now)

        # microsecond parts
        with self.subTest(msg="microsOfMilli"):
            junk = DateTimeUtil.microsOfMilli(now, tz)
        with self.subTest(msg="microsOfMilliNy"):
            junk = DateTimeUtil.microsOfMilliNy(now)

        # nanosecond parts
        with self.subTest(msg="nanosOfDay"):
            junk = DateTimeUtil.nanosOfDay(now, tz)
        with self.subTest(msg="nanosOfSecond"):
            junk = DateTimeUtil.nanosOfSecond(now, tz)
        with self.subTest(msg="nanosOfDayNy"):
            junk = DateTimeUtil.nanosOfDayNy(now)
        with self.subTest(msg="nanosOfSecondNy"):
            junk = DateTimeUtil.nanosOfSecondNy(now)

    def testArithmetic(self):
        """
        Coverage test of DateTime arithmetic helper methods
        """

        with self.subTest(msg="secondsToNanos"):
            junk = DateTimeUtil.secondsToNanos(1)
        with self.subTest(msg="millisToNanos"):
            junk = DateTimeUtil.millisToNanos(1)
        with self.subTest(msg="microsToNanos"):
            junk = DateTimeUtil.microsToNanos(1)
        with self.subTest(msg="nanosToMicros"):
            junk = DateTimeUtil.nanosToMicros(1000)
        with self.subTest(msg="nanosToMillis"):
            junk = DateTimeUtil.nanosToMillis(1000000)

        # get current time
        now = DateTimeUtil.currentTime()
        period = DateTimeUtil.convertPeriod("1D")  # returns a Period object
        # various plus/minus signatures
        with self.subTest(msg="plus(DateTime, long"):
            now2 = DateTimeUtil.plus(now, 6000000000)  # shift forward by an hour
        with self.subTest(msg="plus(DateTime, Period"):
            junk = DateTimeUtil.plus(now, period)  # shift forward by a day
        with self.subTest(msg="minus(DateTime, long"):
            junk = DateTimeUtil.minus(now, 6000000000)  # shift back by an hour
        with self.subTest(msg="minus(DateTime, DateTime"):
            junk = DateTimeUtil.minus(now2, now)
        with self.subTest(msg="minus(DateTime, Period)"):
            junk = DateTimeUtil.minus(now2, period)

        # other style date subtractions
        with self.subTest(msg="diffYear"):
            junk = DateTimeUtil.diffYear(now2, now)
        with self.subTest(msg="diffDay"):
            junk = DateTimeUtil.diffDay(now2, now)
        with self.subTest(msg="diffNanos"):
            junk = DateTimeUtil.diffNanos(now2, now)

        with self.subTest(msg="isAfter"):
            junk = DateTimeUtil.isAfter(now2, now)
        with self.subTest(msg="isBefore"):
            junk = DateTimeUtil.isBefore(now2, now)

        with self.subTest(msg="lowerBin"):
            junk = DateTimeUtil.lowerBin(now, 1000000000)
        with self.subTest(msg="upperBin"):
            junk = DateTimeUtil.upperBin(now, 1000000000)

    def testFormatting(self):
        """
        Test suite for date formatting
        """

        tz = jpy.get_type('io.deephaven.engine.time.TimeZone').TZ_NY
        # get current time
        now = DateTimeUtil.currentTime()

        with self.subTest(msg="formatNy(DateTime)"):
            junk = DateTimeUtil.formatNy(now)  # Eastern timezone format
        with self.subTest(msg="format(DateTime, DbTimeZone)"):
            junk = DateTimeUtil.format(now, tz)
        # Unrelated method - formats long as period <days>T<hours>:....
        with self.subTest(msg="format(long)"):
            junk = DateTimeUtil.format(86400000000000 + 2*3600000000000 + 4*60000000000 + 8*1000000000 + 16)

        # Basically aliases of the above - why both?
        with self.subTest(msg="formatDate(DateTime, DbTimeZone)"):
            junk = DateTimeUtil.formatDate(now, tz)
        with self.subTest(msg="formatDateNy(DateTime)"):
            junk = DateTimeUtil.formatDateNy(now)

        formatter = None
        with self.subTest(msg="createFormatter"):
            formatter = DateTimeUtil.createFormatter("America/Denver")
        if formatter is not None:
            # get timestamp
            tsNow = time.time()
            tsSeconds = int(tsNow)
            tsMillis = 1000*tsSeconds
            tsMicros = 1000*tsMillis
            tsNanos = 1000*tsMicros
            with self.subTest(msg="getPartitionFromTimestampNanos"):
                junk = DateTimeUtil.getPartitionFromTimestampNanos(formatter, tsNanos)
            with self.subTest(msg="getPartitionFromTimestampMicros"):
                junk = DateTimeUtil.getPartitionFromTimestampMicros(formatter, tsMicros)
            with self.subTest(msg="getPartitionFromTimestampMillis"):
                junk = DateTimeUtil.getPartitionFromTimestampMillis(formatter, tsMillis)
            with self.subTest(msg="getPartitionFromTimestampSeconds"):
                junk = DateTimeUtil.getPartitionFromTimestampSeconds(formatter, tsSeconds)

    def testBusinessDay(self):
        """
        Test last business day functionality - why is this so limited?
        """

        with self.subTest(msg="lastBusinessDateNy()"):
            junk = DateTimeUtil.lastBusinessDateNy()

        tsMillis = 1000*int(time.time())
        with self.subTest(msg="lastBusinessDateNy(millis)"):
            junk = DateTimeUtil.lastBusinessDateNy(tsMillis)

        with self.subTest(msg="overrideLastBusinessDateNyFromCurrentDateNy"):
            DateTimeUtil.overrideLastBusinessDateNyFromCurrentDateNy()

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

