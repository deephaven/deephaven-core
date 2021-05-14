#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#


##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy
import time

from deephaven import DBTimeUtils

if sys.version_info[0] < 3:
    int = long  # cheap python2/3 compliance - probably only required on a 32 bit system
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestDBTimeUtils(unittest.TestCase):
    """
    Test cases for the deephaven.DBTimeUtils module (performed locally) -
    """

    def testCurrent(self):
        """
        Test suite for getting DBDateTime object representing the current time
        """

        tz = jpy.get_type('io.deephaven.db.tables.utils.DBTimeZone').TZ_NY
        with self.subTest(msg="currentTime"):
            junk = DBTimeUtils.currentTime()
        with self.subTest(msg="currentDate"):
            junk = DBTimeUtils.currentDate(tz)
        with self.subTest(msg="currentTimeNy"):
            junk = DBTimeUtils.currentDateNy()

    def testDatetimeFromTimestamp(self):
        """
        Test suite for converting timestamp to DBDateTime
        """

        sampleTimestamp = time.time()  # seconds of type double
        with self.subTest(msg="autoEpochToTime"):
            autoDate = DBTimeUtils.autoEpochToTime(int(sampleTimestamp))
        with self.subTest(msg="secondsToTime"):
            secondDate = DBTimeUtils.secondsToTime(int(sampleTimestamp))
            self.assertTrue(autoDate.equals(secondDate))
        with self.subTest(msg="millisToTime"):
            millisDate = DBTimeUtils.millisToTime(1000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(millisDate))
        with self.subTest(msg="nanosToTime"):
            microsDate = DBTimeUtils.microsToTime(1000000*int(sampleTimestamp))
            self.assertTrue(autoDate.equals(microsDate))
        with self.subTest(msg="nanosToTime"):
            nanosDate = DBTimeUtils.nanosToTime(1000000000*int(sampleTimestamp))
            # NB: underscore literals work in Python 3.5 and after
            self.assertTrue(autoDate.equals(nanosDate))

    def testConversionFromString(self):
        """
        Coverage tests for converting strings of various forms to DbDateTime, DbPeriod, or nanosecond time (period)
        """

        # TODO: Methods which almost certainly have no business being wrapped...
        #   convertExpression(string), expressionToNanos(string), getFinestDefinedUnit(string)

        with self.subTest(msg="convertDateTime"):
            junk = DBTimeUtils.convertDateTime("2018-11-13T15:00:00.000000 NY")
        with self.subTest(msg="convertDateTimeQuiet"):
            junk = DBTimeUtils.convertDateTimeQuiet("2018-11-13T15:00:00.000000 NY")

        # appear to operate on string formatted as <days>T<hh>:<mm>:<ss>.<fractional seconds up to nanos>
        # NB: the documentation sucks, and I'm just guessing
        with self.subTest(msg="convertTime"):
            junk = DBTimeUtils.convertTime("1T02:04:16.3264128")
        with self.subTest(msg="convertTimeQuiet"):
            junk = DBTimeUtils.convertTimeQuiet("1T02:04:16.3264128")

        # appear to operate on string formatted as <years>Y<months>M<weeks>W<days>D<hours>H<minutes>M<seconds>S
        #   all literals could be upper or lower case
        # NB: the documentation sucks, and I'm mostly guessing
        with self.subTest(msg="convertPeriod"):
            junk = DBTimeUtils.convertPeriod("1Y1M1W1DT1H1M1S")
        with self.subTest(msg="convertPeriodQuiet"):
            junk = DBTimeUtils.convertPeriodQuiet("1Y1M1W1DT1H1M1S")

    def testTimestampFromDatetime(self):
        """
        Coverage tests for converting DbDateTime object to timestamp
        """

        # get current time
        now = DBTimeUtils.currentTime()  # unit tested above...

        with self.subTest(msg="millis"):
            junk = DBTimeUtils.millis(now)
        with self.subTest(msg="nanos"):
            junk = DBTimeUtils.nanos(now)
        with self.subTest(msg="getExcelDateTime(DbDateTime)"):
            junk = DBTimeUtils.getExcelDateTime(now)

        tz = jpy.get_type('io.deephaven.db.tables.utils.DBTimeZone').TZ_NY
        with self.subTest(msg="getExcelDateTime(DbDateTime, DbTimeZone)"):
            junk = DBTimeUtils.getExcelDateTime(now, tz)
        with self.subTest(msg="getExcelDateTime(DbDateTime, TimeZone)"):
            # NB: converting from DbTimeZone to TimeZone - should work...
            # should we even test this?
            junk = DBTimeUtils.getExcelDateTime(now, tz.getTimeZone().toTimeZone())

    def testZonedMethods(self):
        """
        Test suite for DbDateTime <-> ZonedDateTime - not clear this is ever to be used
        """

        # get current time
        now = DBTimeUtils.currentTime()  # unit tested above...
        with self.subTest(msg="getZonedDateTime(DbDateTime)"):
            junk = DBTimeUtils.getZonedDateTime(now)  # will have TZ_DEFAULT applied

        tz = jpy.get_type('io.deephaven.db.tables.utils.DBTimeZone').TZ_NY
        with self.subTest(msg="getZonedDateTime(DbDateTime, DbTimeZone)"):
            zoned = DBTimeUtils.getZonedDateTime(now, tz)
        with self.subTest(msg="toDateTime(ZonedDateTime)"):
            junk = DBTimeUtils.toDateTime(zoned)

    def testDateParts(self):
        """
        Coverage test suite for various date part extractions
        """

        tz = jpy.get_type('io.deephaven.db.tables.utils.DBTimeZone').TZ_NY
        # get current time
        now = DBTimeUtils.currentTime()

        # year parts
        with self.subTest(msg="year"):
            junk = DBTimeUtils.year(now, tz)
        with self.subTest(msg="yearOfCentury"):
            junk = DBTimeUtils.yearOfCentury(now, tz)
        with self.subTest(msg="yearNy"):
            junk = DBTimeUtils.yearNy(now)
        with self.subTest(msg="yearOfCenturyNy"):
            junk = DBTimeUtils.yearOfCenturyNy(now)

        # month parts
        with self.subTest(msg="monthOfYear"):
            junk = DBTimeUtils.monthOfYear(now, tz)
        with self.subTest(msg="monthOfYearNy"):
            junk = DBTimeUtils.monthOfYearNy(now)

        # day parts
        with self.subTest(msg="dayOfYear"):
            junk = DBTimeUtils.dayOfYear(now, tz)
        with self.subTest(msg="dayOfMonth"):
            junk = DBTimeUtils.dayOfMonth(now, tz)
        with self.subTest(msg="dayOfWeek"):
            junk = DBTimeUtils.dayOfWeek(now, tz)
        with self.subTest(msg="dayOfYearNy"):
            junk = DBTimeUtils.dayOfYearNy(now)
        with self.subTest(msg="dayOfMonthNy"):
            junk = DBTimeUtils.dayOfMonthNy(now)
        with self.subTest(msg="dayOfWeekNy"):
            junk = DBTimeUtils.dayOfWeekNy(now)

        # hour parts
        with self.subTest(msg="hourOfDay"):
            junk = DBTimeUtils.hourOfDay(now, tz)
        with self.subTest(msg="hourOfDayNy"):
            junk = DBTimeUtils.hourOfDayNy(now)

        # minute parts
        with self.subTest(msg="minuteOfDay"):
            junk = DBTimeUtils.minuteOfDay(now, tz)
        with self.subTest(msg="minuteOfHour"):
            junk = DBTimeUtils.minuteOfHour(now, tz)
        with self.subTest(msg="minuteOfDayNy"):
            junk = DBTimeUtils.minuteOfDayNy(now)
        with self.subTest(msg="minuteOfHourNy"):
            junk = DBTimeUtils.minuteOfHourNy(now)

        # second parts
        with self.subTest(msg="secondOfDay"):
            junk = DBTimeUtils.secondOfDay(now, tz)
        with self.subTest(msg="secondOfMinute"):
            junk = DBTimeUtils.secondOfMinute(now, tz)
        with self.subTest(msg="secondOfDayNy"):
            junk = DBTimeUtils.secondOfDayNy(now)
        with self.subTest(msg="secondOfMinuteNy"):
            junk = DBTimeUtils.secondOfMinuteNy(now)

        # millisecond parts
        with self.subTest(msg="millisOfDay"):
            junk = DBTimeUtils.millisOfDay(now, tz)
        with self.subTest(msg="millisOfSecond"):
            junk = DBTimeUtils.millisOfSecond(now, tz)
        with self.subTest(msg="millisOfDayNy"):
            junk = DBTimeUtils.millisOfDayNy(now)
        with self.subTest(msg="millisOfSecondNy"):
            junk = DBTimeUtils.millisOfSecondNy(now)

        # microsecond parts
        with self.subTest(msg="microsOfMilli"):
            junk = DBTimeUtils.microsOfMilli(now, tz)
        with self.subTest(msg="microsOfMilliNy"):
            junk = DBTimeUtils.microsOfMilliNy(now)

        # nanosecond parts
        with self.subTest(msg="nanosOfDay"):
            junk = DBTimeUtils.nanosOfDay(now, tz)
        with self.subTest(msg="nanosOfSecond"):
            junk = DBTimeUtils.nanosOfSecond(now, tz)
        with self.subTest(msg="nanosOfDayNy"):
            junk = DBTimeUtils.nanosOfDayNy(now)
        with self.subTest(msg="nanosOfSecondNy"):
            junk = DBTimeUtils.nanosOfSecondNy(now)

    def testArithmetic(self):
        """
        Coverage test of DbDateTime arithmetic helper methods
        """

        with self.subTest(msg="secondsToNanos"):
            junk = DBTimeUtils.secondsToNanos(1)
        with self.subTest(msg="millisToNanos"):
            junk = DBTimeUtils.millisToNanos(1)
        with self.subTest(msg="microsToNanos"):
            junk = DBTimeUtils.microsToNanos(1)
        with self.subTest(msg="nanosToMicros"):
            junk = DBTimeUtils.nanosToMicros(1000)
        with self.subTest(msg="nanosToMillis"):
            junk = DBTimeUtils.nanosToMillis(1000000)

        # get current time
        now = DBTimeUtils.currentTime()
        period = DBTimeUtils.convertPeriod("1D")  # returns a DbPeriod object
        # various plus/minus signatures
        with self.subTest(msg="plus(DbDateTime, long"):
            now2 = DBTimeUtils.plus(now, 6000000000)  # shift forward by an hour
        with self.subTest(msg="plus(DbDateTime, DbPeriod"):
            junk = DBTimeUtils.plus(now, period)  # shift forward by a day
        with self.subTest(msg="minus(DbDateTime, long"):
            junk = DBTimeUtils.minus(now, 6000000000)  # shift back by an hour
        with self.subTest(msg="minus(DbDateTime, DbDateTime"):
            junk = DBTimeUtils.minus(now2, now)
        with self.subTest(msg="minus(DbDateTime, DbPeriod)"):
            junk = DBTimeUtils.minus(now2, period)

        # other style date subtractions
        with self.subTest(msg="diffYear"):
            junk = DBTimeUtils.diffYear(now2, now)
        with self.subTest(msg="diffDay"):
            junk = DBTimeUtils.diffDay(now2, now)
        with self.subTest(msg="diffNanos"):
            junk = DBTimeUtils.diffNanos(now2, now)

        with self.subTest(msg="isAfter"):
            junk = DBTimeUtils.isAfter(now2, now)
        with self.subTest(msg="isBefore"):
            junk = DBTimeUtils.isBefore(now2, now)

        with self.subTest(msg="lowerBin"):
            junk = DBTimeUtils.lowerBin(now, 1000000000)
        with self.subTest(msg="upperBin"):
            junk = DBTimeUtils.upperBin(now, 1000000000)

    def testFormatting(self):
        """
        Test suite for date formatting
        """

        tz = jpy.get_type('io.deephaven.db.tables.utils.DBTimeZone').TZ_NY
        # get current time
        now = DBTimeUtils.currentTime()

        with self.subTest(msg="formatNy(DbDateTime)"):
            junk = DBTimeUtils.formatNy(now)  # Eastern timezone format
        with self.subTest(msg="format(DbDateTime, DbTimeZone)"):
            junk = DBTimeUtils.format(now, tz)
        # Unrelated method - formats long as period <days>T<hours>:....
        with self.subTest(msg="format(long)"):
            junk = DBTimeUtils.format(86400000000000 + 2*3600000000000 + 4*60000000000 + 8*1000000000 + 16)

        # Basically aliases of the above - why both?
        with self.subTest(msg="formatDate(DbDateTime, DbTimeZone)"):
            junk = DBTimeUtils.formatDate(now, tz)
        with self.subTest(msg="formatDateNy(DbDateTime)"):
            junk = DBTimeUtils.formatDateNy(now)

        formatter = None
        with self.subTest(msg="createFormatter"):
            formatter = DBTimeUtils.createFormatter("America/Denver")
        if formatter is not None:
            # get timestamp
            tsNow = time.time()
            tsSeconds = int(tsNow)
            tsMillis = 1000*tsSeconds
            tsMicros = 1000*tsMillis
            tsNanos = 1000*tsMicros
            with self.subTest(msg="getPartitionFromTimestampNanos"):
                junk = DBTimeUtils.getPartitionFromTimestampNanos(formatter, tsNanos)
            with self.subTest(msg="getPartitionFromTimestampMicros"):
                junk = DBTimeUtils.getPartitionFromTimestampMicros(formatter, tsMicros)
            with self.subTest(msg="getPartitionFromTimestampMillis"):
                junk = DBTimeUtils.getPartitionFromTimestampMillis(formatter, tsMillis)
            with self.subTest(msg="getPartitionFromTimestampSeconds"):
                junk = DBTimeUtils.getPartitionFromTimestampSeconds(formatter, tsSeconds)

    def testBusinessDay(self):
        """
        Test last business day functionality - why is this so limited?
        """

        with self.subTest(msg="lastBusinessDateNy()"):
            junk = DBTimeUtils.lastBusinessDateNy()

        tsMillis = 1000*int(time.time())
        with self.subTest(msg="lastBusinessDateNy(millis)"):
            junk = DBTimeUtils.lastBusinessDateNy(tsMillis)

        with self.subTest(msg="overrideLastBusinessDateNyFromCurrentDateNy"):
            DBTimeUtils.overrideLastBusinessDateNyFromCurrentDateNy()

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

