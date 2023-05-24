#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
from time import sleep
from datetime import datetime

from deephaven.constants import NULL_LONG, NULL_INT
from deephaven.time import *
from tests.testbase import BaseTestCase


class TimeTestCase(BaseTestCase):

    # region Constants

    def test_constants(self):
        self.assertEqual(1000,MICRO)
        self.assertEqual(1000000,MILLI)
        self.assertEqual(1000000000,SECOND)
        self.assertEqual(60*1000000000,MINUTE)
        self.assertEqual(60*60*1000000000,HOUR)
        self.assertEqual(24*60*60*1000000000,DAY)
        self.assertEqual(7*24*60*60*1000000000,WEEK)
        self.assertEqual(365*24*60*60*1000000000,YEAR)

        self.assertEqual(1/SECOND, SECONDS_PER_NANO)
        self.assertEqual(1/MINUTE, MINUTES_PER_NANO)
        self.assertEqual(1/HOUR, HOURS_PER_NANO)
        self.assertEqual(1/DAY, DAYS_PER_NANO)
        self.assertEqual(1/YEAR, YEARS_PER_NANO)

    #TODO:  DateStyle

    # endregion

    # region: CLock

    def test_now(self):
        for system in [True, False]:
            for resolution in ['ns', 'ms']:
                dt = now(system=system, resolution=resolution)
                sleep(1)
                dt1 = now(system=system, resolution=resolution)
                self.assertGreaterEqual(diff_nanos(dt, dt1), 100000000)

    def test_today(self):
        tz = time_zone("America/New_york")
        td = today(tz)
        target = datetime.today().strftime('%Y-%m-%d')
        self.assertEqual(td, target)

    # endregion
    
    # region: Time Zone

    def test_time_zone:
        tz = time_zone("America/New_York")
        self.assertEqual(str(tz), "America/New_York")

        tz = time_zone("MN")
        self.assertEqual(str(tz), "America/Chicago")

        tz = time_zone(None)
        self.assertEqual(str(tz), "America/New_York")

    # endregion
    
    # region: Conversions: Time Units

    def test_micros_to_nanos(self):
        t = 123456789
        self.assertEqual(t // 10 ** 3, micros_to_nanos(t))
        self.assertEqual(NULL_LONG, micros_to_nanos(NULL_LONG))

    def test_millis_to_nanos(self):
        t = 123456789
        self.assertEqual(t // 10 ** 6, millis_to_nanos(t))
        self.assertEqual(NULL_LONG, millis_to_nanos(NULL_LONG))

    def test_seconds_to_nanos(self):
        t = 123456789
        self.assertEqual(t // 10 ** 9, seconds_to_nanos(t))
        self.assertEqual(NULL_LONG, seconds_to_nanos(NULL_LONG))

    def test_nanos_to_micros(self):
        t = 123456789
        self.assertEqual(t // 10 ** 3, nanos_to_micros(t))
        self.assertEqual(NULL_LONG, nanos_to_micros(NULL_LONG))

    def test_millis_to_micros(self):
        t = 123456789
        self.assertEqual(t * 10 ** 3, millis_to_micros(t))
        self.assertEqual(NULL_LONG, millis_to_micros(NULL_LONG))

    def test_seconds_to_micros(self):
        t = 123456789
        self.assertEqual(t * 10 ** 6, seconds_to_micros(t))
        self.assertEqual(NULL_LONG, seconds_to_micros(NULL_LONG))

    def test_nanos_to_millis(self):
        t = 123456789
        self.assertEqual(t // 10 ** 6, nanos_to_millis(t))
        self.assertEqual(NULL_LONG, nanos_to_millis(NULL_LONG))

    def test_micros_to_millis(self):
        t = 123456789
        self.assertEqual(t // 10 ** 3, micros_to_millis(t))
        self.assertEqual(NULL_LONG, micros_to_millis(NULL_LONG))

    def test_seconds_to_millis(self):
        t = 123456789
        self.assertEqual(t * 10 ** 3, seconds_to_millis(t))
        self.assertEqual(NULL_LONG, seconds_to_millis(NULL_LONG))

    def test_nanos_to_seconds(self):
        t = 123456789
        self.assertEqual(t // 10 ** 9, nanos_to_seconds(t))
        self.assertEqual(NULL_LONG, nanos_to_seconds(NULL_LONG))

    def test_micros_to_seconds(self):
        t = 123456789
        self.assertEqual(t // 10 ** 6, micros_to_seconds(t))
        self.assertEqual(NULL_LONG, micros_to_seconds(NULL_LONG))

    def test_millis_to_seconds(self):
        t = 123456789
        self.assertEqual(t // 10 ** 3, millis_to_seconds(t))
        self.assertEqual(NULL_LONG, millis_to_seconds(NULL_LONG))


    # endregion
    
    # region: Conversions: Date Time Types

    #TODO:  to_instant
    #TODO:  to_zdt
    #TODO:  make_instant
    #TODO:  make_zdt
    #TODO:  to_local_date
    #TODO:  to_local_time

    # endregion
    
    # region: Conversions: Epoch

    def test_epoch_nanos(self):
        dt = parse_instant("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual(1639171277303*10**6 + 123456789, epoch_nanos(dt))
        self.assertEqual(NULL_LONG, epoch_nanos(None))

    def test_epoch_micros(self):
        dt = parse_instant("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual((1639171277303*10**6 + 123456789) // 10**3, epoch_micros(dt))
        self.assertEqual(NULL_LONG, epoch_micros(None))

    def test_epoch_millis(self):
        dt = parse_instant("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual((1639171277303*10**6 + 123456789) // 10**6, epoch_millis(dt))
        self.assertEqual(NULL_LONG, epoch_millis(None))

    def test_epoch_seconds(self):
        dt = parse_instant("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual((1639171277303*10**6 + 123456789) // 10**9, epoch_seconds(dt))
        self.assertEqual(NULL_LONG, epoch_seconds(None))

    def test_epoch_nanos_to_instant(self):
        nanos = 1639171277303*10**6 + 123456789
        dt1 = epoch_nanos_to_instant(nanos)
        dt2 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_nanos_to_instant(NULL_LONG))

    def test_epoch_micros_to_instant(self):
        nanos = 1639171277303*10**6 + 123456789
        micros = nanos // 10**3
        dt1 = epoch_micros_to_instant(micros)
        dt2 = parse_instant("2021-12-10T14:21:17.123456 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_micros_to_instant(NULL_LONG))

    def test_epoch_millis_to_instant(self):
        nanos = 1639171277303*10**6 + 123456789
        millis = nanos // 10**6
        dt1 = epoch_millis_to_instant(millis)
        dt2 = parse_instant("2021-12-10T14:21:17.123 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_millis_to_instant(NULL_LONG))

    def test_epoch_seconds_to_instant(self):
        nanos = 1639171277303*10**6 + 123456789
        seconds = nanos // 10**9
        dt1 = epoch_seconds_to_instant(seconds)
        dt2 = parse_instant("2021-12-10T14:21:17.123 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_seconds_to_instant(NULL_LONG))

    def test_epoch_nanos_to_zdt(self):
        nanos = 1639171277303*10**6 + 123456789
        dt1 = epoch_nanos_to_zdt(nanos)
        dt2 = parse_zdt("2021-12-10T14:21:17.123456789 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_nanos_to_zdt(NULL_LONG))

    def test_epoch_micros_to_zdt(self):
        nanos = 1639171277303*10**6 + 123456789
        micros = nanos // 10**3
        dt1 = epoch_micros_to_zdt(micros)
        dt2 = parse_zdt("2021-12-10T14:21:17.123456 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_micros_to_zdt(NULL_LONG))

    def test_epoch_millis_to_zdt(self):
        nanos = 1639171277303*10**6 + 123456789
        millis = nanos // 10**6
        dt1 = epoch_millis_to_zdt(millis)
        dt2 = parse_zdt("2021-12-10T14:21:17.123 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_millis_to_zdt(NULL_LONG))

    def test_epoch_seconds_to_zdt(self):
        nanos = 1639171277303*10**6 + 123456789
        seconds = nanos // 10**9
        dt1 = epoch_seconds_to_zdt(seconds)
        dt2 = parse_zdt("2021-12-10T14:21:17.123 NY")
        self.assertEqual(dt2, dt1)
        self.assertNone(epoch_seconds_to_zdt(NULL_LONG))

    def test_epoch_auto_to_epoch_nanos(self):
        nanos = 1639171277303*10**6 + 123456789
        micros = nanos // 10**3
        millis = nanos // 10**6
        seconds = nanos // 10**9
        
        self.assertEqual(nanos,epoch_auto_to_epoch_nanos(nanos))
        self.assertEqual(micros * 10**3,epoch_auto_to_epoch_nanos(micros))
        self.assertEqual(milis * 10**6,epoch_auto_to_epoch_nanos(millis))
        self.assertEqual(seconds * 10**9,epoch_auto_to_epoch_nanos(seconds))
        self.asserEqual(NULL_LONG, epoch_auto_to_epoch_nanos(NULL_LONG))

    def test_epoch_auto_to_instant(self):
        nanos = 1639171277303 * 10 ** 6 + 123456789
        micros = nanos // 10 ** 3
        millis = nanos // 10 ** 6
        seconds = nanos // 10 ** 9

        self.assertEqual(epoch_nanos_to_instant(nanos), epoch_auto_to_instant(nanos))
        self.assertEqual(epoch_nanos_to_instant(micros * 10 ** 3), epoch_auto_to_instant(micros))
        self.assertEqual(epoch_nanos_to_instant(milis * 10 ** 6), epoch_auto_to_instant(millis))
        self.assertEqual(epoch_nanos_to_instant(seconds * 10 ** 9), epoch_auto_to_instant(seconds))
        self.asserNone(epoch_auto_to_instant(NULL_LONG))


    def test_epoch_auto_to_zdt(self):
        nanos = 1639171277303 * 10 ** 6 + 123456789
        micros = nanos // 10 ** 3
        millis = nanos // 10 ** 6
        seconds = nanos // 10 ** 9

        self.assertEqual(epoch_nanos_to_zdt(nanos), epoch_auto_to_zdt(nanos))
        self.assertEqual(epoch_nanos_to_zdt(micros * 10 ** 3), epoch_auto_to_zdt(micros))
        self.assertEqual(epoch_nanos_to_zdt(milis * 10 ** 6), epoch_auto_to_zdt(millis))
        self.assertEqual(epoch_nanos_to_zdt(seconds * 10 ** 9), epoch_auto_to_zdt(seconds))
        self.asserNone(epoch_auto_to_zdt(NULL_LONG))

    # endregion
    
    # region: Conversions: Excel

    #TODO:  to_excel_time
    #TODO:  excel_to_instant
    #TODO:  excel_to_zdt

    # endregion
    
    # region: Arithmetic

    #TODO:  OK with plus_period name???
    def test_plus_period(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")

        dt2 = parse_instant("2021-12-12T14:21:17.123456800 NY")
        dt3 = plus_period(dt1, 11)
        self.assertEqual(dt2, dt3)

        dt2 = parse_instant("2021-12-12T16:21:17.123456789 NY")
        duration_str = "PT2H"
        duration = parse_duration(duration_str)
        dt3 = plus_period(dt1, duration)
        self.assertEqual(dt2, dt3)

        dt2 = parse_instant("2021-12-12T14:21:17.123456789 NY")
        period_str = "P2D"
        period = parse_period(period_str)
        dt3 = plus_period(dt1, period)
        self.assertEqual(dt2, dt3)


    #TODO:  OK with minus_period name???
    def test_minus_period(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")

        dt2 = parse_instant("2021-12-12T14:21:17.123456778 NY")
        dt3 = minus_period(dt1, 11)
        self.assertEqual(dt2, dt3)

        dt2 = parse_instant("2021-12-08T12:21:17.123456789 NY")
        duration_str = "PT2H"
        duration = parse_duration(duration_str)
        dt3 = minus_period(dt1, duration)
        self.assertEqual(dt2, dt3)

        dt2 = parse_instant("2021-12-08T14:21:17.123456789 NY")
        period_str = "P2D"
        period = parse_period(period_str)
        dt3 = minus_period(dt1, period)
        self.assertEqual(dt2, dt3)

    def test_diff_nanos(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-12T14:21:17.123456800 NY")
        self.assertEqual(11, diff_nanos(dt1, dt2))
        self.assertEqual(-11, diff_nanos(dt2, dt1))
        self.assertEqual(NULL_LONG, diff_nanos(None, dt2))
        self.assertEqual(NULL_LONG, diff_nanos(dt1, None))

    def test_diff_micros(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-12T14:21:17.123 NY")
        self.assertEqual(456, diff_micros(dt1, dt2))
        self.assertEqual(-456, diff_micros(dt2, dt1))
        self.assertEqual(NULL_LONG, diff_micros(None, dt2))
        self.assertEqual(NULL_LONG, diff_micros(dt1, None))

    def test_diff_millis(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-12T14:21:17 NY")
        self.assertEqual(123, diff_millis(dt1, dt2))
        self.assertEqual(-123, diff_millis(dt2, dt1))
        self.assertEqual(NULL_LONG, diff_millis(None, dt2))
        self.assertEqual(NULL_LONG, diff_millis(dt1, None))

    def test_diff_seconds(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:21:19.123456789 NY")
        self.assertEqual(2.0, diff_seconds(dt1, dt2))
        self.assertEqual(-2.0, diff_seconds(dt2, dt1))
        self.assertEqual(NULL_DOUBLE, diff_seconds(None, dt2))
        self.assertEqual(NULL_DOUBLE, diff_seconds(dt1, None))

    def test_diff_minutes(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:27:19.123456789 NY")
        self.assertEqual(6, diff_minutes(dt1, dt2))
        self.assertEqual(-6, diff_minutes(dt2, dt1))
        self.assertEqual(NULL_INT, diff_minutes(None, dt2))
        self.assertEqual(NULL_INT, diff_minutes(dt1, None))

    def test_diff_days(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-13T14:21:17.123456789 NY")
        self.assertEqual(3.0, diff_days(dt1, dt2))
        self.assertEqual(-3.0, diff_days(dt2, dt1))
        self.assertEqual(NULL_DOUBLE, diff_days(None, dt2))
        self.assertEqual(NULL_DOUBLE, diff_days(dt1, None))

    def test_diff_years(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2023-12-10T14:21:17.123456789 NY")
        self.assertEqual(2.0, diff_years(dt1, dt2))
        self.assertEqual(-2.0, diff_years(dt2, dt1))
        self.assertEqual(NULL_DOUBLE, diff_years(None, dt2))
        self.assertEqual(NULL_DOUBLE, diff_years(dt1, None))

    # endregion

    # region: Comparisons

    def test_is_before(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:21:18.123456789 NY")
        self.assertTrue(is_before(dt1, dt2))
        self.assertFalse(is_before(dt2, dt1))
        self.assertFalse(is_before(dt1, dt1))
        self.assertFalse(is_before(None, dt1))

    def test_is_before_or_equal(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:21:18.123456789 NY")
        self.assertTrue(is_before_or_equal(dt1, dt2))
        self.assertFalse(is_before_or_equal(dt2, dt1))
        self.assertTrue(is_before_or_equal(dt1, dt1))
        self.assertFalse(is_before_or_equal(None, dt1))

    def test_is_after(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:21:18.123456789 NY")
        self.assertFalse(is_after(dt1, dt2))
        self.assertTrue(is_after(dt2, dt1))
        self.assertFalse(is_after(dt1, dt1))
        self.assertFalse(is_after(None, dt1))

    def test_is_after_or_equal(self):
        dt1 = parse_instant("2021-12-10T14:21:17.123456789 NY")
        dt2 = parse_instant("2021-12-10T14:21:18.123456789 NY")
        self.assertFalse(is_after_or_equal(dt1, dt2))
        self.assertTrue(is_after_or_equal(dt2, dt1))
        self.assertTrue(is_after_or_equal(dt1, dt1))
        self.assertFalse(is_after_or_equal(None, dt1))

    # endregion

    # region: Chronology

    def test_nanos_of_milli(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(456789, nanos_of_milli(dt, tz))
        self.assertEqual(NULL_LONG, nanos_of_milli(None, tz))
        self.assertEqual(NULL_LONG, nanos_of_milli(dt, None))

    def test_micros_of_milli(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(456, micros_of_milli(dt, tz))
        self.assertEqual(NULL_LONG, micros_of_milli(None, tz))
        self.assertEqual(NULL_LONG, micros_of_milli(dt, None))

    def test_micros_of_second(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(123456, micros_of_second(dt, tz))
        self.assertEqual(NULL_LONG, micros_of_second(None, tz))
        self.assertEqual(NULL_LONG, micros_of_second(tz, None))

    def test_millis_of_second(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(123, millis_of_second(dt, tz))
        self.assertEqual(NULL_INT, millis_of_second(None, tz))
        self.assertEqual(NULL_INT, millis_of_second(dt, None))

    def test_second_of_minute(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(17, second_of_minute(dt, tz))
        self.assertEqual(NULL_INT, second_of_minute(None, tz))
        self.assertEqual(NULL_INT, second_of_minute(tz, None))

    def test_minute_of_hour(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(21, minute_of_hour(dt, tz))
        self.assertEqual(NULL_INT, minute_of_hour(None, tz))
        self.assertEqual(NULL_INT, minute_of_hour(dt, None))

    def test_nanos_of_day(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(123456789+17*SECOND+21*MINUTE+14*HOUR, nanos_of_day(dt, tz))
        self.assertEqual(NULL_LONG, nanos_of_day(None, tz))
        self.assertEqual(NULL_LONG, nanos_of_day(dt, None))

    def test_millis_of_day(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual((123456789+17*SECOND+21*MINUTE+14*HOUR) // 10**6, millis_of_day(dt, tz))
        self.assertEqual(NULL_INT, millis_of_day(None, tz))
        self.assertEqual(NULL_INT, millis_of_day(dt, None))

    def test_second_of_day(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual((123456789+17*SECOND+21*MINUTE+14*HOUR) // 10**9, second_of_day(dt, tz))
        self.assertEqual(NULL_INT, second_of_day(None, tz))
        self.assertEqual(NULL_INT, second_of_day(dt, None))

    def test_minute_of_day(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(21+14*60, minute_of_day(dt, tz))
        self.assertEqual(NULL_INT, minute_of_day(None, tz))
        self.assertEqual(NULL_INT, minute_of_day(dt, None))

    def test_hour_of_day(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(14, hour_of_day(dt, tz))
        self.assertEqual(NULL_INT, hour_of_day(None, tz))
        self.assertEqual(NULL_INT, hour_of_day(dt, None))

    def test_day_of_week(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        # 5 - Fri
        self.assertEqual(5, day_of_week(dt, tz))
        self.assertEqual(NULL_INT, day_of_week(None, tz))
        self.assertEqual(NULL_INT, day_of_week(dt, None))

    def test_day_of_month(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(10, day_of_month(dt, tz))
        self.assertEqual(NULL_INT, day_of_month(None, tz))
        self.assertEqual(NULL_INT, day_of_month(dt, None))

    def test_day_of_year(self):
        datetime_str = "2021-02-03T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(31+3, day_of_year(dt, tz))
        self.assertEqual(NULL_INT, day_of_year(None, tz))
        self.assertEqual(NULL_INT, day_of_year(dt, None))

    def test_month_of_year(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(12, month_of_year(dt, tz))
        self.assertEqual(NULL_INT, month_of_year(None, tz))
        self.assertEqual(NULL_INT, month_of_year(dt, None))

    def test_year(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(2021, year(dt, tz))
        self.assertEqual(NULL_INT, year(None, tz))
        self.assertEqual(NULL_INT, year(dt, None))

    def test_year_of_century(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertEqual(21, year_of_century(dt, tz))
        self.assertEqual(NULL_INT, year_of_century(None, tz))
        self.assertEqual(NULL_INT, year_of_century(dt, None))

    def test_at_midnight(self):
        datetime_str = "2021-12-10T02:59:59"
        timezone_str = "NY"
        tz_ny = time_zone("NY")
        tz_pt = time_zone("PT")
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        mid_night_time_ny = at_midnight(dt, tz_ny)
        mid_night_time_pt = at_midnight(dt, tz_pt)
        self.assertEqual(diff_nanos(mid_night_time_ny, mid_night_time_pt) // 10 ** 9, -21 * 60 * 60)

        # DST ended in NY but not in PT
        datetime_str = "2021-11-08T02:59:59"
        timezone_str = "NY"
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        mid_night_time_ny = datetime_at_midnight(dt, tz_ny)
        mid_night_time_pt = datetime_at_midnight(dt, tz_pt)
        self.assertEqual(diff_nanos(mid_night_time_ny, mid_night_time_pt) // 10 ** 9, -22 * 60 * 60)

    # endregion

    # region: Binning

    def test_lower_bin(self):
        dt = now()
        self.assertGreaterEqual(diff_nanos(lower_bin(dt, 1000000, MINUTE), dt), 0)

    def test_upper_bin(self):
        dt = now()
        self.assertGreaterEqual(diff_nanos(dt, upper_bin(dt, 1000000, MINUTE)), 0)

    # endregion
    
    # region: Format

    def test_format_nanos(self):
        nanos = 123456789
        ns_str = format_nanos(nanos)
        self.assertEqual("0:00:00.123456789", ns_str)

    def test_format_datetime(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        dt_str = format_datetime(dt, tz)
        self.assertEqual(f"{datetime_str} {timezone_str}", dt_str)

    def test_format_date(self):
        datetime_str = "2021-12-10T14:21:17.123456789"
        timezone_str = "NY"
        tz = time_zone(timezone_str)
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        dt_str = format_date(dt, tz)
        self.assertEqual("2021-12-10", dt_str)

        dt = now()
        self.assertEqual(3, len(format_date(dt, TimeZone.MOS).split("-")))

    # endregion
    
    # region: Parse

    def test_parse_time_zone:
        tz = parse_time_zone("America/New_York")
        self.assertEqual(str(tz), "America/New_York")

        tz = parse_time_zone("MN")
        self.assertEqual(str(tz), "America/Chicago")

        tz = parse_time_zone(None)
        self.assertEqual(str(tz), "America/New_York")

    def test_parse_nanos(self):
        time_str = "530000:59:39.123456789"
        in_nanos = parse_nanos(time_str)
        self.assertEqual(str(in_nanos), "1908003579123456789")

        with self.assertRaises(DHError) as cm:
            time_str = "530000:59:39.X"
            in_nanos = parse_nanos(time_str)
        self.assertIn("RuntimeException", str(cm.exception))

        time_str = "00:59:39.X"
        in_nanos = parse_nanos(time_str, quiet=True)
        self.assertEqual(in_nanos, NULL_LONG)

        time_str = "1:02:03"
        in_nanos = parse_nanos(time_str)
        time_str2 = format_nanos(in_nanos)
        self.assertEqual(time_str2, time_str)

    def test_parse_period(self):
        period_str = "P1W"
        period = parse_period(period_str)
        self.assertEqual(str(period).upper(), period_str)

        period_str = "P1M"
        period = parse_period(period_str)
        self.assertEqual(str(period).upper(), period_str)

        with self.assertRaises(DHError) as cm:
            period_str = "PT1Y"
            period = parse_period(period_str)
        self.assertIn("RuntimeException", str(cm.exception))

        period = parse_period(period_str, quiet=True)
        self.assertNone(period)

    def test_parse_duration(self):
        duration_str = "PT1M"
        duration = parse_duration(duration_str)
        self.assertEqual(str(duration).upper(), duration_str)

        duration_str = "PT1H"
        duration = parse_duration(duration_str)
        self.assertEqual(str(duration).upper(), duration_str)

        with self.assertRaises(DHError) as cm:
            duration = parse_duration("T1Q")
        self.assertIn("RuntimeException", str(cm.exception))

        duration = parse_duration("T1Q", quiet=True)
        self.assertNone(duration)

    def test_parse_instant(self):
        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "NY"
        dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertTrue(str(dt).startswith(datetime_str))

        with self.assertRaises(DHError) as cm:
            datetime_str = "2021-12-10T23:59:59"
            timezone_str = "--"
            dt = parse_instant(f"{datetime_str} {timezone_str}")
        self.assertIn("RuntimeException", str(cm.exception))

        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "--"
        dt = parse_instant(f"{datetime_str} {timezone_str}", quiet=True)
        self.assertNone(dt)

    def test_parse_zdt(self):
        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "NY"
        dt = parse_zdt(f"{datetime_str} {timezone_str}")
        self.assertTrue(str(dt).startswith(datetime_str))

        with self.assertRaises(DHError) as cm:
            datetime_str = "2021-12-10T23:59:59"
            timezone_str = "--"
            dt = parse_zdt(f"{datetime_str} {timezone_str}")
        self.assertIn("RuntimeException", str(cm.exception))

        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "--"
        dt = parse_zdt(f"{datetime_str} {timezone_str}", quiet=True)
        self.assertNone(dt)

    def test_parse_time_precision(self):
        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "NY"
        tp = parse_time_precision(f"{datetime_str} {timezone_str}")
        self.assertEqual(tp, "SecondOfMinute")

        with self.assertRaises(DHError) as cm:
            datetime_str = "2021-12-10T23:59:59"
            timezone_str = "--"
            tp = parse_time_precision(f"{datetime_str} {timezone_str}")
        self.assertIn("RuntimeException", str(cm.exception))

        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "--"
        tp = parse_time_precision(f"{datetime_str} {timezone_str}", quiet=True)
        self.assertNone(tp)

    def test_parse_local_date(self):
        date_str = "2021-12-10"
        dt = parse_local_date(date_str)
        self.assertTrue(str(dt), date_str)

        with self.assertRaises(DHError) as cm:
            date_str = "2021-x12-10"
            dt = parse_local_date(date_str)
        self.assertIn("RuntimeException", str(cm.exception))

        date_str = "2021-x12-10"
        dt = parse_local_date(date_str, quiet=True)
        self.assertNone(dt)

    def test_parse_local_time(self):
        time_str = "23:59:59"
        dt = parse_local_time(time_str)
        self.assertTrue(str(dt), time_str)

        with self.assertRaises(DHError) as cm:
            time_str = "23:59x:59"
            dt = parse_local_time(time_str)
        self.assertIn("RuntimeException", str(cm.exception))

        time_str = "23:59x:59"
        dt = parse_local_time(time_str, quiet=True)
        self.assertNone(dt)

    # endregion


if __name__ == "__main__":
    unittest.main()
