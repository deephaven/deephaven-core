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

    #TODO:  epoch_nanos
    #TODO:  epoch_micros
    #TODO:  epoch_millis
    #TODO:  epoch_seconds
    #TODO:  epoch_nanos_to_instant
    #TODO:  epoch_micros_to_instant
    #TODO:  epoch_millis_to_instant
    #TODO:  epoch_seconds_to_instant
    #TODO:  epoch_nanos_to_zdt
    #TODO:  epoch_micros_to_zdt
    #TODO:  epoch_millis_to_zdt
    #TODO:  epoch_seconds_to_zdt
    #TODO:  epoch_auto_to_epoch_nanos
    #TODO:  epoch_auto_to_instant
    #TODO:  epoch_auto_to_zdt

    # endregion
    
    # region: Conversions: Excel

    #TODO:  to_excel_time
    #TODO:  excel_to_instant
    #TODO:  excel_to_zdt

    # endregion
    
    # region: Arithmetic

    #TODO:  plus_period
    #TODO:  minus_period
    #TODO:  diff_nanos
    #TODO:  diff_micros
    #TODO:  diff_millis
    #TODO:  diff_seconds
    #TODO:  diff_minutes
    #TODO:  diff_days
    #TODO:  diff_years

    # endregion

    # region: Comparisons

    #TODO:  is_before
    #TODO:  is_before_or_equal
    #TODO:  is_after
    #TODO:  is_after_or_equal

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












    def test_diff_days(self):
        dt1 = now()
        dt2 = plus_nanos(dt1, 2*DAY)
        self.assertGreaterEqual(diff_days(dt2, dt1), 1.9)

    def test_diff_years(self):
        dt1 = now()
        dt2 = plus_nanos(dt1, 2*YEAR)
        self.assertGreaterEqual(diff_years(dt2, dt1), 1.9)



    def test_is_after(self):
        dt1 = now()
        sleep(0.001)
        dt2 = now()
        self.assertTrue(is_after(dt2, dt1))
        self.assertFalse(is_after(None, dt1))

    def test_is_before(self):
        dt1 = now()
        sleep(0.001)
        dt2 = now()
        self.assertFalse(is_before(dt2, dt1))
        self.assertFalse(is_after(None, dt1))


    def test_millis(self):
        dt = now()
        self.assertGreaterEqual(nanos(dt), millis(dt) * 10 ** 6)
        self.assertEqual(millis(None), NULL_LONG)


    def test_millis_to_nanos(self):
        dt = now()
        ms = millis(dt)
        self.assertEqual(ms * 10 ** 6, millis_to_nanos(ms))
        self.assertEqual(NULL_LONG, millis_to_nanos(NULL_LONG))

    def test_minus(self):
        dt1 = now()
        dt2 = now()
        self.assertGreaterEqual(0, minus(dt1, dt2))
        self.assertEqual(NULL_LONG, minus(None, dt2))

    def test_minus_nanos(self):
        dt = now()
        dt1 = minus_nanos(dt, 1)
        self.assertEqual(1, diff_nanos(dt1, dt))

    def test_minus_period(self):
        period_str = "T1H"
        period = to_period(period_str)

        dt = now()
        dt1 = minus_period(dt, period)
        self.assertEqual(diff_nanos(dt1, dt), 60 * 60 * 10 ** 9)



    def test_nanos_to_time(self):
        dt = now()
        ns = nanos(dt)
        dt1 = nanos_to_datetime(ns)
        self.assertEqual(dt, dt1)
        self.assertEqual(None, nanos_to_datetime(NULL_LONG))

    def test_plus_period(self):
        period_str = "T1H"
        period = to_period(period_str)

        dt = now()
        dt1 = plus_period(dt, period)
        self.assertEqual(diff_nanos(dt, dt1), 60 * 60 * 10 ** 9)

        period_str = "1WT1H"
        period = to_period(period_str)
        dt2 = plus_period(dt, period)
        self.assertEqual(diff_nanos(dt, dt2), (7 * 24 + 1) * 60 * 60 * 10 ** 9)

    def test_plus_nanos(self):
        dt = now()
        dt1 = plus_nanos(dt, 1)
        self.assertEqual(1, diff_nanos(dt, dt1))
        self.assertEqual(None, plus_nanos(None, 1))


    # def test_timezone(self):
    #     default_tz = TimeZone.get_default_timezone()
    #     TimeZone.set_default_timezone(TimeZone.UTC)
    #     tz1 = TimeZone.get_default_timezone()
    #     self.assertEqual(TimeZone.UTC, tz1)
    #     TimeZone.set_default_timezone(TimeZone.JP)
    #     tz2 = TimeZone.get_default_timezone()
    #     self.assertEqual(TimeZone.JP, tz2)
    #     TimeZone.set_default_timezone(default_tz)


if __name__ == "__main__":
    unittest.main()
