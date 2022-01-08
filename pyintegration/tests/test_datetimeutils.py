#
#   Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import unittest
from time import sleep

from deephaven2.constants import NULL_LONG
from deephaven2.datetimeutils import *


class DateTimeUtilsTestCase(unittest.TestCase):
    def test_convert_datetime(self):
        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "NY"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        print(dt)
        self.assertTrue(str(dt).startswith(datetime_str))

        with self.assertRaises(DHError) as cm:
            datetime_str = "2021-12-10T23:59:59"
            timezone_str = "--"
            dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertIn("RuntimeException", str(cm.exception))

    def test_convert_period(self):
        period_str = "1W"
        period = convert_period(period_str)
        self.assertEqual(str(period).upper(), period_str)

        period_str = "T1M"
        period = convert_period(period_str)
        self.assertEqual(repr(period).upper(), period_str)

        with self.assertRaises(DHError) as cm:
            period_str = "T1Y"
            period = convert_period(period_str)
        self.assertIn("RuntimeException", str(cm.exception))

    def test_convert_time(self):
        time_str = "530000:59:39.123456789"
        in_nanos = convert_time(time_str)
        self.assertEqual(str(in_nanos), "1908003579123456789")

        with self.assertRaises(DHError) as cm:
            time_str = "530000:59:39.X"
            in_nanos = convert_time(time_str)
        self.assertIn("RuntimeException", str(cm.exception))

        time_str = "00:59:39.X"
        in_nanos = convert_time(time_str, quiet=True)
        self.assertEqual(in_nanos, NULL_LONG)

    def test_current_time_and_diff(self):
        dt = current_time()
        sleep(1)
        dt1 = current_time()
        self.assertGreaterEqual(diff_nanos(dt, dt1), 100000000)

    def test_date_at_midnight(self):
        dt = current_time()
        mid_night_time_ny = date_at_midnight(dt, TimeZone.NY)
        mid_night_time_pt = date_at_midnight(dt, TimeZone.PT)
        self.assertGreaterEqual(diff_nanos(mid_night_time_ny, mid_night_time_pt), 0)

    def test_day_of_month(self):
        dt = current_time()
        self.assertIn(day_of_month(dt, TimeZone.MT), range(1, 32))

    def test_day_of_week(self):
        dt = current_time()
        self.assertIn(day_of_week(dt, TimeZone.MT), range(1, 8))

    def test_day_of_year(self):
        dt = current_time()
        self.assertIn(day_of_year(dt, TimeZone.MT), range(1, 366))

    def test_format_date(self):
        dt = current_time()
        self.assertIn(TimeZone.SYD.name, format_datetime(dt, TimeZone.SYD))

    def test_format_nanos(self):
        dt = current_time()
        ns = nanos(dt)
        ns_str1 = format_nanos(ns).split(".")[-1]
        ns_str2 = format_datetime(dt, TimeZone.UTC).split(".")[-1]
        self.assertTrue(ns_str2.startswith(ns_str1))

    def test_format_as_date(self):
        dt = current_time()
        self.assertEqual(3, len(format_as_date(dt, TimeZone.MOS).split("-")))

    def test_hour_of_day(self):
        dt = current_time()
        self.assertIn(hour_of_day(dt, TimeZone.AL), range(0, 24))

    def test_is_after(self):
        dt1 = current_time()
        sleep(0.001)
        dt2 = current_time()
        self.assertTrue(is_after(dt2, dt1))

    def test_is_before(self):
        dt1 = current_time()
        sleep(0.001)
        dt2 = current_time()
        self.assertFalse(is_before(dt2, dt1))

    def test_lower_bin(self):
        dt = current_time()
        self.assertGreaterEqual(diff_nanos(lower_bin(dt, 1000000, MINUTE), dt), 0)

    def test_millis(self):
        dt = current_time()
        self.assertGreaterEqual(nanos(dt), millis(dt) * 10 ** 6)

    def test_millis_of_second(self):
        dt = current_time()
        self.assertGreaterEqual(millis_of_second(dt, TimeZone.AT), 0)

    def test_millis_to_nanos(self):
        dt = current_time()
        ms = millis(dt)
        self.assertEqual(ms * 10 ** 6, millis_to_nanos(ms))

    def test_minus(self):
        dt1 = current_time()
        dt2 = current_time()
        self.assertGreaterEqual(0, minus(dt1, dt2))

    def test_minus_nanos(self):
        dt = current_time()
        dt1 = minus_nanos(dt, 1)
        self.assertEqual(1, diff_nanos(dt1, dt))

    def test_minus_period(self):
        period_str = "T1H"
        period = convert_period(period_str)

        dt = current_time()
        dt1 = minus_period(dt, period)
        self.assertEqual(diff_nanos(dt1, dt), 60 * 60 * 10 ** 9)

    def test_minute_of_day(self):
        datetime_str = "2021-12-10T00:59:59"
        timezone_str = "BT"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(59, minute_of_day(dt, TimeZone.BT))

    def test_minute_of_hour(self):
        datetime_str = "2021-12-10T23:59:59"
        timezone_str = "CE"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(59, minute_of_hour(dt, TimeZone.CE))

    def test_month_of_year(self):
        datetime_str = "2021-08-10T23:59:59"
        timezone_str = "CH"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(8, month_of_year(dt, TimeZone.CH))

    def test_nanos_of_day(self):
        datetime_str = "2021-12-10T00:00:01"
        timezone_str = "CT"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(10 ** 9, nanos_of_day(dt, TimeZone.CT))

    def test_nanos_of_second(self):
        datetime_str = "2021-12-10T00:00:01.000000123"
        timezone_str = "ET"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(123, nanos_of_second(dt, TimeZone.ET))

    def test_nanos_to_millis(self):
        dt = current_time()
        ns = nanos(dt)
        self.assertEqual(ns // 10 ** 6, nanos_to_millis(ns))

    def test_nanos_to_time(self):
        dt = current_time()
        ns = nanos(dt)
        dt1 = nanos_to_time(ns)
        self.assertEqual(dt, dt1)

    def test_plus_period(self):
        period_str = "T1H"
        period = convert_period(period_str)

        dt = current_time()
        dt1 = plus_period(dt, period)
        self.assertEqual(diff_nanos(dt, dt1), 60 * 60 * 10 ** 9)

    def test_plus_nanos(self):
        dt = current_time()
        dt1 = plus_nanos(dt, 1)
        self.assertEqual(1, diff_nanos(dt, dt1))

    def test_second_of_day(self):
        datetime_str = "2021-12-10T00:01:05"
        timezone_str = "HI"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(65, second_of_day(dt, TimeZone.HI))

    def test_second_of_minute(self):
        datetime_str = "2021-12-10T00:01:05"
        timezone_str = "HK"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(5, second_of_minute(dt, TimeZone.HK))

    def test_upper_bin(self):
        dt = current_time()
        self.assertGreaterEqual(diff_nanos(dt, upper_bin(dt, 1000000, MINUTE)), 0)

    def test_year(self):
        datetime_str = "2021-12-10T00:01:05"
        timezone_str = "IN"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(2021, year(dt, TimeZone.IN))

    def test_year(self):
        datetime_str = "2021-12-10T00:01:05"
        timezone_str = "JP"
        dt = convert_datetime(f"{datetime_str} {timezone_str}")
        self.assertEqual(21, year_of_century(dt, TimeZone.JP))


if __name__ == '__main__':
    unittest.main()
