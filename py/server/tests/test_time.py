#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest
from time import sleep
import datetime

from deephaven import dtypes
from deephaven.constants import NULL_LONG, NULL_INT
from deephaven.time import *
from tests.testbase import BaseTestCase

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

class TimeTestCase(BaseTestCase):

    # region: CLock

    def test_now(self):
        for system in [True, False]:
            for resolution in ['ns', 'ms']:
                dt = now(system=system, resolution=resolution)
                sleep(1)
                dt1 = now(system=system, resolution=resolution)
                self.assertGreaterEqual(diff_nanos(dt, dt1), 100000000)

    def test_today(self):
        tz = time_zone("UTC")
        td = today(tz)
        target = datetime.datetime.utcnow().date().strftime('%Y-%m-%d')
        self.assertEqual(td, target)

    # endregion
    
    # region: Time Zone

    # def test_time_zone(self):
    #     tz = time_zone("America/New_York")
    #     self.assertEqual(str(tz), "America/New_York")
    #
    #     tz = time_zone("CT")
    #     self.assertEqual(str(tz), "America/Chicago")
    #
    #     tz = time_zone(None)
    #     self.assertEqual(str(tz), "Etc/UTC")

    def test_time_zone_alias_add_rm(self):
        alias = "TestAlias"
        tz_str = "Etc/UTC"

        with self.assertRaises(DHError) as cm:
            to_j_time_zone(alias)

        self.assertFalse(time_zone_alias_rm(alias))
        time_zone_alias_add(alias, tz_str)
        tz = to_j_time_zone(alias)
        self.assertEqual(str(tz), tz_str)
        self.assertTrue(time_zone_alias_rm(alias))

        with self.assertRaises(DHError) as cm:
            to_j_time_zone(alias)


    # endregion

    # region Conversions: Python To Java

    def test_to_j_time_zone(self):
        tz = to_j_time_zone("America/New_York")
        self.assertEqual(str(tz), "America/New_York")

        tz = to_j_time_zone("CT")
        self.assertEqual(str(tz), "America/Chicago")

        tz = to_j_time_zone(None)
        self.assertEqual(str(tz), "Etc/UTC")


    def test_to_j_local_date(self):
        ld = to_j_local_date("2021-12-10")
        self.assertEqual(str(ld), "2021-12-10")

        d = datetime.date(2021, 12, 10)
        ld = to_j_local_date(d)
        self.assertEqual(str(ld), "2021-12-10")

        d = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456)
        ld = to_j_local_date(d)
        self.assertEqual(str(ld), "2021-12-10")

        d = np.datetime64("2021-12-10")
        ld = to_j_local_date(d)
        self.assertEqual(str(ld), "2021-12-10")

        ld = to_j_local_date(None)
        self.assertEqual(ld, None)


    def test_to_j_local_time(self):
        lt = to_j_local_time("14:21:17.123456")
        self.assertEqual(str(lt), "14:21:17.123456")

        t = datetime.time(14, 21, 17, 123456)
        lt = to_j_local_time(t)
        self.assertEqual(str(lt), "14:21:17.123456")

        t = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456)
        lt = to_j_local_time(t)
        self.assertEqual(str(lt), "14:21:17.123456")

        t = np.datetime64(t)
        lt = to_j_local_time(t)
        self.assertEqual(str(lt), "14:21:17.123456")

        lt = to_j_local_time(None)
        self.assertEqual(lt, None)

    def test_to_j_instant(self):
        dt = to_j_instant("2021-12-10T14:21:17.123456789 ET")
        self.assertTrue(str(dt).startswith("2021-12-10T19:21:17.123456789Z"))

        x = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456789)
        dt = to_j_instant(x)
        self.assertTrue(str(dt).startswith("2021-12-10T14:21:17.123456789Z"))

        x = np.datetime64(x)
        dt = to_j_instant(x)
        self.assertTrue(str(dt).startswith("2021-12-10T14:21:17.123456789Z"))

        dt = to_j_instant(None)
        self.assertEqual(dt, None)


    def test_to_j_zdt(self):
        dt = to_j_zdt("2021-12-10T14:21:17.123456789 ET")
        self.assertTrue(str(dt).startswith("2021-12-10T19:21:17.123456789Z"))

        x = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456789)
        dt = to_j_zdt(x)
        self.assertTrue(str(dt).startswith("2021-12-10T14:21:17.123456789Z"))

        x = np.datetime64(x)
        dt = to_j_zdt(x)
        self.assertTrue(str(dt).startswith("2021-12-10T14:21:17.123456789Z"))

        dt = to_j_zdt(None)
        self.assertEqual(dt, None)


    def test_to_j_duration(self):
        d = to_j_duration("PT1H")
        self.assertEqual(str(d), "PT1H")

        x = datetime.timedelta(hours=1, minutes=2, seconds=3, milliseconds=4, microseconds=5)
        dt = to_j_duration(x)
        self.assertEqual(dt, _JDateTimeUtils.parseDuration("PT1H2M3.004005S"))

        x = np.timedelta64(x)
        dt = to_j_duration(x)
        self.assertEqual(dt, _JDateTimeUtils.parseDuration("PT1H2M3.004005S"))

        d = to_j_duration(None)
        self.assertEqual(d, None)


    def test_to_j_period(self):
        p = to_j_period("P1W")
        self.assertEqual(str(p), "P7D")

        x = datetime.timedelta(days=2)
        p = to_j_period(x)
        self.assertEqual(str(p), "P2D")

        x = np.timedelta64(2, 'D')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2D")

        x = np.timedelta64(2, 'W')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2W")

        x = np.timedelta64(2, 'Y')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2Y")

        p = to_j_period(None)
        self.assertEqual(p, None)

    # endregion


    # region Conversions: Java To Python

    def test_xxx(self):
        self.fail("TODO--more tests")
        self.fail("TODO--test all branches")

    # endregion


    # #################### TODO REMOVE BELOW ####################
    #
    # # region: Conversions: Date Time Types
    #
    # def test_to_instant(self):
    #     dt1 = parse_instant("2021-12-10T14:21:17.123456789 ET")
    #     dt2 = parse_zdt("2021-12-10T14:21:17.123456789 ET")
    #
    #     self.assertEqual(dt1, to_instant(dt2))
    #     self.assertEquals(None,to_instant(None))
    #
    # def test_to_zdt(self):
    #     dt1 = parse_instant("2021-12-10T14:21:17.123456789 ET")
    #     dt2 = parse_zdt("2021-12-10T14:21:17.123456789 ET")
    #
    #     self.assertEqual(dt2, to_zdt(dt1, time_zone("ET")))
    #     self.assertEquals(None,to_zdt(None, time_zone("ET")))
    #
    # def test_make_instant(self):
    #     dt = parse_instant("2021-12-10T14:21:17.123456789 ET")
    #     ld = parse_local_date("2021-12-10")
    #     lt = parse_local_time("14:21:17.123456789")
    #     tz = time_zone("ET")
    #
    #     self.assertEqual(dt, make_instant(ld, lt, tz))
    #     self.assertEquals(None,make_instant(ld, lt, None))
    #     self.assertEquals(None,make_instant(ld, None, tz))
    #     self.assertEquals(None,make_instant(None, lt, tz))
    #
    # def test_make_zdt(self):
    #     dt = parse_zdt("2021-12-10T14:21:17.123456789 ET")
    #     ld = parse_local_date("2021-12-10")
    #     lt = parse_local_time("14:21:17.123456789")
    #     tz = time_zone("ET")
    #
    #     self.assertEqual(dt, make_zdt(ld, lt, tz))
    #     self.assertEquals(None,make_zdt(ld, lt, None))
    #     self.assertEquals(None,make_zdt(ld, None, tz))
    #     self.assertEquals(None,make_zdt(None, lt, tz))
    #
    # def test_to_local_date(self):
    #     dt1 = parse_instant("2021-12-10T14:21:17.123456789 ET")
    #     dt2 = parse_zdt("2021-12-10T14:21:17.123456789 ET")
    #     tz = time_zone("ET")
    #     ld = parse_local_date("2021-12-10")
    #
    #     self.assertEqual(ld, to_local_date(dt1, tz))
    #     self.assertEqual(ld, to_local_date(dt2, tz))
    #     self.assertEquals(None,to_local_date(dt1, None))
    #     self.assertEquals(None,to_local_date(dt2, None))
    #     self.assertEquals(None,to_local_date(None, tz))
    #
    # def test_to_local_time(self):
    #     dt1 = parse_instant("2021-12-10T14:21:17.123456789 ET")
    #     dt2 = parse_zdt("2021-12-10T14:21:17.123456789 ET")
    #     tz = time_zone("ET")
    #     lt = parse_local_time("14:21:17.123456789")
    #
    #     self.assertEqual(lt, to_local_time(dt1, tz))
    #     self.assertEqual(lt, to_local_time(dt2, tz))
    #     self.assertEquals(None,to_local_time(dt1, None))
    #     self.assertEquals(None,to_local_time(dt2, None))
    #     self.assertEquals(None,to_local_time(None, tz))
    #
    # # endregion
    #
    #
    # # region: Parse
    #
    # def test_parse_time_zone(self):
    #     tz = parse_time_zone("America/New_York")
    #     self.assertEqual(str(tz), "America/New_York")
    #
    #     tz = parse_time_zone("CT")
    #     self.assertEqual(str(tz), "America/Chicago")
    #
    #     with self.assertRaises(DHError) as cm:
    #         tz = parse_time_zone(None)
    #     self.assertTrue(cm.exception.root_cause)
    #     self.assertIn("Cannot parse", cm.exception.compact_traceback)
    #
    #     with self.assertRaises(DHError) as cm:
    #         tz = parse_time_zone("JUNK")
    #     self.assertTrue(cm.exception.root_cause)
    #     self.assertIn("Cannot parse time zone", cm.exception.compact_traceback)
    #
    #     tz = parse_time_zone("JUNK", quiet=True)
    #     self.assertEqual(None, tz)
    #
    # def test_parse_duration_nanos(self):
    #     time_str = "PT530000:59:39.123456789"
    #     in_nanos = parse_duration_nanos(time_str)
    #     self.assertEqual(str(in_nanos), "1908003579123456789")
    #
    #     with self.assertRaises(DHError) as cm:
    #         time_str = "PT530000:59:39.X"
    #         in_nanos = parse_duration_nanos(time_str)
    #     self.assertIn("DateTimeParseException", str(cm.exception))
    #
    #     time_str = "PT00:59:39.X"
    #     in_nanos = parse_duration_nanos(time_str, quiet=True)
    #     self.assertEqual(in_nanos, NULL_LONG)
    #
    #     time_str = "PT1:02:03"
    #     in_nanos = parse_duration_nanos(time_str)
    #     time_str2 = format_duration_nanos(in_nanos)
    #     self.assertEqual(time_str2, time_str)
    #
    #     time_str = "PT1h"
    #     in_nanos = parse_duration_nanos(time_str)
    #     time_str2 = format_duration_nanos(in_nanos)
    #     self.assertEqual(time_str2, "PT1:00:00")
    #
    # def test_parse_period(self):
    #     period_str = "P1W"
    #     period = parse_period(period_str)
    #     # Java Period normalizes weeks to days in toString()
    #     self.assertEqual(str(period).upper(), "P7D")
    #
    #     period_str = "P6D"
    #     period = parse_period(period_str)
    #     self.assertEqual(str(period).upper(), period_str)
    #
    #     period_str = "P1M"
    #     period = parse_period(period_str)
    #     self.assertEqual(str(period).upper(), period_str)
    #
    #     with self.assertRaises(DHError) as cm:
    #         period_str = "PT1Y"
    #         period = parse_period(period_str)
    #     self.assertIn("DateTimeParseException", str(cm.exception))
    #
    #     period = parse_period(period_str, quiet=True)
    #     self.assertEquals(None,period)
    #
    # def test_parse_duration(self):
    #     duration_str = "PT1M"
    #     duration = parse_duration(duration_str)
    #     self.assertEqual(str(duration).upper(), duration_str)
    #
    #     duration_str = "PT1H"
    #     duration = parse_duration(duration_str)
    #     self.assertEqual(str(duration).upper(), duration_str)
    #
    #     with self.assertRaises(DHError) as cm:
    #         duration = parse_duration("T1Q")
    #     self.assertIn("DateTimeParseException", str(cm.exception))
    #
    #     duration = parse_duration("T1Q", quiet=True)
    #     self.assertEquals(None,duration)
    #
    # def test_parse_epoch_nanos(self):
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "ET"
    #     dt = parse_instant(f"{datetime_str} {timezone_str}")
    #     n = parse_epoch_nanos(f"{datetime_str} {timezone_str}")
    #     self.assertEqual(epoch_nanos(dt), n)
    #
    #     with self.assertRaises(DHError) as cm:
    #         datetime_str = "2021-12-10T23:59:59"
    #         timezone_str = "--"
    #         dt = parse_epoch_nanos(f"{datetime_str} {timezone_str}")
    #     self.assertIn("RuntimeException", str(cm.exception))
    #
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "--"
    #     dt = parse_epoch_nanos(f"{datetime_str} {timezone_str}", quiet=True)
    #     self.assertEquals(NULL_LONG,dt)
    #
    # def test_parse_instant(self):
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "ET"
    #     dt = parse_instant(f"{datetime_str} {timezone_str}")
    #     self.assertTrue(format_datetime(dt, time_zone("ET")).startswith(datetime_str))
    #
    #     with self.assertRaises(DHError) as cm:
    #         datetime_str = "2021-12-10T23:59:59"
    #         timezone_str = "--"
    #         dt = parse_instant(f"{datetime_str} {timezone_str}")
    #     self.assertIn("RuntimeException", str(cm.exception))
    #
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "--"
    #     dt = parse_instant(f"{datetime_str} {timezone_str}", quiet=True)
    #     self.assertEquals(None,dt)
    #
    # def test_parse_zdt(self):
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "ET"
    #     dt = parse_zdt(f"{datetime_str} {timezone_str}")
    #     self.assertTrue(str(dt).startswith(datetime_str))
    #
    #     with self.assertRaises(DHError) as cm:
    #         datetime_str = "2021-12-10T23:59:59"
    #         timezone_str = "--"
    #         dt = parse_zdt(f"{datetime_str} {timezone_str}")
    #     self.assertIn("RuntimeException", str(cm.exception))
    #
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "--"
    #     dt = parse_zdt(f"{datetime_str} {timezone_str}", quiet=True)
    #     self.assertEquals(None,dt)
    #
    # def test_parse_time_precision(self):
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "ET"
    #     tp = parse_time_precision(f"{datetime_str} {timezone_str}")
    #     self.assertEqual(tp, "SecondOfMinute")
    #
    #     with self.assertRaises(DHError) as cm:
    #         datetime_str = "2021-12-10T23:59:59"
    #         timezone_str = "--"
    #         tp = parse_time_precision(f"{datetime_str} {timezone_str}")
    #     self.assertIn("RuntimeException", str(cm.exception))
    #
    #     datetime_str = "2021-12-10T23:59:59"
    #     timezone_str = "--"
    #     tp = parse_time_precision(f"{datetime_str} {timezone_str}", quiet=True)
    #     self.assertEquals(None,tp)
    #
    # def test_parse_local_date(self):
    #     date_str = "2021-12-10"
    #     dt = parse_local_date(date_str)
    #     self.assertTrue(str(dt), date_str)
    #
    #     with self.assertRaises(DHError) as cm:
    #         date_str = "2021-x12-10"
    #         dt = parse_local_date(date_str)
    #     self.assertIn("DateTimeParseException", str(cm.exception))
    #
    #     date_str = "2021-x12-10"
    #     dt = parse_local_date(date_str, quiet=True)
    #     self.assertEquals(None,dt)
    #
    # def test_parse_local_time(self):
    #     time_str = "23:59:59"
    #     dt = parse_local_time(time_str)
    #     self.assertTrue(str(dt), time_str)
    #
    #     with self.assertRaises(DHError) as cm:
    #         time_str = "23:59x:59"
    #         dt = parse_local_time(time_str)
    #     self.assertIn("DateTimeParseException", str(cm.exception))
    #
    #     time_str = "23:59x:59"
    #     dt = parse_local_time(time_str, quiet=True)
    #     self.assertEquals(None,dt)
    #
    # # endregion


if __name__ == "__main__":
    unittest.main()
