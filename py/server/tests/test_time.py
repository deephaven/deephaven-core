#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#

import unittest
from time import sleep
import datetime
import sys
import pandas as pd
import numpy as np

from deephaven.time import *
from tests.testbase import BaseTestCase

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

class TimeTestCase(BaseTestCase):

    # region: CLock

    def test_dh_now(self):
        for system in [True, False]:
            for resolution in ['ns', 'ms']:
                dt = dh_now(system=system, resolution=resolution)
                sleep(1)
                dt1 = dh_now(system=system, resolution=resolution)
                self.assertGreaterEqual(_JDateTimeUtils.diffNanos(dt, dt1), 100000000)

    def test_dh_today(self):
        tz = _JDateTimeUtils.timeZone("UTC")
        td = dh_today(tz)
        target = datetime.datetime.utcnow().date().strftime('%Y-%m-%d')
        self.assertEqual(td, target)

    def test_dh_time_zone(self):
        tz = dh_time_zone()
        self.assertEqual(str(tz), "Etc/UTC")

    # endregion
    
    # region: Time Zone

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

        pytz = datetime.datetime.now().astimezone().tzinfo
        tz = to_j_time_zone(pytz)
        self.assertEqual(str(tz), "UTC")

        pytz = datetime.datetime.now()
        with self.assertRaises(DHError):
            tz = to_j_time_zone(pytz)
            self.fail("Expected DHError")

        pytz = datetime.datetime.now().astimezone()
        tz = to_j_time_zone(pytz)
        self.assertEqual(str(tz), "UTC")

        pytz = pd.Timestamp("2021-12-10T14:21:17.123456Z")
        tz = to_j_time_zone(pytz)
        self.assertEqual(str(tz), "UTC")

        tz = to_j_time_zone(None)
        self.assertEqual(tz, None)

        tz = to_j_time_zone(pd.Timestamp("NaT"))
        self.assertEqual(tz, None)

        tz1 = to_j_time_zone("CT")
        tz2 = to_j_time_zone(tz1)
        self.assertEqual(tz1, tz2)

        ts = pd.Timestamp("2022-07-07", tz="America/New_York")
        self.assertEqual(to_j_time_zone(ts), to_j_time_zone("America/New_York"))

        dttz = datetime.timezone(offset=datetime.timedelta(hours=5), name="XYZ")
        dt = datetime.datetime(2022, 7, 7, 14, 21, 17, 123456, tzinfo=dttz)
        self.assertEqual(to_j_time_zone(dttz), to_j_time_zone("UTC+5"))
        self.assertEqual(to_j_time_zone(dt), to_j_time_zone("UTC+5"))

        dttz = datetime.timezone(offset=-datetime.timedelta(hours=5), name="XYZ")
        dt = datetime.datetime(2022, 7, 7, 14, 21, 17, 123456, tzinfo=dttz)
        self.assertEqual(to_j_time_zone(dttz), to_j_time_zone("UTC-5"))
        self.assertEqual(to_j_time_zone(dt), to_j_time_zone("UTC-5"))

        dttz = datetime.timezone(offset=-datetime.timedelta(hours=5, microseconds=10), name="XYZ")
        dt = datetime.datetime(2022, 7, 7, 14, 21, 17, 123456, tzinfo=dttz)

        with self.assertRaises(DHError):
            to_j_time_zone(dttz)
            self.fail("Expected DHError")

        with self.assertRaises(DHError):
            to_j_time_zone(dt)
            self.fail("Expected DHError")

        if sys.version_info >= (3, 9):
            import zoneinfo
            dttz = zoneinfo.ZoneInfo("America/New_York")
            dt = datetime.datetime(2022, 7, 7, 14, 21, 17, 123456, tzinfo=dttz)
            self.assertEqual(to_j_time_zone(dttz), to_j_time_zone("America/New_York"))
            self.assertEqual(to_j_time_zone(dt), to_j_time_zone("America/New_York"))

        with self.assertRaises(TypeError):
            to_j_time_zone(False)
            self.fail("Expected TypeError")


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

        d = pd.Timestamp("2021-12-10T14:21:17.123456Z")
        ld = to_j_local_date(d)
        self.assertEqual(str(ld), "2021-12-10")

        ld = to_j_local_date(None)
        self.assertEqual(ld, None)

        ld = to_j_local_date(pd.Timestamp("NaT"))
        self.assertEqual(ld, None)

        ld1 = to_j_local_date("2021-12-10")
        ld2 = to_j_local_date(ld1)
        self.assertEqual(ld1, ld2)

        with self.assertRaises(TypeError):
            to_j_local_date(False)
            self.fail("Expected TypeError")


    def test_to_j_local_time(self):
        lt = to_j_local_time("14:21:17.123456")
        self.assertEqual(str(lt), "14:21:17.123456")

        ltn = lt.toNanoOfDay()
        lt = to_j_local_time(ltn)
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

        t = pd.Timestamp("2021-12-10T14:21:17.123456789Z")
        lt = to_j_local_time(t)
        self.assertEqual(str(lt), "14:21:17.123456789")

        lt = to_j_local_time(None)
        self.assertEqual(lt, None)

        lt = to_j_local_time(np.datetime64("NaT"))
        self.assertEqual(lt, None)

        lt = to_j_local_time(pd.Timestamp("NaT"))
        self.assertEqual(lt, None)

        lt1 = to_j_local_time("14:21:17.123456")
        lt2 = to_j_local_time(lt1)
        self.assertEqual(lt1, lt2)

        with self.assertRaises(TypeError):
            to_j_local_time(False)
            self.fail("Expected TypeError")

    def test_to_j_instant(self):
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T19:21:17.123456Z")

        dt = to_j_instant("2021-12-10T14:21:17.123456 ET")
        self.assertEqual(str(target), str(dt))

        dtn = _JDateTimeUtils.epochNanos(dt)
        dt = to_j_instant(dtn)
        self.assertEqual(str(target), str(dt))

        # 1 ns is a rounding error
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T19:21:17.123456001Z")
        x = datetime.datetime(2021, 12, 10, 19, 21, 17, 123456, tzinfo=datetime.timezone.utc)
        dt = to_j_instant(x)
        self.assertEqual(str(target), str(dt))

        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T19:21:17.123456Z")
        x = np.datetime64(x)
        dt = to_j_instant(x)
        self.assertEqual(str(target), str(dt))

        # 1 ns is a rounding error
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T19:21:17.123456001Z")
        x = pd.Timestamp(x)
        dt = to_j_instant(x)
        self.assertEqual(str(target), str(dt))

        dt = to_j_instant(None)
        self.assertEqual(dt, None)

        dt = to_j_instant(pd.NaT)
        self.assertEqual(dt, None)

        dt = to_j_instant(np.datetime64("NaT"))
        self.assertEqual(dt, None)

        dt1 = target.toInstant()
        dt2 = to_j_instant(dt1)
        self.assertEqual(dt1, dt2)

        with self.assertRaises(TypeError):
            to_j_instant(False)
            self.fail("Expected TypeError")


    def test_to_j_zdt(self):
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456-05:00[America/New_York]")

        dt = to_j_zdt("2021-12-10T14:21:17.123456 ET")
        self.assertEqual(str(target), str(dt))

        # 1 ns is a rounding error
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456001Z[Etc/UTC]")
        x = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456)
        dt = to_j_zdt(x)
        self.assertEqual(str(target), str(dt))

        # 1 ns is a rounding error
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456001Z[Etc/UTC]")
        x = pd.Timestamp(x)
        dt = to_j_zdt(x)
        self.assertEqual(str(target), str(dt))

        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456Z[Etc/UTC]")
        x = np.datetime64(x)
        dt = to_j_zdt(x)
        self.assertEqual(str(target), str(dt))

        dt = to_j_zdt(None)
        self.assertEqual(dt, None)

        dt = to_j_zdt(np.datetime64("NaT"))
        self.assertEqual(dt, None)

        dt = to_j_zdt(pd.Timestamp("NaT"))
        self.assertEqual(dt, None)

        dt = to_j_zdt(target)
        self.assertEqual(dt, target)

        with self.assertRaises(TypeError):
            to_j_zdt(False)
            self.fail("Expected TypeError")


    def test_to_j_duration(self):
        d = to_j_duration("PT1H")
        self.assertEqual(str(d), "PT1H")

        d = to_j_duration(2)
        self.assertEqual(str(d), "PT0.000000002S")

        x = datetime.timedelta(hours=1, minutes=2, seconds=3, milliseconds=4, microseconds=5)
        dt = to_j_duration(x)
        self.assertEqual(dt, _JDateTimeUtils.parseDuration("PT1H2M3.004005S"))

        x = np.timedelta64(x)
        dt = to_j_duration(x)
        self.assertEqual(dt, _JDateTimeUtils.parseDuration("PT1H2M3.004005S"))

        x = pd.Timedelta(x)
        dt = to_j_duration(x)
        self.assertEqual(dt, _JDateTimeUtils.parseDuration("PT1H2M3.004005S"))

        d = to_j_duration(None)
        self.assertEqual(d, None)

        d = to_j_duration(np.timedelta64("NaT"))
        self.assertEqual(d, None)

        d = to_j_duration(pd.Timedelta("NaT"))
        self.assertEqual(d, None)

        d1 = to_j_duration("PT1H")
        d2 = to_j_duration(d1)
        self.assertEqual(d1, d2)

        with self.assertRaises(TypeError):
            to_j_duration(False)
            self.fail("Expected TypeError")


    def test_to_j_period(self):
        p = to_j_period("P2W")
        self.assertEqual(str(p), "P14D")

        x = datetime.timedelta(days=2)
        p = to_j_period(x)
        self.assertEqual(str(p), "P2D")

        x = pd.Timedelta(days=2)
        p = to_j_period(x)
        self.assertEqual(str(p), "P2D")

        x = np.timedelta64(2, 'D')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2D")

        x = np.timedelta64(2, 'W')
        p = to_j_period(x)
        self.assertEqual(str(p), "P14D")

        x = np.timedelta64(2, 'M')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2M")

        x = np.timedelta64(2, 'Y')
        p = to_j_period(x)
        self.assertEqual(str(p), "P2Y")

        p = to_j_period(None)
        self.assertEqual(p, None)

        d = to_j_period(np.timedelta64("NaT"))
        self.assertEqual(d, None)

        d = to_j_period(pd.Timedelta("NaT"))
        self.assertEqual(d, None)

        p1 = to_j_period("P2W")
        p2 = to_j_period(p1)
        self.assertEqual(p1, p2)

        with self.assertRaises(TypeError):
            to_j_period(False)
            self.fail("Expected TypeError")

        with self.assertRaises(ValueError):
            x = datetime.timedelta(days=2, seconds=1)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = datetime.timedelta(days=2, microseconds=1)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = datetime.timedelta(days=2.3)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = pd.Timedelta(days=2, seconds=1)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = pd.Timedelta(days=2, microseconds=1)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = pd.Timedelta(days=2, nanoseconds=1)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = pd.Timedelta(days=2.3)
            to_j_period(x)
            self.fail("Expected ValueError")

        with self.assertRaises(ValueError):
            x = np.timedelta64(2, 'h')
            to_j_period(x)
            self.fail("Expected ValueError")


    # endregion


    # region Conversions: Java To Python

    def test_to_date(self):
        target = datetime.date(2021, 12, 10)

        ld = _JDateTimeUtils.parseLocalDate("2021-12-10")
        dt = to_date(ld)
        self.assertEqual(dt, target)

        lt = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456 ET")
        dt = to_date(lt)
        self.assertEqual(dt, target)

        dt = to_date(None)
        self.assertEqual(dt, None)

        with self.assertRaises(TypeError):
            to_date(False)
            self.fail("Expected TypeError")

    def test_to_time(self):
        target = datetime.time(14, 21, 17, 123456)

        lt = _JDateTimeUtils.parseLocalTime("14:21:17.123456")
        dt = to_time(lt)
        self.assertEqual(dt, target)

        lt = _JDateTimeUtils.parseZonedDateTime("2023-07-11T14:21:17.123456 ET")
        dt = to_time(lt)
        self.assertEqual(dt, target)

        dt = to_time(None)
        self.assertEqual(dt, None)

        with self.assertRaises(TypeError):
            to_time(False)
            self.fail("Expected TypeError")

    def test_to_datetime(self):
        target = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456)

        dt = _JDateTimeUtils.parseInstant("2021-12-10T14:21:17.123456Z")
        dt = to_datetime(dt)
        self.assertEqual(dt, target)

        dt = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456Z")
        dt = to_datetime(dt)
        self.assertEqual(dt, target)

        dt = to_datetime(None)
        self.assertEqual(dt, None)

        with self.assertRaises(TypeError):
            to_datetime(False)
            self.fail("Expected TypeError")

    def test_to_pd_timestamp(self):
        target = pd.Timestamp(year=2021, month=12, day=10, hour=14, minute=21, second=17, microsecond=123456, nanosecond=789)

        dt = _JDateTimeUtils.parseInstant("2021-12-10T14:21:17.123456789Z")
        dt = to_pd_timestamp(dt)
        self.assertEqual(dt, target)

        dt = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456789Z")
        dt = to_pd_timestamp(dt)
        self.assertEqual(dt, target)

        dt = to_pd_timestamp(None)
        self.assertEqual(dt, None)

        with self.assertRaises(TypeError):
            to_pd_timestamp(False)

    def test_to_np_datetime64(self):
        target = np.datetime64("2021-12-10T14:21:17.123456Z")

        dt = _JDateTimeUtils.parseInstant("2021-12-10T14:21:17.123456Z")
        dt = to_np_datetime64(dt)
        self.assertEqual(dt, target)

        dt = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456Z")
        dt = to_np_datetime64(dt)
        self.assertEqual(dt, target)

        dt = to_np_datetime64(None)
        self.assertEqual(dt, None)

        with self.assertRaises(TypeError):
            to_np_datetime64(False)
            self.fail("Expected TypeError")

    def test_to_timedelta(self):
        target = datetime.timedelta(hours=1, minutes=2, seconds=3, milliseconds=4, microseconds=5)

        d = _JDateTimeUtils.parseDuration("PT1H2M3.004005S")
        dt = to_timedelta(d)
        self.assertEqual(dt, target)

        target = datetime.timedelta(days=2)
        d = _JDateTimeUtils.parsePeriod("P2D")
        dt = to_timedelta(d)
        self.assertEqual(dt, target)

        target = datetime.timedelta(days=14)
        d = _JDateTimeUtils.parsePeriod("P2W")
        dt = to_timedelta(d)
        self.assertEqual(dt, target)

        d = to_timedelta(None)
        self.assertEqual(d, None)

        d = _JDateTimeUtils.parsePeriod("P1Y")
        with self.assertRaises(ValueError):
            to_timedelta(d)
            self.fail("Expected ValueError")

        d = _JDateTimeUtils.parsePeriod("P1M")
        with self.assertRaises(ValueError):
            to_timedelta(d)
            self.fail("Expected ValueError")

        with self.assertRaises(TypeError):
            to_timedelta(False)
            self.fail("Expected TypeError")

    def test_to_pd_timedelta(self):
        target = pd.Timedelta(hours=1, minutes=2, seconds=3, milliseconds=4, microseconds=5)

        d = _JDateTimeUtils.parseDuration("PT1H2M3.004005S")
        dt = to_pd_timedelta(d)
        self.assertEqual(dt, target)

        target = datetime.timedelta(days=2)
        d = _JDateTimeUtils.parsePeriod("P2D")
        dt = to_pd_timedelta(d)
        self.assertEqual(dt, target)

        target = datetime.timedelta(days=14)
        d = _JDateTimeUtils.parsePeriod("P2W")
        dt = to_pd_timedelta(d)
        self.assertEqual(dt, target)

        d = to_pd_timedelta(None)
        self.assertEqual(d, None)

        d = _JDateTimeUtils.parsePeriod("P1Y")
        with self.assertRaises(ValueError):
            to_pd_timedelta(d)
            self.fail("Expected ValueError")

        d = _JDateTimeUtils.parsePeriod("P1M")
        with self.assertRaises(ValueError):
            to_pd_timedelta(d)
            self.fail("Expected ValueError")

        with self.assertRaises(TypeError):
            to_pd_timedelta(False)
            self.fail("Expected TypeError")

    def test_to_np_timedelta64(self):
        target = np.timedelta64(1, 'h') + np.timedelta64(2, 'm') + np.timedelta64(3, 's') + np.timedelta64(4, 'ms') + np.timedelta64(5, 'us')

        d = _JDateTimeUtils.parseDuration("PT1H2M3.004005S")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(2, 'D')
        d = _JDateTimeUtils.parsePeriod("P2D")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(14, 'D')
        d = _JDateTimeUtils.parsePeriod("P2W")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        d = to_np_timedelta64(None)
        self.assertEqual(d, None)

        target = np.timedelta64(2, 'Y')
        d = _JDateTimeUtils.parsePeriod("P2Y")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(2, 'M')
        d = _JDateTimeUtils.parsePeriod("P2M")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(0, 'D')
        d = _JDateTimeUtils.parsePeriod("P0M")
        dt = to_np_timedelta64(d)
        self.assertEqual(dt, target)

        d = _JDateTimeUtils.parsePeriod("P1Y1M")
        with self.assertRaises(ValueError):
            to_np_timedelta64(d)
            self.fail("Expected ValueError")

        d = _JDateTimeUtils.parsePeriod("P1Y1D")
        with self.assertRaises(ValueError):
            to_np_timedelta64(d)
            self.fail("Expected ValueError")

        d = _JDateTimeUtils.parsePeriod("P1M1D")
        with self.assertRaises(ValueError):
            to_np_timedelta64(d)
            self.fail("Expected ValueError")

        with self.assertRaises(TypeError):
            to_np_timedelta64(False)
            self.fail("Expected TypeError")

    # endregion

    # region: Utilities

    def test_simple_date_format(self):
        s = "12/10/2021 14:21:17 CST"
        i = _JDateTimeUtils.parseInstant("2021-12-10T14:21:17 CT")
        sdf = simple_date_format("MM/dd/yyyy HH:mm:ss z")
        self.assertEqual(sdf.parse(s).toInstant(), i)

        with self.assertRaises(DHError):
            simple_date_format("junk")
            self.fail("Expected DHError")

    # endregion


if __name__ == "__main__":
    unittest.main()
