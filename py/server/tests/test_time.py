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

        try:
            to_j_local_date(123)
            self.fail("Expected DHError")
        except DHError:
            pass


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

        try:
            to_j_local_time(123)
            self.fail("Expected DHError")
        except DHError:
            pass

    def test_to_j_instant(self):
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T19:21:17.123456Z")

        dt = to_j_instant("2021-12-10T14:21:17.123456 ET")
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

        dt = to_j_instant(None)
        self.assertEqual(dt, None)

        try:
            to_j_instant(123)
            self.fail("Expected DHError")
        except DHError:
            pass


    def test_to_j_zdt(self):
        target = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456-05:00[America/New_York]")

        dt = to_j_zdt("2021-12-10T14:21:17.123456 ET")
        self.assertEqual(str(target), str(dt))

        x = datetime.datetime(2021, 12, 10, 14, 21, 17, 123456)
        dt = to_j_zdt(x)
        self.assertEqual(str(target), str(dt))

        x = np.datetime64(x)
        dt = to_j_zdt(x)
        self.assertEqual(str(target), str(dt))

        dt = to_j_zdt(None)
        self.assertEqual(dt, None)

        try:
            to_j_zdt(123)
            self.fail("Expected DHError")
        except DHError:
            pass


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

        try:
            to_j_duration(123)
            self.fail("Expected DHError")
        except DHError:
            pass


    def test_to_j_period(self):
        p = to_j_period("P2W")
        self.assertEqual(str(p), "P14D")

        x = datetime.timedelta(days=2)
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

        try:
            to_j_period(123)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            x = datetime.timedelta(days=2, seconds=1)
            to_j_period(x)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            x = datetime.timedelta(days=2, microseconds=1)
            to_j_period(x)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            x = datetime.timedelta(days=2.3)
            to_j_period(x)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            x = np.timedelta64(2, 'h')
            to_j_period(x)
            self.fail("Expected DHError")
        except DHError:
            pass


    # endregion


    # region Conversions: Java To Python

    def test_to_date(self):
        target = datetime.date(2021, 12, 10)

        ld = _JDateTimeUtils.parseLocalDate("2021-12-10")
        dt = to_date(ld)
        self.assertEqual(dt, target)

        dt = to_date(None)
        self.assertEqual(dt, None)

        try:
            to_date(123)
            self.fail("Expected DHError")
        except DHError:
            pass

    def test_to_time(self):
        target = datetime.time(14, 21, 17, 123456)

        lt = _JDateTimeUtils.parseLocalTime("14:21:17.123456")
        dt = to_time(lt)
        self.assertEqual(dt, target)

        dt = to_time(None)
        self.assertEqual(dt, None)

        try:
            to_time(123)
            self.fail("Expected DHError")
        except DHError:
            pass

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

        try:
            to_datetime(123)
            self.fail("Expected DHError")
        except DHError:
            pass

    def test_to_datetime64(self):
        target = np.datetime64("2021-12-10T14:21:17.123456Z")

        dt = _JDateTimeUtils.parseInstant("2021-12-10T14:21:17.123456Z")
        dt = to_datetime64(dt)
        self.assertEqual(dt, target)

        dt = _JDateTimeUtils.parseZonedDateTime("2021-12-10T14:21:17.123456Z")
        dt = to_datetime64(dt)
        self.assertEqual(dt, target)

        dt = to_datetime64(None)
        self.assertEqual(dt, None)

        try:
            to_datetime64(123)
            self.fail("Expected DHError")
        except DHError:
            pass

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
        try:
            to_timedelta(d)
            self.fail("Expected DHError")
        except DHError:
            pass

        d = _JDateTimeUtils.parsePeriod("P1M")
        try:
            to_timedelta(d)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            to_timedelta(123)
            self.fail("Expected DHError")
        except DHError:
            pass

    def test_to_timedelta64(self):
        target = np.timedelta64(1, 'h') + np.timedelta64(2, 'm') + np.timedelta64(3, 's') + np.timedelta64(4, 'ms') + np.timedelta64(5, 'us')

        d = _JDateTimeUtils.parseDuration("PT1H2M3.004005S")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(2, 'D')
        d = _JDateTimeUtils.parsePeriod("P2D")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(14, 'D')
        d = _JDateTimeUtils.parsePeriod("P2W")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        d = to_timedelta64(None)
        self.assertEqual(d, None)

        target = np.timedelta64(2, 'Y')
        d = _JDateTimeUtils.parsePeriod("P2Y")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(2, 'M')
        d = _JDateTimeUtils.parsePeriod("P2M")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        target = np.timedelta64(0, 'D')
        d = _JDateTimeUtils.parsePeriod("P0M")
        dt = to_timedelta64(d)
        self.assertEqual(dt, target)

        d = _JDateTimeUtils.parsePeriod("P1Y1M")
        try:
            to_timedelta64(d)
            self.fail("Expected DHError")
        except DHError:
            pass

        d = _JDateTimeUtils.parsePeriod("P1Y1D")
        try:
            to_timedelta64(d)
            self.fail("Expected DHError")
        except DHError:
            pass

        d = _JDateTimeUtils.parsePeriod("P1M1D")
        try:
            to_timedelta64(d)
            self.fail("Expected DHError")
        except DHError:
            pass

        try:
            to_timedelta64(123)
            self.fail("Expected DHError")
        except DHError:
            pass

    # endregion


if __name__ == "__main__":
    unittest.main()
