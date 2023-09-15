#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven import DHError
from deephaven.calendar import calendar_names, default_calendar_name, BusinessCalendar, DayOfWeek
from tests.testbase import BaseTestCase

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

class CalendarTestCase(BaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_calendar = BusinessCalendar("USNYSE")
        self.b_day1 = "2022-01-03"
        self.b_day = "2022-03-08"
        self.prev_b_day = "2022-03-07"
        self.prev_nb_day = "2022-03-06"
        self.next_b_day = "2022-03-09"
        self.next_nb_day = "2022-03-12"
        self.last_b_day_month = "2022-03-31"
        self.last_b_day_week = "2022-03-11"

    def tearDown(self) -> None:
        super().tearDown()

    def test_calendar(self):
        with self.subTest(msg="calendarNames() test"):
            cal_names = calendar_names()
            self.assertTrue(isinstance(cal_names, list) and len(cal_names) > 0)

        with self.subTest(msg="getDefaultName() test"):
            def_cal_name = default_calendar_name()
            self.assertIsNotNone(def_cal_name)

        with self.subTest(msg="calendar() construction"):
            self.assertIsNotNone(BusinessCalendar())

        with self.subTest(msg="calendar(invalid name) construction"):
            self.assertRaises(DHError, BusinessCalendar, "garbage_name")

        with self.subTest(msg="calendar(valid name) construction"):
            self.assertIsNotNone(BusinessCalendar(cal_names[0]))

        default_calendar = BusinessCalendar()
        current_date = default_calendar.current_day
        self.assertIsNotNone(default_calendar.previous_day(current_date))
        self.assertIsNotNone(default_calendar.next_day(current_date))
        self.assertIsNotNone(default_calendar.day_of_week(current_date).name)
        self.assertEqual(default_calendar.time_zone, _JDateTimeUtils.timeZone("America/New_York"))

        self.assertEqual(self.test_calendar.previous_day(self.b_day), self.prev_b_day)
        self.assertEqual(self.test_calendar.next_day(self.b_day), self.next_b_day)
        self.assertEqual(self.test_calendar.days_in_range(self.prev_nb_day, self.b_day),
                         [self.prev_nb_day, self.test_calendar.next_day(self.prev_nb_day), self.b_day])
        self.assertEqual(self.test_calendar.number_of_days(self.prev_nb_day, self.next_b_day), 3)
        self.assertEqual(self.test_calendar.day_of_week(self.b_day), DayOfWeek.TUESDAY)

        bad_date = "2022-A1-03"
        with self.assertRaises(DHError) as cm:
            self.assertFalse(self.test_calendar.is_last_business_day_of_month(bad_date))

    def test_business_period(self):
        b_periods = self.test_calendar.business_schedule(self.b_day1).business_periods
        self.assertEqual(len(b_periods), 1)
        p = b_periods[0]
        s = _JDateTimeUtils.formatDateTime(p.start_time, _JDateTimeUtils.timeZone('America/New_York'))
        self.assertEqual(p.start_time, _JDateTimeUtils.parseInstant(s))
        s = _JDateTimeUtils.formatDateTime(p.end_time, _JDateTimeUtils.timeZone('America/New_York'))
        self.assertEqual(p.end_time, _JDateTimeUtils.parseInstant(s))
        self.assertEqual(p.length, 6.5 * 60 * 60 * 10 ** 9)

    def test_business_schedule_business_day(self):
        b_schedule = self.test_calendar.business_schedule(self.prev_nb_day)
        self.assertFalse(b_schedule.is_business_day())

        b_schedule = self.test_calendar.business_schedule(self.b_day)
        self.assertTrue(b_schedule.is_business_day())

        b_period = b_schedule.business_periods[0]
        self.assertEqual(b_period.start_time, b_schedule.start_of_day)
        self.assertEqual(b_period.end_time, b_schedule.end_of_day)

        self.assertTrue(b_schedule.is_business_time(b_period.start_time))
        self.assertTrue(b_schedule.is_business_time(b_period.end_time))
        non_b_time = _JDateTimeUtils.minus(b_schedule.start_of_day, 1)
        self.assertFalse(b_schedule.is_business_time(non_b_time))
        non_b_time = _JDateTimeUtils.plus(b_schedule.end_of_day, 1)
        self.assertFalse(b_schedule.is_business_time(non_b_time))

        b_time = _JDateTimeUtils.plus(b_schedule.start_of_day, 10 * 10 ** 9)
        self.assertEqual(10 * 10 ** 9, b_schedule.business_time_elapsed(b_time))
        b_time = _JDateTimeUtils.plus(b_schedule.end_of_day, 10 * 10 ** 9)
        self.assertEqual(b_period.length, b_schedule.business_time_elapsed(b_time))

    def test_business_calendar(self):
        default_calendar = BusinessCalendar()

        self.assertIn(default_calendar.is_business_day, {True, False})
        self.assertEqual(self.test_calendar.default_business_periods, ["09:30,16:00"])
        self.assertEqual(self.test_calendar.standard_business_day_length, 6.5 * 60 * 60 * 10 ** 9)
        self.assertEqual(self.test_calendar.previous_business_day(self.b_day), self.prev_b_day)
        self.assertEqual(self.test_calendar.previous_non_business_day(self.b_day), self.prev_nb_day)
        self.assertEqual(self.test_calendar.next_business_day(self.b_day), self.next_b_day)
        self.assertEqual(self.test_calendar.next_non_business_day(self.b_day), self.next_nb_day)

        self.assertEqual(self.test_calendar.business_days_in_range(self.prev_nb_day, self.b_day),
                         [self.prev_b_day, self.b_day])
        self.assertEqual(self.test_calendar.non_business_days_in_range(self.prev_nb_day, self.b_day),
                         [self.prev_nb_day])

        self.assertEqual(self.test_calendar.number_of_business_days(self.prev_nb_day, self.next_b_day), 2)
        self.assertEqual(
            self.test_calendar.number_of_business_days(self.prev_nb_day, self.next_b_day, end_inclusive=True), 3)
        self.assertEqual(self.test_calendar.number_of_non_business_days(self.prev_nb_day, self.next_nb_day), 1)
        self.assertEqual(
            self.test_calendar.number_of_non_business_days(self.prev_nb_day, self.next_nb_day, end_inclusive=True), 2)

        self.assertFalse(self.test_calendar.is_last_business_day_of_month(self.b_day))
        self.assertFalse(self.test_calendar.is_last_business_day_of_week(self.b_day))
        self.assertTrue(self.test_calendar.is_last_business_day_of_month(self.last_b_day_month))
        self.assertTrue(self.test_calendar.is_last_business_day_of_week(self.last_b_day_week))

        bad_date = "2022-A1-03"
        with self.assertRaises(DHError) as cm:
            self.assertFalse(self.test_calendar.is_last_business_day_of_month(bad_date))

        self.assertIn("RuntimeError", cm.exception.root_cause)

    def test_business_schedule_non_business_day(self):
        default_calendar = BusinessCalendar()
        business_schedule = default_calendar.business_schedule(self.next_nb_day)
        self.assertEqual(business_schedule.business_periods, [])
        self.assertFalse(business_schedule.is_business_day())


if __name__ == '__main__':
    unittest.main()
