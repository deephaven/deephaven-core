#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

import unittest

from deephaven2 import DHError
from deephaven2.calendars import calendar_names, default_calendar_name, Calendar
from tests.testbase import BaseTestCase


class CalendarsTestCase(BaseTestCase):
    def test_calendars(self):
        with self.subTest(msg="calendarNames() test"):
            names = calendar_names()
            self.assertTrue(isinstance(names, list) and len(names) > 0)

        with self.subTest(msg="getDefaultName() test"):
            name = default_calendar_name()
            self.assertIsNotNone(name)

        with self.subTest(msg="calendar() construction"):
            self.assertIsNotNone(Calendar())

        with self.subTest(msg="calendar(invalid name) construction"):
            self.assertRaises(DHError, Calendar, "garbage_name")

        with self.subTest(msg="calendar(valid name) construction"):
            self.assertIsNotNone(Calendar(name))

    def test_calendar(self):
        default_calendar = Calendar()
        self.assertIsNotNone(default_calendar.name)
        self.assertIsNotNone(default_calendar.current_day)
        self.assertIsNotNone(default_calendar.previous_day)
        self.assertIsNotNone(default_calendar.next_day)
        self.assertIsNotNone(default_calendar.day_of_week.name)
        self.assertIsNotNone(default_calendar.is_business_day)


if __name__ == '__main__':
    unittest.main()
