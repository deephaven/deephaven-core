#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

import unittest

import jpy

from deephaven import DHError, new_table
from deephaven.calendar import (
    add_calendar,
    calendar,
    calendar_name,
    calendar_names,
    remove_calendar,
    set_calendar,
)
from deephaven.column import bool_col, string_col
from tests.testbase import BaseTestCase, get_resource_path

_JDateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")
_JCalendars = jpy.get_type("io.deephaven.time.calendar.Calendars")

_calendars_registered = False


def _register_config_calendars() -> None:
    """Registers the calendars from the configuration once per JVM. Registering them again fails, because
    Calendars.addCalendar rejects a second calendar with the same name."""
    global _calendars_registered
    if not _calendars_registered:
        cals = _JCalendars.calendarsFromConfiguration()
        for i in range(cals.size()):
            _JCalendars.addCalendar(cals.get(i))
        _calendars_registered = True


class CalendarTestCase(BaseTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        _register_config_calendars()

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    def test_add_remove_calendar(self):
        path1 = get_resource_path("/TEST1.calendar")
        path2 = get_resource_path("/TEST2.calendar")

        cal = jpy.get_type(
            "io.deephaven.time.calendar.BusinessCalendarXMLParser"
        ).loadBusinessCalendar(path2)

        with self.assertRaises(DHError) as cm:
            add_calendar(None)

        add_calendar(path1)
        self.assertIn("TEST1", calendar_names())

        add_calendar(cal)
        self.assertIn("TEST2", calendar_names())

        # Testing calendar removal here so that the global calendar state is not affected by the test

        remove_calendar("TEST1")
        remove_calendar("TEST2")

        # Make sure the calendars are gone

        with self.assertRaises(DHError) as cm:
            calendar("TEST1")

        with self.assertRaises(DHError) as cm:
            calendar("TEST2")

    def test_set_calendar(self):
        default = calendar_name()

        try:
            new_default = "CAL1"
            self.assertNotEqual(calendar_name(), new_default)
            set_calendar(new_default)
            self.assertEqual(calendar_name(), new_default)
        finally:
            set_calendar(default)

    def test_calendar_names(self):
        self.assertEqual(
            calendar_names(),
            ["CAL1", "CAL2", "USBANK_EXAMPLE", "USNYSE_EXAMPLE", "UTC"],
        )

    def test_calendar(self):
        self.assertEqual("USNYSE_EXAMPLE", calendar("USNYSE_EXAMPLE").name())
        self.assertEqual("CAL1", calendar("CAL1").name())
        self.assertEqual(calendar_name(), calendar().name())

        with self.assertRaises(DHError) as cm:
            calendar("JUNK_NAME")

    # A simple integration test to make sure the calendar is working
    def test_integration(self):
        default = calendar_name()
        # Wed / Thurs / Fri / Sat / Sun / Mon
        t = new_table(
            [
                string_col(
                    "Date",
                    [
                        "2020-01-01",
                        "2020-01-02",
                        "2020-01-03",
                        "2020-01-04",
                        "2020-01-05",
                        "2020-01-06",
                    ],
                ),
                bool_col("TARGET_NYSE", [False, True, True, False, False, True]),
                bool_col("TARGET_UTC", [True, True, True, True, True, True]),
            ]
        )

        try:
            set_calendar("USNYSE_EXAMPLE")
            cal = calendar("USNYSE_EXAMPLE")
            t2 = t.update(
                [
                    "EQUAL_NYSE_1 = isBusinessDay(Date) == TARGET_NYSE",
                    "EQUAL_NYSE_2 = cal.isBusinessDay(Date) == TARGET_NYSE",
                    "EQUAL_NYSE_3 = calendar(`USNYSE_EXAMPLE`).isBusinessDay(Date) == TARGET_NYSE",
                    "EQUAL_UTC = calendar(`UTC`).isBusinessDay(Date) == TARGET_UTC",
                ]
            )
            self.assertEqual(t2.where("!EQUAL_NYSE_1").size, 0)
            self.assertEqual(t2.where("!EQUAL_NYSE_2").size, 0)
            self.assertEqual(t2.where("!EQUAL_NYSE_3").size, 0)
            self.assertEqual(t2.where("!EQUAL_UTC").size, 0)
        finally:
            set_calendar(default)


if __name__ == "__main__":
    unittest.main()
