#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys

import deephaven.Calendars as Calendars

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestCalendars(unittest.TestCase):
    """
    Test cases for the deephaven.Calendars module (performed locally) -
    """

    def testCalendar(self):
        """
        Coverage test of Calendar module methods
        """

        with self.subTest(msg="calendarNames() test"):
            names = Calendars.calendarNames()  # should at least return default name...
            self.assertTrue(isinstance(names, list) and len(names) > 0)
            print("calendarNames() values {}".format(names))

        name = None
        with self.subTest(msg="getDefaultName() test"):
            name = Calendars.getDefaultName()
            self.assertIsNotNone(name)  # just in case?

        with self.subTest(msg="calendar() construction"):
            junk = Calendars.calendar()  # should return the default calendar instance?
            self.assertIsNotNone(junk)  # just in case?

        with self.subTest(msg="calendar(invalid name) construction"):
            self.assertRaises(RuntimeError, Calendars.calendar, "garbage_name")

        with self.subTest(msg="calendar(valid name) construction"):
            junk = Calendars.calendar(name)  # NB: this will fail if getDefaultName() does
            self.assertIsNotNone(junk)  # just in case?

