#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" This module provides access to a collection of business calendars and basic calendar functions. """
from enum import Enum
from typing import List

import jpy

from deephaven2 import DHError
from deephaven2._wrapper_abc import JObjectWrapper

_JCalendars = jpy.get_type("io.deephaven.time.calendar.Calendars")
_JDayOfWeek = jpy.get_type("java.time.DayOfWeek")


class DayOfWeek(Enum):
    """ A Enum that defines the days of a week. """
    MONDAY = _JDayOfWeek.MONDAY
    TUESDAY = _JDayOfWeek.TUESDAY
    WEDNESDAY = _JDayOfWeek.WEDNESDAY
    THURSDAY = _JDayOfWeek.THURSDAY
    FRIDAY = _JDayOfWeek.FRIDAY
    SATURDAY = _JDayOfWeek.SATURDAY
    SUNDAY = _JDayOfWeek.SUNDAY


class Calendar(JObjectWrapper):
    """ This class wraps a Deephaven Business Calendar. """

    def __init__(self, name: str = None):
        """ Creates a business calendar.

        Args:
            name (str) : name of the calendar

        Raises:
            DHError
        """
        try:
            if name:
                self.name = name
                self.j_calendar = _JCalendars.calendar(name)
            else:
                self.name = default_calendar_name()
                self.j_calendar = _JCalendars.calendar()
        except Exception as e:
            raise DHError(e, "failed to create a business calendar.") from e

    @property
    def j_object(self) -> jpy.JType:
        return self.j_calendar

    @property
    def current_day(self) -> str:
        """ The current day. """
        return self.j_calendar.currentDay()

    @property
    def previous_day(self) -> str:
        """ The previous day. """
        return self.j_calendar.previousDay()

    @property
    def next_day(self) -> str:
        """ The next day. """
        return self.j_calendar.nextDay()

    @property
    def day_of_week(self) -> DayOfWeek:
        """ The day of week for today. """
        return DayOfWeek(self.j_calendar.dayOfWeek())

    @property
    def is_business_day(self) -> bool:
        """ If today is a business day. """
        return self.j_calendar.isBusinessDay()


def calendar_names() -> List[str]:
    """ Returns the names of all available calendars

    Returns:
        a list of names of all available calendars

    Raises:
        DHError
    """
    try:
        return list(_JCalendars.calendarNames())
    except Exception as e:
        raise DHError(e, "failed to obtain the available calendar names.") from e


def default_calendar_name():
    """ Returns the default calendar name

    Returns:
        the default business calendar name

    Raises:
        DHError
    """
    try:
        return _JCalendars.getDefaultName()
    except Exception as e:
        raise DHError(e, "failed to get the default calendar name.") from e
