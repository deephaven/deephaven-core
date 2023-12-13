#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for working with business calendars. """

from typing import Optional, Union, List

import jpy

from deephaven import DHError
from deephaven.dtypes import BusinessCalendar


_JCalendars = jpy.get_type("io.deephaven.time.calendar.Calendars")


def remove_calendar(name: str) -> None:
    """ Removes the calendar with the given name from the set of available options.

    Args:
        name (str): the name of the calendar to remove

    Raises:
        DHError
    """
    try:
        _JCalendars.removeCalendar(name)
    except Exception as e:
        raise DHError(e, f"failed to remove calendar '{name}'") from e


def add_calendar(cal: Union[BusinessCalendar, str]) -> None:
    """ Adds a new business calendar to the set of available options.

    Args:
        cal (Union[BusinessCalendar, str]): business calendar or a path to a business calendar file

    Raises:
        DHError
    """

    if cal is None:
        raise DHError(message="cal must be specified")
    elif isinstance(cal, str):
        try:
            _JCalendars.addCalendarFromFile(cal)
        except Exception as e:
            raise DHError(e, f"failed to add calendar from file '{cal}'") from e
    else:
        try:
            _JCalendars.addCalendar(cal)
        except Exception as e:
            raise DHError(e, f"failed to add calendar") from e


def set_calendar(name: str) -> None:
    """ Sets the default calendar.

    Args:
        name (str): the name of the calendar

    Raises:
        DHError
    """
    try:
        _JCalendars.setCalendar(name)
    except Exception as e:
        raise DHError(e, f"failed to set the default calendar name to '{name}'") from e


def calendar_names() -> List[str]:
    """ Returns the names of all available calendar names.

    Returns:
        a list of all available calendar names

    Raises:
        DHError
    """
    try:
        return list(_JCalendars.calendarNames())
    except Exception as e:
        raise DHError(e, "failed to obtain the available calendar names.") from e


def calendar_name() -> str:
    """ Returns the default business calendar name.

    The default business calendar is set by the 'Calendar.default' property or by calling 'set_calendar'.

    Returns:
        the default business calendar name

    Raises:
        DHError
    """
    try:
        return _JCalendars.calendarName()
    except Exception as e:
        raise DHError(e, "failed to get the default calendar name.") from e


def calendar(name: Optional[str] = None) -> BusinessCalendar:
    """ Returns the calendar with the given name.

    The returned calendar is an 'io.deephaven.time.calendar.BusinessCalendar' Java object that can be used in Python.
    For details on the available methods, see https://deephaven.io/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html.
    These methods must be called with string arguments or with Java date-time objects.  To convert Python date-time
    objects to Java date-time objects, use the conversion functions in the 'deephaven.time' module.

    Args:
        name (str): the name of the calendar.  If not specified, the default calendar is returned.

    Returns:
        the calendar with the given name or the defalt calendar if name is not specified.

    Raises:
        DHError
    """
    try:
        if name is None:
            return _JCalendars.calendar()
        else:
            return _JCalendars.calendar(name)
    except Exception as e:
        raise DHError(e, "failed to get the default calendar.") from e


