#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#

""" This module defines functions for working with business calendars. """

from typing import Optional, Union, List

import jpy

from deephaven import DHError


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


def add_calendar(cal: Optional[jpy.JType] = None, file: Optional[str] = None) -> None:
    """ Adds a new business calendar to the set of available options.

    Args:
        cal (jpy.JType): business calendar
        file (str): business calendar file

    Raises:
        DHError
    """

    if cal is None and file is None:
        raise DHError("either cal or file must be specified")
    elif cal is not None and file is not None:
        raise DHError("only one of cal or file may be specified")
    elif cal is not None:
        try:
            _JCalendars.addCalendar(cal)
        except Exception as e:
            raise DHError(e, f"failed to add calendar") from e
    elif file is not None:
        try:
            _JCalendars.addCalendarFromFile(file)
        except Exception as e:
            raise DHError(e, f"failed to add calendar from file '{file}'") from e
    else:
        raise DHError(f"unexpected error: cal={cal} file={file}")


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
        a list of names of all available calendar names

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


def calendar(name: Optional[str] = None) -> jpy.JType:
    """ Returns the calendar with the given name.

    The returned calendar is a 'io.deephaven.time.calendar.BusinessCalendar' Java object that can be used in Python.
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



#TODO: to_numpy_calendar method