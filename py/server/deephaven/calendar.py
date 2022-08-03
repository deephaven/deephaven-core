#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides a collection of business calendars and convenient calendar functions. """

from enum import Enum
from typing import List

import jpy

from deephaven._wrapper import JObjectWrapper
from deephaven.time import TimeZone, DateTime

from deephaven import DHError

_JCalendars = jpy.get_type("io.deephaven.time.calendar.Calendars")
_JCalendar = jpy.get_type("io.deephaven.time.calendar.Calendar")
_JDayOfWeek = jpy.get_type("java.time.DayOfWeek")
_JBusinessPeriod = jpy.get_type("io.deephaven.time.calendar.BusinessPeriod")
_JBusinessSchedule = jpy.get_type("io.deephaven.time.calendar.BusinessSchedule")
_JBusinessCalendar = jpy.get_type("io.deephaven.time.calendar.BusinessCalendar")


def calendar_names() -> List[str]:
    """ Returns the names of all available calendars.

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
    """ Returns the default calendar name which is set by the 'Calendar.default' property in the configuration file
    that the Deephaven server is started with.

    Returns:
        the default business calendar name

    Raises:
        DHError
    """
    try:
        return _JCalendars.getDefaultName()
    except Exception as e:
        raise DHError(e, "failed to get the default calendar name.") from e


class DayOfWeek(Enum):
    """ A Enum that defines the days of a week. """
    MONDAY = _JDayOfWeek.MONDAY
    TUESDAY = _JDayOfWeek.TUESDAY
    WEDNESDAY = _JDayOfWeek.WEDNESDAY
    THURSDAY = _JDayOfWeek.THURSDAY
    FRIDAY = _JDayOfWeek.FRIDAY
    SATURDAY = _JDayOfWeek.SATURDAY
    SUNDAY = _JDayOfWeek.SUNDAY


class BusinessPeriod(JObjectWrapper):
    """ A period of business time during a business day. """

    j_object_type = _JBusinessPeriod

    def __init__(self, j_business_period):
        self.j_business_period = j_business_period

    def __repr__(self):
        return f"[{self.start_time}, {self.end_time}]"

    def __str__(self):
        return repr(self)

    @property
    def j_object(self) -> jpy.JType:
        return self.j_business_period

    @property
    def start_time(self) -> DateTime:
        """ The start of the period. """
        return self.j_business_period.getStartTime()

    @property
    def end_time(self) -> DateTime:
        """ The end of the period. """
        return self.j_business_period.getEndTime()

    @property
    def length(self) -> int:
        """ The length of the period in nanoseconds. """
        return self.j_business_period.getLength()


class BusinessSchedule(JObjectWrapper):
    """ A business schedule defines a single business day. """
    j_object_type = _JBusinessSchedule

    def __init__(self, j_business_schedule):
        self.j_business_schedule = j_business_schedule

    @property
    def j_object(self) -> jpy.JType:
        return self.j_business_schedule

    @property
    def business_periods(self) -> List[BusinessPeriod]:
        """ The business periods of the day.

        Returns:
             List[BusinessPeriod]
        """
        j_periods = self.j_business_schedule.getBusinessPeriods()
        periods = []
        for j_period in j_periods:
            periods.append(BusinessPeriod(j_period))
        return periods

    @property
    def start_of_day(self) -> DateTime:
        """ The start of the business day. """
        return self.j_business_schedule.getStartOfBusinessDay()

    @property
    def end_of_day(self) -> DateTime:
        """ The end of the business day. """
        return self.j_business_schedule.getEndOfBusinessDay()

    def is_business_day(self) -> bool:
        """ Whether it is a business day.

        Returns:
            bool
        """
        return self.j_business_schedule.isBusinessDay()

    def is_business_time(self, time: DateTime) -> bool:
        """ Whether the specified time is a business time for the day.

        Args:
             time (DateTime): the time during the day

        Return:
            bool
        """
        return self.j_business_schedule.isBusinessTime(time)

    def business_time_elapsed(self, time: DateTime) -> int:
        """ Returns the amount of business time in nanoseconds that has elapsed on the given day by the specified
        time.

        Args:
            time (DateTime): the time during the day

        Returns:
            int
        """
        return self.j_business_schedule.businessTimeElapsed(time)


class Calendar(JObjectWrapper):
    j_object_type = _JCalendar

    @property
    def j_object(self) -> jpy.JType:
        return self.j_calendar

    @property
    def current_day(self) -> str:
        """ The current day. """
        return self.j_calendar.currentDay()

    @property
    def time_zone(self) -> TimeZone:
        """ Returns the timezone of the calendar. """
        return TimeZone(self.j_calendar.timeZone())

    def previous_day(self, date: str) -> str:
        """ Gets the day prior to the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.previousDay(date)
        except Exception as e:
            raise DHError(e, "failed in previous_day.") from e

    def next_day(self, date: str) -> str:
        """ Gets the day after the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.nextDay(date)
        except Exception as e:
            raise DHError(e, "failed in next_day.") from e

    def day_of_week(self, date: str) -> DayOfWeek:
        """ The day of week for the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return DayOfWeek(self.j_calendar.dayOfWeek(date))
        except Exception as e:
            raise DHError(e, "failed in day_of_week.") from e

    def days_in_range(self, start: str, end: str) -> List[str]:
        """ Returns the days between the specified start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range

        Returns:
            List[str]: a list of dates

        Raises:
            DHError
        """
        try:
            j_days = self.j_calendar.daysInRange(start, end)
            return list(j_days)
        except Exception as e:
            raise DHError(e, "failed in days_in_range.") from e

    def number_of_days(self, start: str, end: str, end_inclusive: bool = False) -> int:
        """ Returns the number of days between the start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range
            end_inclusive (bool): whether to include the end date, default is False

        Returns:
            int

        Raises:
            DHError
        """
        try:
            return self.j_calendar.numberOfDays(start, end, end_inclusive)
        except Exception as e:
            raise DHError(e, "failed in number_of_days.") from e


class BusinessCalendar(Calendar):
    """ A business calendar. """

    j_object_type = _JBusinessCalendar

    def __init__(self, name: str = None):
        """ Loads a business calendar.

        Args:
            name (str) : name of the calendar, default is None, which means to use the default Deephaven calendar

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
            raise DHError(e, "failed to load a business calendar.") from e

    @property
    def is_business_day(self) -> bool:
        """ If today is a business day. """
        return self.j_calendar.isBusinessDay()

    @property
    def default_business_periods(self) -> List[str]:
        """ The default business periods for the business days. Returns a list of strings with a comma separating
        open and close times. """

        default_business_periods = self.j_calendar.getDefaultBusinessPeriods()
        return list(default_business_periods.toArray())

    @property
    def standard_business_day_length(self) -> int:
        """ The length of a standard business day in nanoseconds. """
        return self.j_calendar.standardBusinessDayLengthNanos()

    def business_schedule(self, date: str) -> BusinessSchedule:
        """ Returns the specified day's business schedule.

        Args:
             date (str): the date str, format must be "yyyy-MM-dd"

        Returns:
            a BusinessSchedule instance

        Raises:
            DHError
        """
        try:
            return BusinessSchedule(j_business_schedule=self.j_calendar.getBusinessSchedule(date))
        except Exception as e:
            raise DHError(e, "failed in get_business_schedule.") from e

    def previous_business_day(self, date: str) -> str:
        """ Gets the business day prior to the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.previousBusinessDay(date)
        except Exception as e:
            raise DHError(e, "failed in previous_business_day.") from e

    def previous_non_business_day(self, date: str) -> str:
        """ Gets the non-business day prior to the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.previousNonBusinessDay(date)
        except Exception as e:
            raise DHError(e, "failed in previous_non_business_day.") from e

    def next_business_day(self, date: str) -> str:
        """ Gets the business day after the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.nextBusinessDay(date)
        except Exception as e:
            raise DHError(e, "failed in next_business_day.") from e

    def next_non_business_day(self, date: str) -> str:
        """ Gets the non-business day after the given date.

        Args:
            date (str): the date of interest

        Returns:
            str

        Raises:
            DHError
        """
        try:
            return self.j_calendar.nextNonBusinessDay(date)
        except Exception as e:
            raise DHError(e, "failed in next_non_business_day.") from e

    def business_days_in_range(self, start: str, end: str) -> List[str]:
        """ Returns the business days between the specified start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range

        Returns:
            List[str]: a list of dates

        Raises:
            DHError
        """
        try:
            j_days = self.j_calendar.businessDaysInRange(start, end)
            return list(j_days)
        except Exception as e:
            raise DHError(e, "failed in business_days_in_range.") from e

    def non_business_days_in_range(self, start: str, end: str) -> List[str]:
        """ Returns the non-business days between the specified start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range

        Returns:
            List[str]: a list of dates

        Raises:
            DHError
        """
        try:
            j_days = self.j_calendar.nonBusinessDaysInRange(start, end)
            return list(j_days)
        except Exception as e:
            raise DHError(e, "failed in non_business_days_in_range.") from e

    def number_of_business_days(self, start: str, end: str, end_inclusive: bool = False) -> int:
        """ Returns the number of business days between the start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range
            end_inclusive (bool): whether to include the end date, default is False

        Returns:
            int

        Raises:
            DHError
        """
        try:
            return self.j_calendar.numberOfBusinessDays(start, end, end_inclusive)
        except Exception as e:
            raise DHError(e, "failed in number_of_business_days.") from e

    def number_of_non_business_days(self, start: str, end: str, end_inclusive: bool = False) -> int:
        """ Returns the number of non-business days between the start and end dates.

        Args:
            start (str): the start day of the range
            end (str): the end day of the range
            end_inclusive (bool): whether to include the end date, default is False

        Returns:
            int

        Raises:
            DHError
        """
        try:
            return self.j_calendar.numberOfNonBusinessDays(start, end, end_inclusive)
        except Exception as e:
            raise DHError(e, "failed in number_of_non_business_days.") from e

    def is_last_business_day_of_month(self, date: str) -> bool:
        """ Returns if the specified date is the last business day of the month.

        Args:
            date (str): the date

        Returns:
            bool

        Raises:
            DHError
        """
        try:
            return self.j_calendar.isLastBusinessDayOfMonth(date)
        except Exception as e:
            raise DHError(e, "failed in is_last_business_day_of_month.") from e

    def is_last_business_day_of_week(self, date: str) -> bool:
        """ Returns if the specified date is the last business day of the week.

        Args:
            date (str): the date

        Returns:
            bool

        Raises:
            DHError
        """
        try:
            return self.j_calendar.isLastBusinessDayOfWeek(date)
        except Exception as e:
            raise DHError(e, "failed in is_last_business_day_of_week.") from e
