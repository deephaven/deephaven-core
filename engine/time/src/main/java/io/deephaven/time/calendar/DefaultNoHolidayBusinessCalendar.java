package io.deephaven.time.calendar;

import io.deephaven.time.DateTime;
import io.deephaven.time.TimeZone;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * A {@link BusinessCalendar} with no non-business days.
 */
public class DefaultNoHolidayBusinessCalendar extends AbstractBusinessCalendar {

    private final BusinessCalendar calendar;

    /**
     * Creates a new Default24HourBusinessCalendar instance. Assumes that {@code calendar} is a {@link BusinessCalendar}
     * with no holidays and a 24 hour business day.
     *
     * @param calendar {@link BusinessCalendar} with no holidays and a 24 hour business day
     */
    protected DefaultNoHolidayBusinessCalendar(final BusinessCalendar calendar) {
        this.calendar = calendar;
    }

    @Override
    public List<String> getDefaultBusinessPeriods() {
        return calendar.getDefaultBusinessPeriods();
    }

    @Override
    public Map<LocalDate, BusinessSchedule> getHolidays() {
        return calendar.getHolidays();
    }

    @Override
    public boolean isBusinessDay(DayOfWeek day) {
        return true;
    }

    @Override
    public String name() {
        return calendar.name();
    }

    @Override
    public TimeZone timeZone() {
        return calendar.timeZone();
    }

    @Override
    public long standardBusinessDayLengthNanos() {
        return calendar.standardBusinessDayLengthNanos();
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final DateTime time) {
        return calendar.getBusinessDay(time);
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final String date) {
        return calendar.getBusinessDay(date);
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final LocalDate date) {
        return calendar.getBusinessDay(date);
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final DateTime time) {
        return calendar.getBusinessSchedule(time);
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final String date) {
        return calendar.getBusinessSchedule(date);
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final LocalDate date) {
        return calendar.getBusinessSchedule(date);
    }

    @Override
    public long diffBusinessNanos(final DateTime start, final DateTime end) {
        return calendar.diffBusinessNanos(start, end);
    }

    @Override
    public double diffBusinessYear(final DateTime startTime, final DateTime endTime) {
        return calendar.diffBusinessYear(startTime, endTime);
    }

    @Override
    public String toString() {
        return calendar.toString();
    }

    @Override
    public boolean equals(Object obj) {
        return calendar.equals(obj);
    }

    @Override
    public int hashCode() {
        return calendar.hashCode();
    }

    ////////////////////////////////// NonBusinessDay methods //////////////////////////////////
    @Override
    public String previousNonBusinessDay() {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(DateTime time) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(DateTime time, int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(String date) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(String date, int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay() {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(DateTime time) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(DateTime time, int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(String date) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(String date, int days) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String[] nonBusinessDaysInRange(DateTime start, DateTime end) {
        return new String[0];
    }

    @Override
    public String[] nonBusinessDaysInRange(String start, String end) {
        return new String[0];
    }

    @Override
    public long diffNonBusinessNanos(DateTime start, DateTime end) {
        return 0;
    }

    @Override
    public double diffNonBusinessDay(DateTime start, DateTime end) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(DateTime start, DateTime end) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(DateTime start, DateTime end, boolean endInclusive) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(String start, String end) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(String start, String end, boolean endInclusive) {
        return 0;
    }
}
