package io.deephaven.util.calendar;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeZone;

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
     * Creates a new Default24HourBusinessCalendar instance. Assumes that {@code calendar} is a
     * {@link BusinessCalendar} with no holidays and a 24 hour business day.
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
    public DBTimeZone timeZone() {
        return calendar.timeZone();
    }

    @Override
    public long standardBusinessDayLengthNanos() {
        return calendar.standardBusinessDayLengthNanos();
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final DBDateTime time) {
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
    public BusinessSchedule getBusinessSchedule(final DBDateTime time) {
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
    public long diffBusinessNanos(final DBDateTime start, final DBDateTime end) {
        return calendar.diffBusinessNanos(start, end);
    }

    @Override
    public double diffBusinessYear(final DBDateTime startTime, final DBDateTime endTime) {
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
    public String previousNonBusinessDay(DBDateTime time) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String previousNonBusinessDay(DBDateTime time, int days) {
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
    public String nextNonBusinessDay(DBDateTime time) {
        throw new UnsupportedOperationException("Calendar has no non-business days.");
    }

    @Override
    public String nextNonBusinessDay(DBDateTime time, int days) {
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
    public String[] nonBusinessDaysInRange(DBDateTime start, DBDateTime end) {
        return new String[0];
    }

    @Override
    public String[] nonBusinessDaysInRange(String start, String end) {
        return new String[0];
    }

    @Override
    public long diffNonBusinessNanos(DBDateTime start, DBDateTime end) {
        return 0;
    }

    @Override
    public double diffNonBusinessDay(DBDateTime start, DBDateTime end) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(DBDateTime start, DBDateTime end) {
        return 0;
    }

    @Override
    public int numberOfNonBusinessDays(DBDateTime start, DBDateTime end, boolean endInclusive) {
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
