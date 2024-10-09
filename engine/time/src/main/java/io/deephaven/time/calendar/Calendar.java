//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * A calendar.
 * <p>
 * A calendar is associated with a specific time zone.
 * <p>
 * Date strings must be in a format that can be parsed by {@link DateTimeUtils#parseLocalDate(String)}. Methods that
 * accept strings can be slower than methods written explicitly for {@link Instant}, {@link ZonedDateTime}, or
 * {@link LocalDate}.
 */
public class Calendar {

    private final String name;
    private final String description;
    private final ZoneId timeZone;

    // region Cache

    private static class SummaryData extends ReadOptimizedConcurrentCache.IntKeyedValue {
        final LocalDate startDate;
        final LocalDate endDate; // exclusive
        final List<LocalDate> dates;

        SummaryData(int key, LocalDate startDate, LocalDate endDate, List<LocalDate> dates) {
            super(key);
            this.startDate = startDate;
            this.endDate = endDate;
            this.dates = dates;
        }

    }

    private final YearMonthSummaryCache<SummaryData> summaryCache =
            new YearMonthSummaryCache<>(this::computeMonthSummary, this::computeYearSummary);

    private SummaryData summarize(final int key, final LocalDate startDate, final LocalDate endDate) {
        LocalDate date = startDate;
        final ArrayList<LocalDate> dates = new ArrayList<>();

        while (date.isBefore(endDate)) {
            dates.add(date);
            date = date.plusDays(1);
        }

        return new SummaryData(key, startDate, endDate, dates); // end date is exclusive
    }

    private SummaryData computeMonthSummary(final int yearMonth) {
        final int year = YearMonthSummaryCache.yearFromYearMonthKey(yearMonth);
        final int month = YearMonthSummaryCache.monthFromYearMonthKey(yearMonth);
        final LocalDate startDate = LocalDate.of(year, month, 1);
        final LocalDate endDate = startDate.plusMonths(1); // exclusive
        return summarize(yearMonth, startDate, endDate);
    }

    private SummaryData computeYearSummary(final int year) {
        LocalDate startDate = null;
        LocalDate endDate = null;
        ArrayList<LocalDate> dates = new ArrayList<>();

        for (int month = 1; month <= 12; month++) {
            SummaryData ms = summaryCache.getMonthSummary(year, month);
            if (month == 1) {
                startDate = ms.startDate;
            }

            if (month == 12) {
                endDate = ms.endDate;
            }

            dates.addAll(ms.dates);
        }

        return new SummaryData(year, startDate, endDate, dates);
    }

    // endregion

    // region Constructors

    /**
     * Creates a new calendar.
     *
     * @param name calendar name.
     * @param description calendar description.
     * @param timeZone calendar time zone.
     * @throws RequirementFailure if {@code name} or {@code timeZone} is {@code null}
     */
    Calendar(final String name, final String description, final ZoneId timeZone) {
        this.name = Require.neqNull(name, "name");
        this.description = description;
        this.timeZone = Require.neqNull(timeZone, "timeZone");
    }

    // endregion

    // region Cache

    /**
     * Clears the cache. This should not generally be used and is provided for benchmarking.
     */
    synchronized void clearCache() {
        summaryCache.clear();
    }

    // endregion

    // region Getters

    /**
     * Gets the name of the calendar.
     *
     * @return the name of the calendar
     */
    public String name() {
        return name;
    }

    /**
     * Gets the description of the calendar.
     *
     * @return the description of the calendar
     */
    public String description() {
        return description;
    }

    /**
     * Gets the timezone of the calendar.
     *
     * @return the time zone of the calendar
     */
    public ZoneId timeZone() {
        return timeZone;
    }

    @Override
    public String toString() {
        return "Calendar{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", timeZone=" + timeZone +
                '}';
    }

    // endregion

    // region Now

    /**
     * The current date for the calendar.
     *
     * @return current date
     */
    public LocalDate calendarDate() {
        return DateTimeUtils.todayLocalDate(timeZone());
    }

    /**
     * The current day of the week for the calendar.
     *
     * @return current day of the week
     */
    public DayOfWeek dayOfWeek() {
        return dayOfWeek(calendarDate());
    }

    /**
     * The current day of the week for the calendar.
     *
     * @param date date
     * @return current day of the week, or {@code null} if the input is {@code null}.
     */
    public DayOfWeek dayOfWeek(final LocalDate date) {
        return DateTimeUtils.dayOfWeek(date);
    }

    /**
     * The current day of the week for the calendar.
     *
     * @param date date
     * @return current day of the week, or {@code null} if the input is {@code null}.
     */
    public DayOfWeek dayOfWeek(final String date) {
        if (date == null) {
            return null;
        }

        return DateTimeUtils.dayOfWeek(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * The current day of the week for the calendar.
     *
     * @param time time
     * @return current day of the week, or {@code null} if the input is {@code null}.
     */
    public DayOfWeek dayOfWeek(final Instant time) {
        if (time == null) {
            return null;
        }

        return DateTimeUtils.dayOfWeek(time, timeZone);
    }

    /**
     * The current day of the week for the calendar.
     *
     * @param time time
     * @return current day of the week, or {@code null} if the input is {@code null}.
     */
    public DayOfWeek dayOfWeek(final ZonedDateTime time) {
        if (time == null) {
            return null;
        }

        return DateTimeUtils.dayOfWeek(time.toInstant(), timeZone);
    }

    /**
     * Returns a 1-based int value of the day of the week for the calendar, where 1 is Monday and 7 is Sunday.
     *
     * @return current day of the week
     */
    public int dayOfWeekValue() {
        return dayOfWeekValue(calendarDate());
    }

    /**
     * Returns a 1-based int value of the day of the week for the calendar, where 1 is Monday and 7 is Sunday.
     *
     * @param date date
     * @return current day of the week, or {@link io.deephaven.util.QueryConstants#NULL_INT} if the input is
     *         {@code null}.
     */
    public int dayOfWeekValue(final LocalDate date) {
        if (date == null) {
            return NULL_INT;
        }

        return DateTimeUtils.dayOfWeekValue(date);
    }

    /**
     * Returns a 1-based int value of the day of the week for the calendar, where 1 is Monday and 7 is Sunday.
     *
     * @param date date
     * @return current day of the week, or {@link io.deephaven.util.QueryConstants#NULL_INT} if the input is
     *         {@code null}.
     */
    public int dayOfWeekValue(final String date) {
        if (date == null) {
            return NULL_INT;
        }

        return DateTimeUtils.dayOfWeekValue(DateTimeUtils.parseLocalDate(date));
    }

    /**
     * Returns a 1-based int value of the day of the week for the calendar, where 1 is Monday and 7 is Sunday.
     *
     * @param time time
     * @return current day of the week, or {@link io.deephaven.util.QueryConstants#NULL_INT} if the input is
     *         {@code null}.
     */
    public int dayOfWeekValue(final Instant time) {
        if (time == null) {
            return NULL_INT;
        }

        return DateTimeUtils.dayOfWeekValue(time, timeZone);
    }

    /**
     * Returns a 1-based int value of the day of the week for the calendar, where 1 is Monday and 7 is Sunday.
     *
     * @param time time
     * @return current day of the week, or {@link io.deephaven.util.QueryConstants#NULL_INT} if the input is
     *         {@code null}.
     */
    public int dayOfWeekValue(final ZonedDateTime time) {
        if (time == null) {
            return NULL_INT;
        }

        return DateTimeUtils.dayOfWeekValue(time.toInstant(), timeZone);
    }

    // endregion


    // region Arithmetic

    /**
     * Adds a specified number of days to an input date. Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public LocalDate plusDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return date.plusDays(days);
    }

    /**
     * Adds a specified number of days to an input date. Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String plusDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        final LocalDate d = plusDays(DateTimeUtils.parseLocalDate(date), days);
        return d == null ? null : d.toString();
    }

    /**
     * Adds a specified number of days to an input time. Adding negative days is equivalent to subtracting days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public Instant plusDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusDays(DateTimeUtils.toZonedDateTime(time, timeZone), days).toInstant();
    }

    /**
     * Adds a specified number of days to an input time. Adding negative days is equivalent to subtracting days.
     * <p>
     * Day additions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public ZonedDateTime plusDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        final ZonedDateTime zdt = time.withZoneSameInstant(timeZone);
        return plusDays(zdt.toLocalDate(), days)
                .atTime(zdt.toLocalTime())
                .atZone(timeZone);
    }

    /**
     * Subtracts a specified number of days to an input date. Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract
     * @return {@code days} days before {@code date}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public LocalDate minusDays(final LocalDate date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        return date.minusDays(days);
    }

    /**
     * Subtracts a specified number of days to an input date. Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract
     * @return {@code days} days before {@code date}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String minusDays(final String date, final int days) {
        if (date == null || days == NULL_INT) {
            return null;
        }

        final LocalDate d = minusDays(DateTimeUtils.parseLocalDate(date), days);
        return d == null ? null : d.toString();
    }

    /**
     * Subtracts a specified number of days to an input time. Subtracting negative days is equivalent to adding days.
     * <p>
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     *
     * @param time time
     * @param days number of days to subtract
     * @return {@code days} days before {@code time}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public Instant minusDays(final Instant time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusDays(time, -days);
    }

    /**
     * Subtracts a specified number of days to an input time. Subtracting negative days is equivalent to adding days.
     * <p>
     * Day subtractions are not always 24 hours. The resultant time will have the same local time as the input time, as
     * determined by the calendar's time zone. This accounts for Daylight Savings Time. For example, 2023-11-05 has a
     * daylight savings time adjustment, so '2023-11-04T14:00 ET' plus 1 day will result in '2023-11-05T15:00 ET', which
     * is a 25-hour difference.
     * <p>
     * The resultant time will have the same time zone as the calendar. This could be different than the time zone of
     * the input {@link ZonedDateTime}.
     *
     * @param time time
     * @param days number of days to subtract
     * @return {@code days} days before {@code time}, or {@code null} if any input is {@code null} or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public ZonedDateTime minusDays(final ZonedDateTime time, final int days) {
        if (time == null || days == NULL_INT) {
            return null;
        }

        return plusDays(time, -days);
    }

    /**
     * Adds a specified number of days to the current date. Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} days after the current date, or {@code null} if {@code days} is
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public LocalDate futureDate(final int days) {
        return plusDays(calendarDate(), days);
    }

    /**
     * Subtracts a specified number of days from the current date. Subtracting negative days is equivalent to adding
     * days.
     *
     * @param days number of days to subtract.
     * @return {@code days} days before the current date, or {@code null} if {@code days} is
     *         {@link io.deephaven.util.QueryConstants#NULL_INT}.
     */
    public LocalDate pastDate(final int days) {
        return minusDays(calendarDate(), days);
    }

    // endregion

    // region Ranges

    private void calendarDatesInternal(final ArrayList<LocalDate> result, final LocalDate start, final LocalDate end,
            final boolean startInclusive,
            final boolean endInclusive) {
        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if (!skip) {
                result.add(day);
            }
        }
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}, or {@code null} if any input is {@code null}.
     */
    public LocalDate[] calendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        if (start.isAfter(end)) {
            return new LocalDate[0];
        }

        final ArrayList<LocalDate> dateList = new ArrayList<>();

        SummaryData summaryFirst = null;
        SummaryData summary = null;

        for (Iterator<SummaryData> it = summaryCache.iterator(start, end, startInclusive, endInclusive); it
                .hasNext();) {
            summary = it.next();

            if (summaryFirst == null) {
                summaryFirst = summary;
                calendarDatesInternal(dateList, start, summaryFirst.startDate, startInclusive, false);
            }

            dateList.addAll(summary.dates);
        }

        if (summaryFirst == null) {
            calendarDatesInternal(dateList, start, end, startInclusive, endInclusive);
        } else {
            calendarDatesInternal(dateList, summary.endDate, end, true, endInclusive);
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}, or {@code null} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] calendarDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        final LocalDate[] dates =
                calendarDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive,
                        endInclusive);
        return dates == null ? null : Arrays.stream(dates).map(DateTimeUtils::formatDate).toArray(String[]::new);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}, or {@code null} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        return calendarDates(start.withZoneSameInstant(timeZone).toLocalDate(),
                end.withZoneSameInstant(timeZone).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}, or {@code null} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return null;
        }

        return calendarDates(DateTimeUtils.toLocalDate(start, timeZone), DateTimeUtils.toLocalDate(end, timeZone),
                startInclusive, endInclusive);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return dates between {@code start} and {@code end}, including {@code start} and {@code end}, or {@code null} if
     *         any input is {@code null}.
     */
    public LocalDate[] calendarDates(final LocalDate start, final LocalDate end) {
        return calendarDates(start, end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return dates between {@code start} and {@code end}, including {@code start} and {@code end}, or {@code null} if
     *         any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public String[] calendarDates(final String start, final String end) {
        return calendarDates(start, end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return dates between {@code start} and {@code end}, including {@code start} and {@code end}, or {@code null} if
     *         any input is {@code null}.
     */
    public LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end) {
        return calendarDates(start, end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return dates between {@code start} and {@code end}, including {@code start} and {@code end}, or {@code null} if
     *         any input is {@code null}.
     */
    public LocalDate[] calendarDates(final Instant start, final Instant end) {
        return calendarDates(start, end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     */
    public int numberCalendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        int days = (int) ChronoUnit.DAYS.between(start, end.plusDays(1));

        if (!startInclusive) {
            days -= 1;
        }

        if (!endInclusive) {
            days -= 1;
        }

        return Math.max(days, 0);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final String start, final String end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        return numberCalendarDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end),
                startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        return numberCalendarDates(start.withZoneSameInstant(timeZone).toLocalDate(),
                end.withZoneSameInstant(timeZone).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final Instant start, final Instant end, final boolean startInclusive,
            final boolean endInclusive) {
        if (start == null || end == null) {
            return NULL_INT;
        }

        return numberCalendarDates(DateTimeUtils.toLocalDate(start, timeZone), DateTimeUtils.toLocalDate(end, timeZone),
                startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of dates between {@code start} and {@code end}, including {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     */
    public int numberCalendarDates(final LocalDate start, final LocalDate end) {
        return numberCalendarDates(start, end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of dates between {@code start} and {@code end}, including {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final String start, final String end) {
        return numberCalendarDates(start, end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of dates between {@code start} and {@code end}, including {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     */
    public int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end) {
        return numberCalendarDates(start, end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end end of a time range
     * @return number of dates between {@code start} and {@code end}, including {@code start} and {@code end}, or
     *         {@link io.deephaven.util.QueryConstants#NULL_INT} if any input is {@code null}.
     */
    public int numberCalendarDates(final Instant start, final Instant end) {
        return numberCalendarDates(start, end, true, true);
    }

    // endregion

}
