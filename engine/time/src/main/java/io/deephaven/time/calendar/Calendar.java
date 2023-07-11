/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

//TODO: review all docs

/**
 * A calendar.
 *
 * A calendar is associated with a specific time zone.
 *
 * Date strings must be in a format that can be parsed by {@code DateTimeUtils#parseDate}.  Methods that accept
 * strings can be slower than methods written explicitly for {@code Instant}, {@code ZonedDateTime}, or {@code LocalDate}.
 */
@SuppressWarnings("unused") //TODO: remove unused annotation
public class Calendar {
    
    private final String name;
    private final String description;
    private final ZoneId timeZone;

    // region Constructors

    /**
     * Creates a new calendar.
     * 
     * @param name calendar name.
     * @param description calendar description.
     * @param timeZone calendar time zone.
     */
    public Calendar(final String name, final String description, final ZoneId timeZone) {
        this.name = name;
        this.description = description;
        this.timeZone = timeZone;
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

    // region Arithmetic

    /**
     * Adds a specified number of days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate plusDays(final LocalDate date, final int days) {
        if (date == null) {
            return null;
        }

        return date.plusDays(days);
    }

    /**
     * Adds a specified number of days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate plusDays(final String date, final int days) {
        if (date == null) {
            return null;
        }

        //TODO: quiet parsing?  document exception?
        return plusDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Adds a specified number of days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate plusDays(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return plusDays(DateTimeUtils.toLocalDate(time, timeZone), days);
    }

    /**
     * Adds a specified number of days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate plusDays(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return plusDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate minusDays(final LocalDate date, final int days) {
        if (date == null) {
            return null;
        }

        return date.minusDays(days);
    }

    /**
     * Subtracts a specified number of days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to subtract
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate minusDays(final String date, final int days) {
        if (date == null) {
            return null;
        }

        return minusDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Subtracts a specified number of days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate minusDays(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return minusDays(DateTimeUtils.toLocalDate(time, timeZone), days);
    }

    /**
     * Subtracts a specified number of days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to subtract
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate minusDays(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return minusDays(time.toInstant(), days);
    }

    /**
     * The current date.
     *
     * @return current date
     */
    public LocalDate currentDate() {
        return DateTimeUtils.today(timeZone());
    }

    /**
     * Adds a specified number of days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} days after the current date
     */
    public LocalDate futureDate(final int days) {
        return plusDays(currentDate(), days);
    }

    /**
     * Subtracts a specified number of days from the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} days before the current date
     */
    public LocalDate pastDate(final int days) {
        return minusDays(currentDate(), days);
    }

    // endregion

    // region Ranges

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public LocalDate[] calendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        List<LocalDate> dateList = new ArrayList<>();

        for (LocalDate day = start; !day.isAfter(end); day = day.plusDays(1)) {
            final boolean skip = (!startInclusive && day.equals(start)) || (!endInclusive && day.equals(end));

            if(!skip) {
                dateList.add(day);
            }
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return calendarDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive, endInclusive);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return calendarDates(start.withZoneSameInstant(timeZone).toLocalDate(), end.withZoneSameInstant(timeZone).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return calendarDates(DateTimeUtils.toLocalDate(start, timeZone), DateTimeUtils.toLocalDate(end, timeZone), startInclusive, endInclusive);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public LocalDate[] calendarDates(final LocalDate start, final LocalDate end) {
        return calendarDates(start,end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public LocalDate[] calendarDates(final String start, final String end) {
        return calendarDates(start,end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public LocalDate[] calendarDates(final ZonedDateTime start, final ZonedDateTime end) {
        return calendarDates(start,end, true, true);
    }

    /**
     * Returns the dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public LocalDate[] calendarDates(final Instant start, final Instant end) {
        return calendarDates(start,end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public int numberCalendarDates(final LocalDate start, final LocalDate end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");

        int days = (int) ChronoUnit.DAYS.between(start, end.plusDays(1));

            if(!startInclusive){
                days -= 1;
            }

            if(!endInclusive){
                days -= 1;
            }

            return Math.max(days, 0);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final String start, final String end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberCalendarDates(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberCalendarDates(start.withZoneSameInstant(timeZone).toLocalDate(), end.withZoneSameInstant(timeZone).toLocalDate(), startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @param startInclusive true to include {@code start} in the result; false to exclude {@code start}
     * @param endInclusive true to include {@code end} in the result; false to exclude {@code end}
     * @return number of dates between {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final Instant start, final Instant end, final boolean startInclusive, final boolean endInclusive) {
        Require.neqNull(start, "start");
        Require.neqNull(end, "end");
        return numberCalendarDates(DateTimeUtils.toLocalDate(start, timeZone), DateTimeUtils.toLocalDate(end, timeZone), startInclusive, endInclusive);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return number of dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public int numberCalendarDates(final LocalDate start, final LocalDate end) {
        return numberCalendarDates(start,end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return number of dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     * @throws DateTimeUtils.DateTimeParseException if the string cannot be parsed
     */
    public int numberCalendarDates(final String start, final String end) {
        return numberCalendarDates(start,end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return number of dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public int numberCalendarDates(final ZonedDateTime start, final ZonedDateTime end) {
        return numberCalendarDates(start,end, true, true);
    }

    /**
     * Returns the number of dates in a given range.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return number of dates between {@code start} and {@code end}; including {@code start} and {@code end}
     * @throws RequirementFailure if any input is null
     */
    public int numberCalendarDates(final Instant start, final Instant end) {
        return numberCalendarDates(start,end, true, true);
    }

    // endregion

    // region Differences

    //TODO: time diff methods?
//    diffBusinessDay
//            diffBusinessDay
//    diffBusinessNanos
//            diffBusinessNanos
//    diffBusinessYear
//            diffBusinessYear
//    diffNonBusinessDay
//            diffNonBusinessDay
//    diffNonBusinessNanos
//            diffNonBusinessNanos

    // endregion

}
