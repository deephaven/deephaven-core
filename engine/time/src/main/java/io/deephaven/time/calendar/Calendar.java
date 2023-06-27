/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

//TODO: update all headers
//TODO: review all docs

//TODO: add regions

/**
 * A calendar.
 *
 * A calendar is associated with a specific time zone.
 *
 * Date strings must be in a format that can be parsed by {@code DateTimeUtils#parseDate}.  Methods that accept
 * strings can be slower than methods written explicitly for {@code Instant}, {@code ZonedDateTime}, or {@code LocalDate}.
 */
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
    public Calendar(String name, String description, ZoneId timeZone) {
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

    // endregion

    // region Arithmetic

    //TODO should these methods be named add/subtract or plus/minus?

    /**
     * Adds a specified number of days to an input date.  Adding negative days is equivalent to subtracting days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate addDays(final LocalDate date, final int days) {
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
    public LocalDate addDays(final String date, final int days) {
        if (date == null) {
            return null;
        }

        //TODO: quiet parsing?  document exception?
        return addDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Adds a specified number of days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate addDays(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return addDays(DateTimeUtils.toLocalDate(time, timeZone), days);
    }

    /**
     * Adds a specified number of days to an input time.  Adding negative days is equivalent to subtracting days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate addDays(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return addDays(time.toInstant(), days);
    }

    /**
     * Subtracts a specified number of days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate subtractDays(final LocalDate date, final int days) {
        if (date == null) {
            return null;
        }

        return date.minusDays(days);
    }

    /**
     * Subtracts a specified number of days to an input date.  Subtracting negative days is equivalent to adding days.
     *
     * @param date date
     * @param days number of days to add
     * @return {@code days} days after {@code date}; null if {@code date} is null
     */
    public LocalDate subtractDays(final String date, final int days) {
        if (date == null) {
            return null;
        }

        return subtractDays(DateTimeUtils.parseLocalDate(date), days);
    }

    /**
     * Subtracts a specified number of days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate subtractDays(final Instant time, final int days) {
        if (time == null) {
            return null;
        }

        return subtractDays(DateTimeUtils.toLocalDate(time, timeZone), days);
    }

    /**
     * Subtracts a specified number of days to an input time.  Subtracting negative days is equivalent to adding days.
     *
     * @param time time
     * @param days number of days to add
     * @return {@code days} days after {@code time}; null if {@code date} is null
     */
    public LocalDate subtractDays(final ZonedDateTime time, final int days) {
        if (time == null) {
            return null;
        }

        return subtractDays(time.toInstant(), days);
    }

//TODO: future_day / past_day
    //TODO: rename currentDate? -> nextDate() or futureDate()
    /**
     * Adds a specified number of days to the current date.  Adding negative days is equivalent to subtracting days.
     *
     * @param days number of days to add.
     * @return {@code days} days after the current date
     */
    public LocalDate nextDay(int days) {
        return addDays(currentDay(), days);
    }

//TODO: future_day / past_day
    //TODO: rename previousDate?
    /**
     * Subtracts a specified number of days from the current date.  Subtracting negative days is equivalent to adding days.
     *
     * @param days number of days to subtract.
     * @return {@code days} days before the current date
     */
    public LocalDate previousDay(int days) {
        return subtractDays(currentDay(), days);
    }

    //TODO: rename currentDate? -> nextDate() or futureDate()
    /**
     * The current date.
     *
     * @return current date
     */
    public LocalDate currentDay() {
        return DateTimeUtils.today(timeZone());
    }

    // endregion

    // region Ranges

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final LocalDate start, final LocalDate end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        LocalDate day = start;
        List<LocalDate> dateList = new ArrayList<>();

        while (!day.isAfter(end)) {
            dateList.add(day);
            day = day.plusDays(1);
        }

        return dateList.toArray(new LocalDate[0]);
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final ZonedDateTime start, final ZonedDateTime end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(start.withZoneSameInstant(timeZone()).toLocalDate(), end.withZoneSameInstant(timeZone()).toLocalDate());
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final Instant start, final Instant end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(DateTimeUtils.toZonedDateTime(start, timeZone()).toLocalDate(), DateTimeUtils.toZonedDateTime(end, timeZone()).toLocalDate());
    }

    /**
     * Gets the days in a given range.
     *
     * @param start start of a time range; if null, return empty array
     * @param end   end of a time range; if null, return empty array
     * @return the inclusive days between {@code start} and {@code end}
     */
    public LocalDate[] daysInRange(final String start, final String end) {
        if (start == null || end == null) {
            return new LocalDate[0];
        }

        return daysInRange(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end));
    }

    //TODO: add InRange to the names?
    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final LocalDate start, final LocalDate end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        int days = (int) ChronoUnit.DAYS.between(start, end);
        if (days < 0) {
            days = days - (endInclusive ? 1 : 0);
        } else {
            days = days + (endInclusive ? 1 : 0);
        }
        return days;
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final LocalDate start, final LocalDate end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final ZonedDateTime start, final ZonedDateTime end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(start.withZoneSameInstant(timeZone()).toLocalDate(), end.withZoneSameInstant(timeZone()).toLocalDate(), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final ZonedDateTime start, final ZonedDateTime end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final Instant start, final Instant end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(DateTimeUtils.toZonedDateTime(start, timeZone()).toLocalDate(), DateTimeUtils.toZonedDateTime(end, timeZone()).toLocalDate(), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final Instant start, final Instant end) {
        return numberOfDays(start, end, false);
    }

    /**
     * Gets the number of days in a given range.
     *
     * @param start        start of a time range
     * @param end          end of a time range
     * @param endInclusive whether to treat the {@code end} inclusive or exclusively
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final String start, final String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(DateTimeUtils.parseLocalDate(start), DateTimeUtils.parseLocalDate(end), endInclusive);
    }

    /**
     * Gets the number of days in a given range, end date exclusive.
     *
     * @param start start of a time range
     * @param end   end of a time range
     * @return the number of days between {@code start} and {@code end}, or {@code NULL_INT} if any input is null.
     */
    public int numberOfDays(final String start, final String end) {
        return numberOfDays(start, end, false);
    }

    //TODO:
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
