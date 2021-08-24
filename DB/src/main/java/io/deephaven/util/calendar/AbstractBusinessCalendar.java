/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.QueryConstants;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBusinessCalendar extends AbstractCalendar
    implements BusinessCalendar {

    public boolean isBusinessDay(final DBDateTime time) {
        return fractionOfStandardBusinessDay(time) > 0.0;
    }

    public boolean isBusinessDay(final String date) {
        return date != null && getBusinessSchedule(date).isBusinessDay();
    }

    public boolean isBusinessDay(final LocalDate date) {
        return date != null && getBusinessSchedule(date).isBusinessDay();
    }

    public boolean isBusinessTime(final DBDateTime time) {
        return time != null && getBusinessSchedule(time).isBusinessTime(time);
    }

    public String previousBusinessDay(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().minusDays(1);
        while (!isBusinessDay(t)) {
            t = t.minusDays(1);
        }

        return DateStringUtils.format(t);
    }

    public String previousBusinessDay(final DBDateTime time, int days) {
        if (time == null) {
            return null;
        }

        if (days < 0) {
            return nextBusinessDay(time, -days);
        } else if (days == 0 && !isBusinessDay(time)) {
            return null;
        } else if (days == 0) {
            return time.toDateString(timeZone());
        }

        String date = null;
        while (days > 0) {
            if (date == null) {
                date = previousBusinessDay(time);
            } else {
                date = previousBusinessDay(date);
            }
            days--;
        }

        return date;
    }

    public String previousBusinessDay(String date) {
        if (date == null) {
            return null;
        }

        date = DateStringUtils.minusDays(date, 1);
        while (!isBusinessDay(date)) {
            // first minusDays ensures the date strings will be of the correct format
            date = DateStringUtils.minusDaysQuiet(date, 1);
        }

        return date;
    }

    public String previousBusinessDay(String date, int days) {
        if (date == null) {
            return null;
        }

        if (days < 0) {
            return nextBusinessDay(date, -days);
        } else if (days == 0 && !isBusinessDay(date)) {
            return null;
        } else if (days == 0) {
            return date;
        }

        while (days > 0) {
            date = previousBusinessDay(date);
            days--;
        }

        return date;
    }

    public BusinessSchedule previousBusinessSchedule(final DBDateTime time) {
        return getBusinessSchedule(previousDay(time));
    }

    public BusinessSchedule previousBusinessSchedule(final DBDateTime time, int days) {
        return getBusinessSchedule(previousDay(time, days));
    }

    public BusinessSchedule previousBusinessSchedule(String date) {
        return getBusinessSchedule(previousDay(date));
    }

    public BusinessSchedule previousBusinessSchedule(String date, int days) {
        return getBusinessSchedule(previousDay(date, days));
    }

    public String previousNonBusinessDay(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().minusDays(1);
        while (isBusinessDay(t)) {
            t = t.minusDays(1);
        }

        return DateStringUtils.format(t);
    }

    public String previousNonBusinessDay(final DBDateTime time, int days) {
        if (time == null) {
            return null;
        }

        if (days < 0) {
            return nextNonBusinessDay(time, -days);
        } else if (days == 0 && isBusinessDay(time)) {
            return null;
        } else if (days == 0) {
            return time.toDateString(timeZone());
        }

        String date = null;
        while (days > 0) {
            if (date == null) {
                date = previousNonBusinessDay(time);
            } else {
                date = previousNonBusinessDay(date);
            }
            days--;
        }

        return date;
    }

    public String previousNonBusinessDay(String date) {
        if (date == null) {
            return null;
        }

        date = DateStringUtils.minusDays(date, 1);
        while (isBusinessDay(date)) {
            // first minusDays ensures the date strings will be of the correct format
            date = DateStringUtils.minusDaysQuiet(date, 1);
        }

        return date;
    }

    public String previousNonBusinessDay(String date, int days) {
        if (date == null) {
            return null;
        }

        if (days < 0) {
            return nextNonBusinessDay(date, -days);
        } else if (days == 0 && isBusinessDay(date)) {
            return null;
        } else if (days == 0) {
            return date;
        }

        while (days > 0) {
            date = previousNonBusinessDay(date);
            days--;
        }

        return date;
    }

    public String nextBusinessDay(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().plusDays(1);
        while (!isBusinessDay(t)) {
            t = t.plusDays(1);
        }

        return DateStringUtils.format(t);
    }

    public String nextBusinessDay(final DBDateTime time, int days) {
        if (time == null) {
            return null;
        }

        if (days < 0) {
            return previousBusinessDay(time, -days);
        } else if (days == 0 && !isBusinessDay(time)) {
            return null;
        } else if (days == 0) {
            return time.toDateString(timeZone());
        }

        String date = null;
        while (days > 0) {
            if (date == null) {
                date = nextBusinessDay(time);
            } else {
                date = nextBusinessDay(date);
            }
            days--;
        }

        return date;
    }

    public String nextBusinessDay(String date) {
        if (date == null) {
            return null;
        }

        date = DateStringUtils.plusDays(date, 1);
        while (!isBusinessDay(date)) {
            // first plusDays ensures the date strings will be of the correct format
            date = DateStringUtils.plusDaysQuiet(date, 1);
        }

        return date;
    }

    public String nextBusinessDay(String date, int days) {
        if (date == null) {
            return null;
        }

        if (days < 0) {
            return previousBusinessDay(date, -days);
        } else if (days == 0 && !isBusinessDay(date)) {
            return null;
        } else if (days == 0) {
            return date;
        }

        while (days > 0) {
            date = nextBusinessDay(date);
            days--;
        }

        return date;
    }


    public BusinessSchedule nextBusinessSchedule(final DBDateTime time) {
        return getBusinessSchedule(nextDay(time));
    }

    public BusinessSchedule nextBusinessSchedule(final DBDateTime time, int days) {
        return getBusinessSchedule(nextDay(time, days));
    }

    public BusinessSchedule nextBusinessSchedule(String date) {
        return getBusinessSchedule(nextDay(date));
    }

    public BusinessSchedule nextBusinessSchedule(String date, int days) {
        return getBusinessSchedule(nextDay(date, days));
    }

    public String nextNonBusinessDay(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        LocalDate t = DBTimeUtils.getZonedDateTime(time, timeZone()).toLocalDate().plusDays(1);
        while (isBusinessDay(t)) {
            t = t.plusDays(1);
        }

        return DateStringUtils.format(t);
    }

    public String nextNonBusinessDay(final DBDateTime time, int days) {
        if (time == null) {
            return null;
        }

        if (days < 0) {
            return previousNonBusinessDay(time, -days);
        } else if (days == 0 && isBusinessDay(time)) {
            return null;
        } else if (days == 0) {
            return time.toDateString(timeZone());
        }

        String date = null;
        while (days > 0) {
            if (date == null) {
                date = nextNonBusinessDay(time);
            } else {
                date = nextNonBusinessDay(date);
            }
            days--;
        }

        return date;
    }

    public String nextNonBusinessDay(String date) {
        if (date == null) {
            return null;
        }

        date = DateStringUtils.plusDays(date, 1);
        while (isBusinessDay(date)) {
            // first plusDays ensures the date strings will be of the correct format
            date = DateStringUtils.plusDaysQuiet(date, 1);
        }

        return date;
    }

    public String nextNonBusinessDay(String date, int days) {
        if (date == null) {
            return null;
        }

        if (days < 0) {
            return previousNonBusinessDay(date, -days);
        } else if (days == 0 && isBusinessDay(date)) {
            return null;
        } else if (days == 0) {
            return date;
        }

        while (days > 0) {
            date = nextNonBusinessDay(date);
            days--;
        }

        return date;
    }

    public String[] businessDaysInRange(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return new String[0];
        }
        LocalDate day = DBTimeUtils.getZonedDateTime(start, timeZone()).toLocalDate();
        LocalDate day2 = DBTimeUtils.getZonedDateTime(end, timeZone()).toLocalDate();

        List<String> dateList = new ArrayList<>();
        while (!day.isAfter(day2)) {
            if (isBusinessDay(day)) {
                dateList.add(DateStringUtils.format(day));
            }
            day = day.plusDays(1);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public String[] businessDaysInRange(String start, String end) {
        if (start == null || end == null) {
            return new String[0];
        }

        List<String> dateList = new ArrayList<>();
        if (!DateStringUtils.isAfter(start, end)) {
            if (isBusinessDay(start)) {
                dateList.add(start);
            }
            start = nextBusinessDay(start);
        } else {
            return new String[0];
        }

        // first isAfter ensures the date strings will be of the correct format
        while (!DateStringUtils.isAfterQuiet(start, end)) {
            if (isBusinessDay(start)) {
                dateList.add(start);
            }
            start = DateStringUtils.plusDaysQuiet(start, 1);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public String[] nonBusinessDaysInRange(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return new String[0];
        }
        LocalDate day = DBTimeUtils.getZonedDateTime(start, timeZone()).toLocalDate();
        LocalDate day2 = DBTimeUtils.getZonedDateTime(end, timeZone()).toLocalDate();

        List<String> dateList = new ArrayList<>();
        while (!day.isAfter(day2)) {
            if (!isBusinessDay(day)) {
                dateList.add(DateStringUtils.format(day));
            }
            day = day.plusDays(1);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public String[] nonBusinessDaysInRange(String start, String end) {
        if (start == null || end == null) {
            return new String[0];
        }

        List<String> dateList = new ArrayList<>();
        if (!DateStringUtils.isAfter(start, end)) {
            if (!isBusinessDay(start)) {
                dateList.add(start);
            }
            start = nextNonBusinessDay(start);
        } else {
            return new String[0];
        }

        // first isAfter ensures the date strings will be of the correct format
        while (!DateStringUtils.isAfterQuiet(start, end)) {
            dateList.add(start);
            start = nextNonBusinessDay(start);
        }

        return dateList.toArray(new String[dateList.size()]);
    }

    public long diffNonBusinessNanos(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }

        if (DBTimeUtils.isAfter(start, end)) {
            return -diffNonBusinessNanos(end, start);
        }

        return DBTimeUtils.minus(end, start) - diffBusinessNanos(start, end);
    }

    public double diffBusinessDay(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffBusinessNanos(start, end) / (double) standardBusinessDayLengthNanos();
    }

    public double diffNonBusinessDay(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return (double) diffNonBusinessNanos(start, end)
            / (double) standardBusinessDayLengthNanos();
    }

    public int numberOfBusinessDays(DBDateTime start, DBDateTime end) {
        return numberOfBusinessDays(start, end, false);
    }

    public int numberOfBusinessDays(DBDateTime start, DBDateTime end, final boolean endInclusive) {
        return numberOfBusinessDays(start == null ? null : start.toDateString(timeZone()),
            end == null ? null : end.toDateString(timeZone()), endInclusive);
    }

    public int numberOfBusinessDays(String start, String end) {
        return numberOfBusinessDays(start, end, false);
    }

    public int numberOfBusinessDays(String start, String end, final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        int days = 0;
        if (DateStringUtils.isBefore(start, end)) {
            if (isBusinessDay(start)) {
                days++;
            }
            start = nextBusinessDay(start);
        } else if (DateStringUtils.isAfter(start, end)) {
            return -numberOfBusinessDays(end, start, endInclusive);
        }

        while (DateStringUtils.isBeforeQuiet(start, end)) {
            days++;
            start = nextBusinessDay(start);
        }

        return days + (endInclusive && isBusinessDay(end) ? 1 : 0);
    }

    public int numberOfNonBusinessDays(DBDateTime start, DBDateTime end) {
        return numberOfNonBusinessDays(start, end, false);
    }

    public int numberOfNonBusinessDays(DBDateTime start, DBDateTime end,
        final boolean endInclusive) {
        return numberOfNonBusinessDays(start == null ? null : start.toDateString(timeZone()),
            end == null ? null : end.toDateString(timeZone()), endInclusive);
    }

    public int numberOfNonBusinessDays(final String start, final String end) {
        return numberOfNonBusinessDays(start, end, false);
    }

    public int numberOfNonBusinessDays(final String start, final String end,
        final boolean endInclusive) {
        if (start == null || end == null) {
            return QueryConstants.NULL_INT;
        }

        return numberOfDays(start, end, endInclusive)
            - numberOfBusinessDays(start, end, endInclusive);
    }

    public double fractionOfStandardBusinessDay(final DBDateTime time) {
        final BusinessSchedule businessDate = getBusinessSchedule(time);
        return businessDate == null ? 0.0
            : (double) businessDate.getLOBD() / (double) standardBusinessDayLengthNanos();
    }

    public double fractionOfStandardBusinessDay(final String date) {
        final BusinessSchedule businessDate = getBusinessSchedule(date);
        return businessDate == null ? 0.0
            : (double) businessDate.getLOBD() / (double) standardBusinessDayLengthNanos();
    }

    public double fractionOfBusinessDayRemaining(final DBDateTime time) {
        final BusinessSchedule businessDate = getBusinessSchedule(time);
        if (businessDate == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        if (businessDate.getLOBD() == 0) {
            return 0;
        }

        long businessDaySoFar = businessDate.businessTimeElapsed(time);
        return (double) (businessDate.getLOBD() - businessDaySoFar)
            / (double) businessDate.getLOBD();
    }

    public double fractionOfBusinessDayComplete(final DBDateTime time) {
        if (time == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        return 1.0 - fractionOfBusinessDayRemaining(time);
    }

    public boolean isLastBusinessDayOfMonth(final DBDateTime time) {
        return isBusinessDay(time) && isLastBusinessDayOfMonth(time.toDateString(timeZone()));
    }

    public boolean isLastBusinessDayOfMonth(final String date) {
        if (!isBusinessDay(date)) {
            return false;
        }

        String nextBusAfterDate = nextBusinessDay(date);

        // covers case December to January
        return (DateStringUtils.monthOfYear(date)
            - DateStringUtils.monthOfYear(nextBusAfterDate)) != 0;
    }

    public boolean isLastBusinessDayOfWeek(final DBDateTime time) {
        return isBusinessDay(time) && isLastBusinessDayOfWeek(time.toDateString(timeZone()));
    }

    public boolean isLastBusinessDayOfWeek(final String date) {
        if (!isBusinessDay(date)) {
            return false;
        }

        String nextBusinessDay = nextBusinessDay(date);
        return dayOfWeek(date).compareTo(dayOfWeek(nextBusinessDay)) > 0
            || numberOfDays(date, nextBusinessDay) > 6;
    }

}
