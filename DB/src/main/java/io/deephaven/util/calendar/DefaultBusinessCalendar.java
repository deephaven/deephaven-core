/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.base.Pair;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Default implementation for a {@link BusinessCalendar}. This implementation is thread safe.
 *
 * Overrides many default {@link Calendar} and BusinessCalendar methods for improved performance.
 * See the documentation of Calendar for details.
 */
public class DefaultBusinessCalendar extends AbstractBusinessCalendar implements Serializable {

    private static final long serialVersionUID = -5887343387358189382L;

    // our "null" holder for holidays
    private static final BusinessSchedule HOLIDAY = new Holiday();
    private static final DateTimeFormatter HOLIDAY_PARSER =
        DateTimeFormatter.ofPattern("yyyyMMdd").withLocale(new Locale("en", "US"));

    // each calendar has a name, timezone, and date string format
    private final String calendarName;
    private final DBTimeZone timeZone;

    // length, in nanos, that a default day is open
    private final long lengthOfDefaultDayNanos;

    // holds the open hours for a standard business day
    private final List<String> defaultBusinessPeriodStrings;
    private final Set<DayOfWeek> weekendDays;

    private final Map<LocalDate, BusinessSchedule> dates;
    private final Map<LocalDate, BusinessSchedule> holidays;
    private final Map<Integer, Long> cachedYearLengths = new ConcurrentHashMap<>();

    @VisibleForTesting
    DefaultBusinessCalendar(final CalendarElements calendarElements) {
        this(calendarElements.calendarName, calendarElements.timeZone,
            calendarElements.lengthOfDefaultDayNanos, calendarElements.defaultBusinessPeriodStrings,
            calendarElements.weekendDays, calendarElements.dates, calendarElements.holidays);
    }

    private DefaultBusinessCalendar(final String calendarName, final DBTimeZone timeZone,
        final long lengthOfDefaultDayNanos, final List<String> defaultBusinessPeriodStrings,
        final Set<DayOfWeek> weekendDays, final Map<LocalDate, BusinessSchedule> dates,
        final Map<LocalDate, BusinessSchedule> holidays) {
        this.calendarName = calendarName;
        this.timeZone = timeZone;
        this.lengthOfDefaultDayNanos = lengthOfDefaultDayNanos;
        this.defaultBusinessPeriodStrings = defaultBusinessPeriodStrings;
        this.weekendDays = weekendDays;
        this.dates = dates;
        this.holidays = holidays;
    }

    static BusinessCalendar getInstance(@NotNull final File calendarFile) {
        final CalendarElements calendarElements = constructCalendarElements(calendarFile);
        final BusinessCalendar calendar = new DefaultBusinessCalendar(calendarElements);

        if (!calendarElements.hasHolidays && calendarElements.weekendDays.isEmpty()) {
            return new DefaultNoHolidayBusinessCalendar(calendar);
        }

        return calendar;
    }

    @VisibleForTesting
    static CalendarElements constructCalendarElements(@NotNull final File calendarFile) {
        final CalendarElements calendarElements = new CalendarElements();

        final String filePath = calendarFile.getPath();

        Element root = getRootElement(calendarFile);

        calendarElements.calendarName = getCalendarName(root, filePath);
        calendarElements.timeZone = getTimeZone(root, filePath);

        // Set the default values
        final Element defaultElement = getRequiredChild(root, "default", filePath);

        calendarElements.defaultBusinessPeriodStrings =
            getDefaultBusinessPeriodStrings(defaultElement, filePath);
        calendarElements.weekendDays = getWeekendDays(defaultElement);

        // get the holidays/half days
        final Pair<Map<LocalDate, BusinessSchedule>, Map<LocalDate, BusinessSchedule>> datePair =
            getDates(root, calendarElements);
        calendarElements.dates = datePair.getFirst();
        calendarElements.holidays = datePair.getSecond();

        calendarElements.lengthOfDefaultDayNanos =
            parseStandardBusinessDayLengthNanos(calendarElements.defaultBusinessPeriodStrings);

        return calendarElements;
    }

    private static Pair<Map<LocalDate, BusinessSchedule>, Map<LocalDate, BusinessSchedule>> getDates(
        final Element root, final CalendarElements calendarElements) {
        final Map<LocalDate, BusinessSchedule> holidays = new ConcurrentHashMap<>();
        // initialize the calendar to the years placed in the calendar file
        int minYear = Integer.MAX_VALUE;
        int maxYear = Integer.MIN_VALUE;


        final List<Element> holidayElements = root.getChildren("holiday");
        calendarElements.hasHolidays = !holidayElements.isEmpty();

        for (Element holidayElement : holidayElements) {
            final Element date = holidayElement.getChild("date");
            if (date != null) {
                final String dateStr = getText(date);
                final LocalDate localDate = parseLocalDate(dateStr);
                final int holidayYear = localDate.getYear();
                if (holidayYear < minYear) {
                    minYear = holidayYear;
                }
                if (holidayYear > maxYear) {
                    maxYear = holidayYear;
                }

                final List<Element> businessPeriodsList =
                    holidayElement.getChildren("businessPeriod");
                final List<String> businessPeriodStrings = new ArrayList<>();
                for (Element busPeriod : businessPeriodsList) {
                    String businessPeriod = getText(busPeriod);
                    businessPeriodStrings.add(businessPeriod.trim());
                }

                if (businessPeriodStrings.isEmpty()) {
                    holidays.put(localDate, HOLIDAY);
                } else {
                    holidays.put(localDate,
                        new BusinessSchedule(parseBusinessPeriods(calendarElements.timeZone,
                            localDate, businessPeriodStrings)));
                }
            }
        }

        final Map<LocalDate, BusinessSchedule> dates = new ConcurrentHashMap<>(holidays);
        if (minYear != Integer.MAX_VALUE && maxYear != Integer.MIN_VALUE) {
            for (int i = 0; i < maxYear - minYear; i++) {
                int year = i + minYear;
                boolean isLeap = DateStringUtils.isLeapYear(year);
                int numDays = 365 + (isLeap ? 1 : 0);
                for (int j = 0; j < numDays; j++) {
                    dates.computeIfAbsent(LocalDate.ofYearDay(year, j + 1),
                        date -> newBusinessDay(date, calendarElements.weekendDays,
                            calendarElements.timeZone,
                            calendarElements.defaultBusinessPeriodStrings));
                }
            }
        }

        return new Pair<>(dates, holidays);
    }

    private static Set<DayOfWeek> getWeekendDays(@NotNull final Element defaultElement) {
        final Set<DayOfWeek> weekendDays = new HashSet<>();

        final List<Element> weekends = defaultElement.getChildren("weekend");
        if (weekends != null) {
            for (Element weekendElement : weekends) {
                String weekend = getText(weekendElement);
                weekendDays.add(DayOfWeek.valueOf(weekend.trim().toUpperCase()));
            }
        }

        return weekendDays;
    }

    private static List<String> getDefaultBusinessPeriodStrings(
        @NotNull final Element defaultElement, final String filePath) {
        final List<String> defaultBusinessPeriodStrings = new ArrayList<>();
        final List<Element> businessPeriods = defaultElement.getChildren("businessPeriod");
        if (businessPeriods != null) {
            for (Element businessPeriod1 : businessPeriods) {
                String businessPeriod = getText(businessPeriod1);
                defaultBusinessPeriodStrings.add(businessPeriod);
            }
        } else {
            throw new IllegalArgumentException(
                "Missing the 'businessPeriod' tag in the 'default' section in calendar file "
                    + filePath);
        }

        return defaultBusinessPeriodStrings;
    }

    private static Element getRootElement(File calendarFile) {
        final Document doc;
        try {
            final SAXBuilder builder = new SAXBuilder();
            doc = builder.build(calendarFile);
        } catch (JDOMException e) {
            throw new IllegalArgumentException(
                "Could not initialize business calendar: Error parsing " + calendarFile.getName());
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize business calendar: "
                + calendarFile.getName() + " could not be loaded");
        }

        return doc.getRootElement();
    }

    private static String getCalendarName(@NotNull final Element root, final String filePath) {
        final Element element = getRequiredChild(root, "name", filePath);
        return getText(element);
    }

    private static DBTimeZone getTimeZone(@NotNull final Element root, final String filePath) {
        final Element element = getRequiredChild(root, "timeZone", filePath);
        return DBTimeZone.valueOf(getText(element));
    }

    // throws an error if the child is missing
    private static Element getRequiredChild(@NotNull final Element root, final String child,
        final String filePath) {
        Element element = root.getChild(child);
        if (element != null) {
            return element;
        } else {
            throw new IllegalArgumentException(
                "Missing the " + child + " tag in calendar file " + filePath);
        }
    }

    private static LocalDate parseLocalDate(final String date) {
        try {
            return DateStringUtils.parseLocalDate(date);
        } catch (Exception e) {
            try {
                return LocalDate.parse(date, HOLIDAY_PARSER);
            } catch (Exception ee) {
                throw new IllegalArgumentException(
                    "Malformed date string. Acceptable formats are yyyy-MM-dd and yyyyMMdd. s="
                        + date);
            }
        }
    }

    private static long parseStandardBusinessDayLengthNanos(
        final List<String> defaultBusinessPeriodStrings) {
        long lengthOfDefaultDayNanos = 0;
        Pattern hhmm = Pattern.compile("\\d{2}:\\d{2}");
        for (String businessPeriodString : defaultBusinessPeriodStrings) {
            String[] openClose = businessPeriodString.split(",");
            boolean wellFormed = false;
            if (openClose.length == 2) {
                String open = openClose[0];
                String close = openClose[1];
                if (hhmm.matcher(open).matches() && hhmm.matcher(close).matches()) {
                    String[] openingTimeHHMM = open.split(":");
                    String[] closingTimeHHMM = close.split(":");
                    long defOpenTimeNanos =
                        (Integer.parseInt(openingTimeHHMM[0]) * DBTimeUtils.HOUR)
                            + (Integer.parseInt(openingTimeHHMM[1]) * DBTimeUtils.MINUTE);
                    long defClosingTimeNanos =
                        (Integer.parseInt(closingTimeHHMM[0]) * DBTimeUtils.HOUR)
                            + (Integer.parseInt(closingTimeHHMM[1]) * DBTimeUtils.MINUTE);
                    lengthOfDefaultDayNanos += defClosingTimeNanos - defOpenTimeNanos;
                    wellFormed = true;
                }
            }
            if (!wellFormed) {
                throw new UnsupportedOperationException(
                    "Could not parse business period " + businessPeriodString);
            }
        }

        return lengthOfDefaultDayNanos;
    }

    private static BusinessPeriod[] parseBusinessPeriods(final DBTimeZone timeZone,
        final LocalDate date, final List<String> businessPeriodStrings) {
        final BusinessPeriod[] businessPeriods = new BusinessPeriod[businessPeriodStrings.size()];
        final Pattern hhmm = Pattern.compile("\\d{2}[:]\\d{2}");
        int i = 0;
        for (String businessPeriodString : businessPeriodStrings) {
            final String[] openClose = businessPeriodString.split(",");
            if (openClose.length == 2) {
                final String open = openClose[0];
                String close = openClose[1];
                if (hhmm.matcher(open).matches() && hhmm.matcher(close).matches()) {
                    final String tz =
                        timeZone.name().substring(timeZone.name().indexOf("_")).replace("_", " ");
                    final LocalDate closeDate;

                    if (close.equals("24:00")) { // midnight closing time
                        closeDate = date.plusDays(1);
                        close = "00:00";
                    } else if (Integer.parseInt(open.replaceAll(":", "")) > Integer
                        .parseInt(close.replaceAll(":", ""))) {
                        throw new IllegalArgumentException("Can not parse business periods; open = "
                            + open + " is greater than close = " + close);
                    } else {
                        closeDate = date;
                    }

                    final String openDateStr = date.toString() + "T" + open + tz;
                    final String closeDateStr = closeDate.toString() + "T" + close + tz;

                    businessPeriods[i++] =
                        new BusinessPeriod(DBTimeUtils.convertDateTime(openDateStr),
                            DBTimeUtils.convertDateTime(closeDateStr));
                }
            }
        }

        return businessPeriods;
    }

    @Override
    public List<String> getDefaultBusinessPeriods() {
        return Collections.unmodifiableList(defaultBusinessPeriodStrings);
    }

    @Override
    public Map<LocalDate, BusinessSchedule> getHolidays() {
        return Collections.unmodifiableMap(holidays);
    }

    @Override
    public boolean isBusinessDay(DayOfWeek day) {
        return !weekendDays.contains(day);
    }

    @Override
    public String name() {
        return calendarName;
    }

    @Override
    public DBTimeZone timeZone() {
        return timeZone;
    }

    @Override
    public long standardBusinessDayLengthNanos() {
        return lengthOfDefaultDayNanos;
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        final LocalDate localDate = LocalDate.ofYearDay(DBTimeUtils.year(time, timeZone()),
            DBTimeUtils.dayOfYear(time, timeZone()));

        return getBusinessSchedule(localDate);
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final String date) {
        if (date == null) {
            return null;
        }

        return getBusinessSchedule(DateStringUtils.parseLocalDate(date));
    }

    @Override
    @Deprecated
    public BusinessSchedule getBusinessDay(final LocalDate date) {
        return dates.computeIfAbsent(date, this::newBusinessDay);
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final DBDateTime time) {
        if (time == null) {
            return null;
        }

        final LocalDate localDate = LocalDate.ofYearDay(DBTimeUtils.year(time, timeZone()),
            DBTimeUtils.dayOfYear(time, timeZone()));

        return getBusinessSchedule(localDate);
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final String date) {
        if (date == null) {
            return null;
        }

        return getBusinessSchedule(DateStringUtils.parseLocalDate(date));
    }

    @Override
    public BusinessSchedule getBusinessSchedule(final LocalDate date) {
        return dates.computeIfAbsent(date, this::newBusinessDay);
    }

    private static String getText(Element element) {
        return element == null ? null : element.getTextTrim();
    }

    private BusinessSchedule newBusinessDay(final LocalDate date) {
        return newBusinessDay(date, weekendDays, timeZone(), defaultBusinessPeriodStrings);
    }

    private static BusinessSchedule newBusinessDay(final LocalDate date,
        final Set<DayOfWeek> weekendDays, final DBTimeZone timeZone,
        final List<String> businessPeriodStrings) {
        if (date == null) {
            return null;
        }

        // weekend
        final DayOfWeek dayOfWeek = date.getDayOfWeek();
        if (weekendDays.contains(dayOfWeek)) {
            return HOLIDAY;
        }
        return new BusinessSchedule(parseBusinessPeriods(timeZone, date, businessPeriodStrings));
    }

    @Override
    public long diffBusinessNanos(final DBDateTime start, final DBDateTime end) {
        if (start == null || end == null) {
            return QueryConstants.NULL_LONG;
        }
        if (DBTimeUtils.isAfter(start, end)) {
            return -diffBusinessNanos(end, start);
        }

        long dayDiffNanos = 0;
        DBDateTime day = start;
        while (!DBTimeUtils.isAfter(day, end)) {
            if (isBusinessDay(day)) {
                BusinessSchedule businessDate = getBusinessSchedule(day);

                if (businessDate != null) {
                    for (BusinessPeriod businessPeriod : businessDate.getBusinessPeriods()) {
                        DBDateTime endOfPeriod = businessPeriod.getEndTime();
                        DBDateTime startOfPeriod = businessPeriod.getStartTime();

                        // noinspection StatementWithEmptyBody
                        if (DBTimeUtils.isAfter(day, endOfPeriod)
                            || DBTimeUtils.isBefore(end, startOfPeriod)) {
                            // continue
                        } else if (!DBTimeUtils.isAfter(day, startOfPeriod)) {
                            if (DBTimeUtils.isBefore(end, endOfPeriod)) {
                                dayDiffNanos += DBTimeUtils.minus(end, startOfPeriod);
                            } else {
                                dayDiffNanos += businessPeriod.getLength();
                            }
                        } else {
                            if (DBTimeUtils.isAfter(end, endOfPeriod)) {
                                dayDiffNanos += DBTimeUtils.minus(endOfPeriod, day);
                            } else {
                                dayDiffNanos += DBTimeUtils.minus(end, day);
                            }
                        }
                    }
                }
            }
            day = getBusinessSchedule(nextBusinessDay(day)).getSOBD();
        }
        return dayDiffNanos;
    }

    @Override
    public double diffBusinessYear(final DBDateTime startTime, final DBDateTime endTime) {
        if (startTime == null || endTime == null) {
            return QueryConstants.NULL_DOUBLE;
        }

        double businessYearDiff = 0.0;
        DBDateTime time = startTime;
        while (!DBTimeUtils.isAfter(time, endTime)) {
            // get length of the business year
            final int startYear = DBTimeUtils.year(startTime, timeZone());
            final long businessYearLength =
                cachedYearLengths.computeIfAbsent(startYear, this::getBusinessYearLength);

            final DBDateTime endOfYear = getFirstBusinessDateTimeOfYear(startYear + 1);
            final long yearDiff;
            if (DBTimeUtils.isAfter(endOfYear, endTime)) {
                yearDiff = diffBusinessNanos(time, endTime);
            } else {
                yearDiff = diffBusinessNanos(time, endOfYear);
            }

            businessYearDiff += (double) yearDiff / (double) businessYearLength;
            time = endOfYear;
        }

        return businessYearDiff;
    }

    private long getBusinessYearLength(final int year) {
        int numDays = 365 + (DateStringUtils.isLeapYear(year) ? 1 : 0);
        long yearLength = 0;

        for (int j = 0; j < numDays; j++) {
            final int day = j + 1;
            final BusinessSchedule businessDate =
                getBusinessSchedule(LocalDate.ofYearDay(year, day));
            yearLength += businessDate.getLOBD();
        }

        return yearLength;
    }

    private DBDateTime getFirstBusinessDateTimeOfYear(final int year) {
        boolean isLeap = DateStringUtils.isLeapYear(year);
        int numDays = 365 + (isLeap ? 1 : 0);
        for (int j = 0; j < numDays; j++) {
            final BusinessSchedule businessDate =
                getBusinessSchedule(LocalDate.ofYearDay(year, j + 1));
            if (!(businessDate instanceof Holiday)) {
                return businessDate.getSOBD();
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "DefaultBusinessCalendar{" +
            "name='" + calendarName + '\'' +
            ", timeZone=" + timeZone +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DefaultBusinessCalendar that = (DefaultBusinessCalendar) o;
        return lengthOfDefaultDayNanos == that.lengthOfDefaultDayNanos &&
            Objects.equals(calendarName, that.calendarName) &&
            timeZone == that.timeZone &&
            Objects.equals(defaultBusinessPeriodStrings, that.defaultBusinessPeriodStrings) &&
            Objects.equals(weekendDays, that.weekendDays) &&
            Objects.equals(holidays, that.holidays);
    }

    @Override
    public int hashCode() {
        return Objects.hash(calendarName, timeZone, lengthOfDefaultDayNanos,
            defaultBusinessPeriodStrings, weekendDays, holidays);
    }

    static class CalendarElements {
        private String calendarName;
        private DBTimeZone timeZone;
        private long lengthOfDefaultDayNanos;
        private List<String> defaultBusinessPeriodStrings;
        private Set<DayOfWeek> weekendDays;
        private Map<LocalDate, BusinessSchedule> dates;
        private Map<LocalDate, BusinessSchedule> holidays;
        private boolean hasHolidays;
    }

    private static class Holiday extends BusinessSchedule implements Serializable {

        private static final long serialVersionUID = 5226852380875996172L;

        @Override
        public BusinessPeriod[] getBusinessPeriods() {
            return new BusinessPeriod[0];
        }

        @Override
        public DBDateTime getSOBD() {
            throw new UnsupportedOperationException("This is a holiday");
        }

        @Override
        public DBDateTime getEOBD() {
            throw new UnsupportedOperationException("This is a holiday");
        }

        @Override
        public long getLOBD() {
            return 0;
        }

        @Override
        public boolean isBusinessDay() {
            return false;
        }
    }
}
