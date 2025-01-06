//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.time.TimeZoneAliases;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A parser for reading business calendar XML files.
 *
 * <p>
 * Business calendar XML files should be formatted as:
 *
 * <pre>
 * {@code
 * <calendar>
 *     <name>USNYSE</name>
 *     <!-- Optional description -->
 *     <description>New York Stock Exchange Calendar</description>
 *     <timeZone>America/New_York</timeZone>
 *     <default>
 *          <businessTime>
 *              <open>09:30</open>
 *              <close>16:00</close>
 *              <!-- Optional include the close nanosecond in the business time range. -->
 *              <includeClose>true</includeClose>
 *          </businessTime>
 *          <weekend>Saturday</weekend>
 *          <weekend>Sunday</weekend>
 *      </default>
 *      <!-- Optional firstValidDate.  Defaults to the first holiday. -->
 *      <firstValidDate>1999-01-01</firstValidDate>
 *      <!-- Optional lastValidDate.  Defaults to the first holiday. -->
 *      <lastValidDate>2003-12-31</lastValidDate>
 *      <holiday>
 *          <date>1999-01-01</date>
 *      </holiday>
 *      <holiday>
 *          <date>2002-07-05</date>
 *          <businessTime>
 *              <open>09:30</open>
 *              <close>13:00</close>
 *              <!-- Optional include the close nanosecond in the business time range. -->
 *              <includeClose>true</includeClose>
 *          </businessTime>
 *      </holiday>
 * </calendar>
 * }
 * </pre>
 *
 * In addition, legacy XML files are supported. These files have dates formatted as `yyyyMMdd` instead of ISO-8601
 * `yyy-MM-dd`. Additionally, legacy uses `businessPeriod` tags in place of the `businessTime` tags.
 *
 * <pre>
 * {@code
 * <!-- Current format -->
 * <businessTime><open>09:30</open><close>16:00</close></businessTime>
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * <!-- Legacy format -->
 * <businessPeriod>09:30,16:00</businessPeriod>
 * }
 * </pre>
 *
 * The legacy format may be deprecated in a future release.
 */
public final class BusinessCalendarXMLParser {

    private static class BusinessCalendarInputs {
        private String calendarName;
        private String description;
        private ZoneId timeZone;
        private LocalDate firstValidDate;
        private LocalDate lastValidDate;
        private CalendarDay<LocalTime> standardBusinessDay;
        private Set<DayOfWeek> weekendDays;
        private Map<LocalDate, CalendarDay<Instant>> holidays;
    }

    /**
     * Loads a business calendar from an XML file.
     *
     * @param file XML file
     * @return business calendar.
     * @throws RequirementFailure if the input is null
     */
    public static BusinessCalendar loadBusinessCalendar(@NotNull final String file) {
        Require.neqNull(file, "file");
        return loadBusinessCalendar(new File(file));
    }

    /**
     * Loads a business calendar from an XML file.
     *
     * @param file XML file
     * @return business calendar.
     * @throws RequirementFailure if the input is null
     */
    public static BusinessCalendar loadBusinessCalendar(@NotNull final File file) {
        Require.neqNull(file, "file");
        final BusinessCalendarInputs in = parseBusinessCalendarInputs(file);
        return new BusinessCalendar(in.calendarName, in.description,
                in.timeZone, in.firstValidDate, in.lastValidDate,
                in.standardBusinessDay, in.weekendDays, in.holidays);
    }

    /**
     * Loads a business calendar from an XML input stream.
     *
     * @param inputStream XML input stream
     * @return business calendar.
     * @throws RequirementFailure if the input is null
     */
    public static BusinessCalendar loadBusinessCalendar(@NotNull final InputStream inputStream) {
        Require.neqNull(inputStream, "inputStream");
        final BusinessCalendarInputs in = parseBusinessCalendarInputs(inputStream);
        return new BusinessCalendar(in.calendarName, in.description,
                in.timeZone, in.firstValidDate, in.lastValidDate,
                in.standardBusinessDay, in.weekendDays, in.holidays);
    }

    /**
     * Loads a business calendar from an XML resource.
     *
     * @param resource XML input stream
     * @return business calendar.
     */
    public static BusinessCalendar loadBusinessCalendarFromResource(String resource) throws IOException {
        final InputStream in = Calendars.class.getResourceAsStream(resource);
        if (in == null) {
            throw new RuntimeException("Could not open resource " + resource + " from classpath");
        }
        try (final InputStream bin = new BufferedInputStream(in)) {
            return loadBusinessCalendar(bin);
        }
    }

    private static BusinessCalendarInputs parseBusinessCalendarInputs(@NotNull final File file) {
        Require.neqNull(file, "file");
        try {
            return fill(loadXMLRootElement(file));
        } catch (Exception e) {
            throw new RuntimeException("Unable to load calendar file: file=" + file.getPath(), e);
        }
    }

    private static BusinessCalendarInputs parseBusinessCalendarInputs(@NotNull final InputStream in) {
        Require.neqNull(in, "in");
        try {
            return fill(loadXMLRootElement(in));
        } catch (Exception e) {
            throw new RuntimeException("Unable to load calendar file: inputStream=" + in, e);
        }
    }

    private static BusinessCalendarInputs fill(Element root) throws Exception {
        final BusinessCalendarInputs calendarElements = new BusinessCalendarInputs();
        calendarElements.calendarName = getText(getRequiredChild(root, "name"));
        calendarElements.timeZone = TimeZoneAliases.zoneId(getText(getRequiredChild(root, "timeZone")));
        calendarElements.description = getText(root.getChild("description"));
        calendarElements.holidays = parseHolidays(root, calendarElements.timeZone);
        final String firstValidDateStr = getText(root.getChild("firstValidDate"));
        calendarElements.firstValidDate =
                firstValidDateStr == null ? Collections.min(calendarElements.holidays.keySet())
                        : DateTimeUtils.parseLocalDate(firstValidDateStr);
        final String lastValidDateStr = getText(root.getChild("lastValidDate"));
        calendarElements.lastValidDate =
                lastValidDateStr == null ? Collections.max(calendarElements.holidays.keySet())
                        : DateTimeUtils.parseLocalDate(lastValidDateStr);

        // Set the default values
        final Element defaultElement = getRequiredChild(root, "default");
        calendarElements.weekendDays = parseWeekendDays(defaultElement);
        calendarElements.standardBusinessDay = parseCalendarDaySchedule(defaultElement);
        return calendarElements;
    }

    private static Element loadXMLRootElement(File calendarFile) throws Exception {
        final Document doc;

        try {
            final SAXBuilder builder = new SAXBuilder();
            doc = builder.build(calendarFile);
        } catch (JDOMException e) {
            throw new Exception("Error parsing business calendar: file=" + calendarFile, e);
        } catch (IOException e) {
            throw new Exception("Error loading business calendar: file=" + calendarFile, e);
        }

        return doc.getRootElement();
    }

    private static Element loadXMLRootElement(InputStream in) throws Exception {
        final Document doc;

        try {
            final SAXBuilder builder = new SAXBuilder();
            doc = builder.build(in);
        } catch (JDOMException e) {
            throw new Exception("Error parsing business calendar: inputStream=" + in, e);
        } catch (IOException e) {
            throw new Exception("Error loading business calendar: inputStream=" + in, e);
        }

        return doc.getRootElement();
    }

    private static Element getRequiredChild(@NotNull final Element root, final String child) throws Exception {
        Element element = root.getChild(child);
        if (element != null) {
            return element;
        } else {
            throw new Exception("Missing the '" + child + "' tag in calendar file: text=" + root.getTextTrim());
        }
    }

    private static String getText(Element element) {
        return element == null ? null : element.getTextTrim();
    }

    private static CalendarDay<LocalTime> parseCalendarDaySchedule(final Element element) throws Exception {
        final List<Element> businessTimes = element.getChildren("businessTime");
        final List<Element> businessPeriods = element.getChildren("businessPeriod");

        if (businessTimes.isEmpty() && businessPeriods.isEmpty()) {
            return CalendarDay.HOLIDAY;
        } else if (!businessTimes.isEmpty() && businessPeriods.isEmpty()) {
            return new CalendarDay<>(parseBusinessRanges(businessTimes));
        } else if (businessTimes.isEmpty() && !businessPeriods.isEmpty()) {
            return new CalendarDay<>(parseBusinessRangesLegacy(businessPeriods));
        } else {
            throw new Exception("Cannot have both 'businessTime' and 'businessPeriod' tags in the same element: text="
                    + element.getTextTrim());
        }
    }

    private static TimeRange<LocalTime>[] parseBusinessRanges(final List<Element> businessRanges)
            throws Exception {
        // noinspection unchecked
        final TimeRange<LocalTime>[] rst = new TimeRange[businessRanges.size()];

        for (int i = 0; i < businessRanges.size(); i++) {
            final String openTxt = getText(getRequiredChild(businessRanges.get(i), "open"));
            final String closeTxt = getText(getRequiredChild(businessRanges.get(i), "close"));
            final String includeCloseTxt = getText(businessRanges.get(i).getChild("includeClose"));

            if (closeTxt.startsWith("24:00")) {
                throw new RuntimeException("Close time (" + closeTxt
                        + ") is on the next day.  '23:59:59.999999999' is the maximum close time.");
            }

            final LocalTime open = DateTimeUtils.parseLocalTime(openTxt);
            final LocalTime close = DateTimeUtils.parseLocalTime(closeTxt);
            final boolean inclusiveEnd = Boolean.parseBoolean(includeCloseTxt); // defaults to false
            rst[i] = new TimeRange<>(open, close, inclusiveEnd);
        }

        return rst;
    }

    private static TimeRange<LocalTime>[] parseBusinessRangesLegacy(final List<Element> businessRanges)
            throws Exception {
        // noinspection unchecked
        final TimeRange<LocalTime>[] rst = new TimeRange[businessRanges.size()];
        int i = 0;

        for (Element br : businessRanges) {
            final String[] openClose = br.getTextTrim().split(",");

            if (openClose.length == 2) {
                final String openTxt = openClose[0];
                final String closeTxt = openClose[1];
                final LocalTime open = DateTimeUtils.parseLocalTime(openTxt);
                final LocalTime close = DateTimeUtils.parseLocalTime(closeTxt);
                rst[i] = new TimeRange<>(open, close, false);
            } else {
                throw new IllegalArgumentException("Can not parse business periods; open/close = " + br.getText());
            }

            i++;
        }

        return rst;
    }

    private static Map<LocalDate, CalendarDay<Instant>> parseHolidays(final Element root, final ZoneId timeZone)
            throws Exception {
        final Map<LocalDate, CalendarDay<Instant>> holidays = new ConcurrentHashMap<>();
        final List<Element> holidayElements = root.getChildren("holiday");

        for (Element holidayElement : holidayElements) {
            final Element dateElement = getRequiredChild(holidayElement, "date");
            String dateStr = getText(dateElement);

            // Convert yyyyMMdd to yyyy-MM-dd
            if (dateStr.length() == 8) {
                dateStr = dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
            }

            final LocalDate date = DateTimeUtils.parseLocalDate(dateStr);
            final CalendarDay<LocalTime> schedule = parseCalendarDaySchedule(holidayElement);
            holidays.put(date, CalendarDay.toInstant(schedule, date, timeZone));
        }

        return holidays;
    }

    private static Set<DayOfWeek> parseWeekendDays(@NotNull final Element defaultElement) throws Exception {
        final Set<DayOfWeek> weekendDays = new HashSet<>();

        final List<Element> weekends = defaultElement.getChildren("weekend");
        if (weekends != null) {
            for (Element weekendElement : weekends) {
                final String weekend = getText(weekendElement);
                final String dows = weekend.trim().toUpperCase();
                final DayOfWeek dow;

                try {
                    dow = DayOfWeek.valueOf(dows);
                } catch (IllegalArgumentException e) {
                    throw new Exception("Invalid day of week: day=" + dows, e);
                }

                weekendDays.add(dow);
            }
        }

        return weekendDays;
    }

}
