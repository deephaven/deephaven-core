/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
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

import java.io.File;
import java.io.IOException;
import java.time.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A parser for reading business calendar XML files.
 *
 * Business calendar XML files should be formatted as:
 *
 * <pre>
 * {@code
 * <calendar>
 *     <name>USNYSE</name>
 *     <description>New York Stock Exchange Calendar</description>
 *     <timeZone>America/New_York</timeZone>
 *     <default>
 *          <businessTime><open>09:30</open><close>16:00</close></businessTime>
 *          <weekend>Saturday</weekend>
 *          <weekend>Sunday</weekend>
 *      </default>
 *      <firstValidDate>1999-01-01</firstValidDate>
 *      <lastValidDate>2003-12-31</lastValidDate>
 *      <holiday>
 *          <date>19990101</date>
 *      </holiday>
 *      <holiday>
 *          <date>20020705</date>
 *          <businessTime>
 *              <open>09:30</open>
 *              <close>13:00</close>
 *          </businessTime>
 *      </holiday>
 * </calendar>
 * }
 * </pre>
 */
class BusinessCalendarXMLParser {

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

    private static BusinessCalendarInputs parseBusinessCalendarInputs(@NotNull final File file) {
        Require.neqNull(file, "file");
        try {
            final BusinessCalendarInputs calendarElements = new BusinessCalendarInputs();

            Element root = loadXMLRootElement(file);
            calendarElements.calendarName = getText(getRequiredChild(root, "name"));
            calendarElements.timeZone = TimeZoneAliases.zoneId(getText(getRequiredChild(root, "timeZone")));
            calendarElements.description = getText(getRequiredChild(root, "description"));
            calendarElements.firstValidDate =
                    DateTimeUtils.parseLocalDate(getText(getRequiredChild(root, "firstValidDate")));
            calendarElements.lastValidDate =
                    DateTimeUtils.parseLocalDate(getText(getRequiredChild(root, "lastValidDate")));
            calendarElements.holidays = parseHolidays(root, calendarElements.timeZone);

            // Set the default values
            final Element defaultElement = getRequiredChild(root, "default");
            calendarElements.weekendDays = parseWeekendDays(defaultElement);
            calendarElements.standardBusinessDay = parseCalendarDaySchedule(defaultElement);

            return calendarElements;
        } catch (Exception e) {
            throw new RuntimeException("Unable to load calendar file: file=" + file.getPath(), e);
        }
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

    private static Element getRequiredChild(@NotNull final Element root, final String child) throws Exception {
        Element element = root.getChild(child);
        if (element != null) {
            return element;
        } else {
            throw new Exception("Missing the '" + child + "' tag in calendar file: text=" + root.getText());
        }
    }

    private static String getText(Element element) {
        return element == null ? null : element.getTextTrim();
    }

    private static CalendarDay<LocalTime> parseCalendarDaySchedule(final Element element) throws Exception {
        final List<Element> businessPeriods = element.getChildren("businessTime");
        return businessPeriods.isEmpty() ? CalendarDay.HOLIDAY
                : new CalendarDay<>(parseBusinessRanges(businessPeriods));
    }

    private static TimeRange<LocalTime>[] parseBusinessRanges(final List<Element> businessRanges)
            throws Exception {
        // noinspection unchecked
        final TimeRange<LocalTime>[] rst = new TimeRange[businessRanges.size()];

        for (int i = 0; i < businessRanges.size(); i++) {
            final String openTxt = getText(getRequiredChild(businessRanges.get(i), "open"));
            final String closeTxt = getText(getRequiredChild(businessRanges.get(i), "close"));

            if (closeTxt.startsWith("24:00")) {
                throw new RuntimeException("Close time (" + closeTxt
                        + ") is on the next day.  '23:59:59.999999999' is the maximum close time.");
            }

            final LocalTime open = DateTimeUtils.parseLocalTime(openTxt);
            final LocalTime close = DateTimeUtils.parseLocalTime(closeTxt);
            rst[i] = new TimeRange<>(open, close, true);
        }

        return rst;
    }

    private static Map<LocalDate, CalendarDay<Instant>> parseHolidays(final Element root, final ZoneId timeZone)
            throws Exception {
        final Map<LocalDate, CalendarDay<Instant>> holidays = new ConcurrentHashMap<>();
        final List<Element> holidayElements = root.getChildren("holiday");

        for (Element holidayElement : holidayElements) {
            final Element dateElement = getRequiredChild(holidayElement, "date");
            final LocalDate date = DateTimeUtils.parseLocalDate(getText(dateElement));
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
