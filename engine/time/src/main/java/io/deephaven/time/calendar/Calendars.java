//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A collection of business calendars.
 */
public class Calendars {

    private static final Logger logger = LoggerFactory.getLogger(Calendars.class);
    private static final String BUSINESS_CALENDAR_PROP_INTERNAL = "Calendar.importPath";
    private static final String BUSINESS_CALENDAR_PROP_USER = "Calendar.userImportPath";
    private static String defaultName = Configuration.getInstance().getProperty("Calendar.default");
    private static final Map<String, BusinessCalendar> calMap = new TreeMap<>();

    /**
     * Loads the line-separated calendar XML resources from the resource file configuration value
     * {@value BUSINESS_CALENDAR_PROP_INTERNAL}. If the resource file configuration value
     * {@value BUSINESS_CALENDAR_PROP_USER} exists, those line-separated calendar XML resources will be returned as
     * well.
     *
     * @return the calendars
     * @see BusinessCalendarXMLParser#loadBusinessCalendarFromResource(String)
     */
    public static List<BusinessCalendar> calendarsFromConfiguration() {
        final Configuration configuration = Configuration.getInstance();
        final List<BusinessCalendar> configurationCalendars = new ArrayList<>(
                loadCalendarsFromResourceList(configuration.getProperty(BUSINESS_CALENDAR_PROP_INTERNAL)));
        if (configuration.hasProperty(BUSINESS_CALENDAR_PROP_USER)) {
            configurationCalendars.addAll(
                    loadCalendarsFromResourceList(configuration.getProperty(BUSINESS_CALENDAR_PROP_USER)));
        }
        return configurationCalendars;
    }

    private Calendars() {}

    // region Load

    private static List<BusinessCalendar> loadCalendarsFromResourceList(String resource) {
        final InputStream in = Calendars.class.getResourceAsStream(resource);
        if (in == null) {
            logger.warn("Could not find resource " + resource + " on classpath");
            throw new RuntimeException("Could not open resource " + resource + " from classpath");
        }
        final List<BusinessCalendar> calendars = new ArrayList<>();
        try (final BufferedReader config = new BufferedReader(new InputStreamReader(in))) {
            final Iterator<String> it = config.lines().iterator();
            while (it.hasNext()) {
                final String calendarResource = it.next();
                calendars.add(BusinessCalendarXMLParser.loadBusinessCalendarFromResource(calendarResource));
            }
            return calendars;
        } catch (Exception e) {
            logger.warn("Problem loading calendar: location=" + resource, e);
            throw new RuntimeException("Problem loading calendar: location=" + resource, e);
        }
    }

    /**
     * Removes a calendar from the collection.
     *
     * @param name calendar name
     * @throws RequirementFailure if the input is null
     */
    public synchronized static void removeCalendar(final String name) {
        Require.neqNull(name, "name");
        calMap.remove(name);
    }

    /**
     * Adds a calendar to the collection.
     *
     * @param cal business calendar
     * @throws RequirementFailure if the input is null
     */
    public synchronized static void addCalendar(final BusinessCalendar cal) {
        Require.neqNull(cal, "cal");

        final String name = cal.name().toUpperCase();
        if (!NameValidator.isValidQueryParameterName(name)) {
            throw new IllegalArgumentException("Invalid name for calendar: name='" + name + "'");
        }
        final Map<String, BusinessCalendar> map = calMap;

        if (map.containsKey(name)) {
            final Calendar oldCalendar = map.get(name);
            if (oldCalendar.equals(cal)) {
                return;
            }
            throw new IllegalArgumentException("Multiple calendars have the same name: name='" + name + "'");
        }

        map.put(name, cal);
    }

    /**
     * Adds a calendar to the collection from a file.
     *
     * @param file business calendar file
     * @throws RequirementFailure if the input is null
     */
    public static void addCalendarFromFile(final String file) {
        Require.neqNull(file, "file");
        addCalendarFromFile(new File(file));
    }

    /**
     * Adds a calendar to the collection from a file.
     *
     * @param file business calendar file
     * @throws RequirementFailure if the input is null
     */
    public static void addCalendarFromFile(final File file) {
        Require.neqNull(file, "file");

        if (file.getAbsolutePath().endsWith(".calendar")) {
            final BusinessCalendar cal = BusinessCalendarXMLParser.loadBusinessCalendar(file);
            addCalendar(cal);
        } else {
            throw new UnsupportedOperationException("Calendar file must be in .calendar format");
        }
    }

    private static File inputStreamToFile(@NotNull InputStream inputStream) throws IOException {
        File calendarFile = File.createTempFile("temp-file-name", ".calendar");
        FileOutputStream outputStream =
                new FileOutputStream(calendarFile);

        int read;
        byte[] bytes = new byte[1024];

        while ((read = inputStream.read(bytes)) != -1) {
            outputStream.write(bytes, 0, read);
        }

        outputStream.close();
        return calendarFile;
    }

    // end region

    // region Methods

    /**
     * Sets the default calendar by name. The calendar must already be present in the collection.
     *
     * @see #calendar() to get the default calendar
     * @see #calendarName() to get the name of the default calendar
     * @param name calendar name
     */
    public synchronized static void setCalendar(final String name) {
        Require.neqNull(name, "name");
        defaultName = name;
    }

    /**
     * Returns a business calendar.
     *
     * @param name name of the calendar. The name is case insensitive.
     * @return business calendar or {@code null} if {@code name} is {@code null}.
     * @throws IllegalArgumentException no calendar matching {@code name}
     * @throws RequirementFailure if the input is null
     */
    public synchronized static BusinessCalendar calendar(final String name) {
        if (name == null) {
            return null;
        }

        final String n = name.toUpperCase();
        final Map<String, BusinessCalendar> map = calMap;

        if (!map.containsKey(n)) {
            throw new IllegalArgumentException("No such calendar: " + name);
        }

        return map.get(n);
    }

    /**
     * Returns the default business calendar.
     *
     * @see #setCalendar(String) to set the default calendar
     * @see #calendarName() to get the name of the default calendar
     * @return default business calendar. The default is specified by the {@code Calendar.default} property or
     *         {@link #setCalendar(String)}.
     */
    public synchronized static BusinessCalendar calendar() {
        return calendar(defaultName);
    }

    /**
     * Returns the name of the default business calendar.
     *
     * @see #setCalendar(String) to set the default calendar
     * @see #calendar() to get the default calendar
     * @return name of the default business calendar. The default is specified by the {@code Calendar.default} property
     *         or {@link #setCalendar(String)}.
     */
    public synchronized static String calendarName() {
        return defaultName;
    }

    /**
     * Returns the names of all available calendars
     *
     * @return names of all available calendars
     */
    public synchronized static String[] calendarNames() {
        return calMap.keySet().toArray(String[]::new);
    }

    // endregion
}
