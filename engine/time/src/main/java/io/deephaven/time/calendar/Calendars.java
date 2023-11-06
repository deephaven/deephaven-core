/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A collection of business calendars.
 */
public class Calendars {

    private static final Logger logger = LoggerFactory.getLogger(Calendars.class);
    private static final String BUSINESS_CALENDAR_PROP_INTERNAL = "Calendar.importPath";
    private static final String BUSINESS_CALENDAR_PROP_USER = "Calendar.userImportPath";
    private static String defaultName = Configuration.getInstance().getProperty("Calendar.default");

    private static final Map<String, BusinessCalendar> map = new HashMap<>();
    private static String[] names = new String[0];

    private Calendars() {}

    // region Load

    static {
        final Configuration configuration = Configuration.getInstance();

        loadProperty(configuration, BUSINESS_CALENDAR_PROP_INTERNAL);

        if (configuration.hasProperty(BUSINESS_CALENDAR_PROP_USER)) {
            loadProperty(configuration, BUSINESS_CALENDAR_PROP_USER);
        }
    }

    private static void loadProperty(final Configuration configuration, final String property) {
        final String location = configuration.getProperty(property);
        try {
            load(location);
        } catch (NoSuchFileException e) {
            logger.warn().append("Problem loading calendars. importPath=").append(location).append(e).endl();
        }
    }

    private static void load(final String businessCalendarConfig) throws NoSuchFileException {
        final InputStream configToLoad = Calendars.class.getResourceAsStream(businessCalendarConfig);

        if (configToLoad == null) {
            logger.warn("Could not find " + businessCalendarConfig + " on classpath");
            throw new RuntimeException("Could not open " + businessCalendarConfig + " from classpath");
        }

        final Consumer<String> consumer = (filePath) -> {
            try {
                final InputStream inputStream = Calendars.class.getResourceAsStream(filePath);
                if (inputStream != null) {
                    final File calendarFile = inputStreamToFile(inputStream);
                    final BusinessCalendar businessCalendar = BusinessCalendarParser.loadBusinessCalendar(calendarFile);
                    addCalendar(businessCalendar);
                    // noinspection ResultOfMethodCallIgnored
                    calendarFile.delete();
                } else {
                    logger.warn("Could not open " + filePath + " from classpath");
                    throw new RuntimeException("Could not open " + filePath + " from classpath");
                }
            } catch (IOException e) {
                logger.warn("Problem loading calendar: location=" + businessCalendarConfig, e);
                throw new RuntimeException("Problem loading calendar: location=" + businessCalendarConfig, e);
            }
        };

        try (final BufferedReader config = new BufferedReader(new InputStreamReader(configToLoad))) {
            config.lines().forEach(consumer);
        } catch (NoSuchFileException e) {
            logger.warn("Problem loading calendar: location=" + businessCalendarConfig, e);
            throw e;
        } catch (IOException e) {
            logger.warn("Problem loading calendar: location=" + businessCalendarConfig, e);
            throw new RuntimeException("Problem loading calendar: location=" + businessCalendarConfig, e);
        }
    }

    private synchronized static void addCalendar(final BusinessCalendar cal) {
        final String name = cal.name().toUpperCase();
        if (!NameValidator.isValidQueryParameterName(name)) {
            throw new IllegalArgumentException("Invalid name for calendar: name='" + name + "'");
        }

        if (map.containsKey(name)) {
            final Calendar oldCalendar = map.get(name);
            if (oldCalendar.equals(cal)) {
                return;
            }
            throw new IllegalArgumentException("Multiple calendars have the same name: name='" + name + "'");
        }

        map.put(name, cal);

        names = map.keySet().toArray(String[]::new);
        Arrays.sort(names);
    }

    /**
     * Adds a calendar to the collection from a file.
     *
     * @param file business calendar file
     */
    public static void addCalendarFromFile(final String file) {
        addCalendarFromFile(new File(file));
    }

    /**
     * Adds a calendar to the collection from a file.
     *
     * @param file business calendar file
     */
    public static void addCalendarFromFile(final File file) {
        if (file.getAbsolutePath().endsWith(".calendar")) {
            final BusinessCalendar cal = BusinessCalendarParser.loadBusinessCalendar(file);
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
     * Sets the default calendar name.
     *
     * @param name calendar name
     */
    public synchronized static void setDefaultCalendar(final String name) {
        Require.neqNull(name, "name");
        defaultName = name;
    }

    /**
     * Returns a business calendar.
     *
     * @param name name of the calendar
     * @return business calendar
     * @throws IllegalArgumentException no calendar matching {@code name}
     * @throws RequirementFailure if the input is null
     */
    public synchronized static BusinessCalendar calendar(final String name) {
        Require.neqNull(name, "name");

        final String n = name.toUpperCase();

        if (!map.containsKey(n)) {
            throw new IllegalArgumentException("No such calendar: " + name);
        }

        return map.get(n);
    }

    /**
     * Returns the default business calendar.
     *
     * @return default business calendar. The default is specified by the {@code Calendar.default} property or
     *         {@link #setDefaultCalendar(String)}.
     */
    public synchronized static BusinessCalendar calendar() {
        return calendar(defaultName);
    }

    /**
     * Returns the default business calendar name.
     *
     * @return default business calendar name. The default is specified by the {@code Calendar.default} property or
     *         {@link #setDefaultCalendar(String)}.
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
        return names;
    }

    // endregion
}
