/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.calendar;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.util.files.ResourceResolution;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A collection of business calendars.
 */
public class Calendars implements Map<String, BusinessCalendar> {

    private static final Logger logger = LoggerFactory.getLogger(Calendars.class);
    private static final String SUFFIX = ".calendar";
    private static final String BUSINESS_CALENDAR_PROP_INTERNAL = "Calendar.internalPath";
    private static final String BUSINESS_CALENDAR_PROP_USER = "Calendar.resourcePath";
    private static final Calendars instance = new Calendars();
    private static final String defaultName = Configuration.getInstance().getProperty("Calendar.default");

    /**
     * Gets the singleton map of business calendars.
     *
     * @return singleton map of calendars
     */
    static Calendars getInstance() {
        return instance;
    }

    /**
     * Returns a business calendar.
     *
     * @param name name of the calendar
     * @return business calendar
     * @throws IllegalArgumentException no calendar matching {@code name}
     */
    public static BusinessCalendar calendar(final String name) {
        Require.neqNull(name, "name");

        final String n = name.toUpperCase();

        if(!instance.containsKey(n)){
            throw new IllegalArgumentException("No such calendar: " + name);
        }

        return instance.get(n);
    }

    /**
     * Returns a business calendar.
     *
     * @return default business calendar.  The deault is specified by the {@code Calendar.default} property.
     */
    public static BusinessCalendar calendar() {
        return calendar(defaultName);
    }

    /**
     * Returns the default calendar name
     *
     * @return default business calendar name
     */
    public static String getDefaultName() {
        return defaultName;
    }

    /**
     * Returns the names of all available calendars
     *
     * @return names of all available calendars
     */
    public static String[] calendarNames() {
        return instance.keySet().stream().toArray(String[]::new);
    }



    private final Map<String, BusinessCalendar> calendars = new HashMap<>();


    private Calendars() {
        final Configuration configuration = Configuration.getInstance();


        loadProperty(configuration, BUSINESS_CALENDAR_PROP_INTERNAL);

        if(configuration.hasProperty(BUSINESS_CALENDAR_PROP_USER)){
            loadProperty(configuration, BUSINESS_CALENDAR_PROP_USER);
        }
    }

    private void loadProperty(final Configuration configuration, final String property) {
        final String locations = configuration.getProperty(property);
        try {
            load(configuration, locations);
        } catch (NoSuchFileException e) {
            logger.warn().append("Problem loading calendars. locations=").append(locations).append(e).endl();
        }
    }

    private void load(final Configuration configuration, final String businessCalendarLocations) throws NoSuchFileException {
        final ResourceResolution resourceResolution = new io.deephaven.util.files.ResourceResolution(configuration, ";", businessCalendarLocations);

        final BiConsumer<URL, String> consumer = (URL, filePath) -> {
            try {
                final InputStream inputStream = URL.openStream();
                if (inputStream != null) {
                    final File calendarFile = inputStreamToFile(inputStream);
                    final BusinessCalendar businessCalendar = DefaultBusinessCalendar.getInstance(calendarFile);
                    addCalendar(businessCalendar);
                    calendarFile.deleteOnExit();
                } else {
                    logger.warn("Could not open " + URL);
                    throw new RuntimeException("Could not open " + URL);
                }
            } catch (IOException e) {
                logger.warn("Problem loading calendar: locations=" + businessCalendarLocations, e);
                throw new RuntimeException("Problem loading calendar: locations=" + businessCalendarLocations, e);
            }
        };


        try {
            resourceResolution.findResources(SUFFIX, consumer);
        } catch (NoSuchFileException e){
            logger.warn("Problem loading calendar: locations=" + businessCalendarLocations, e);
            throw e;
        } catch (IOException e) {
            logger.warn("Problem loading calendar: locations=" + businessCalendarLocations, e);
            throw new RuntimeException("Problem loading calendar: locations=" + businessCalendarLocations, e);
        }
    }

    private void addCalendar(final BusinessCalendar cal){
        final String name = cal.name().toUpperCase();
        try {
            NameValidator.validateQueryParameterName(name);
        } catch(NameValidator.InvalidNameException e){
            throw new IllegalArgumentException("Invalid name for calendar: name='" + name + "'" );
        }

        if(containsKey(name)){
            final Calendar oldCalendar = get(name);
            if(oldCalendar.equals(cal)) {
                return;
            }
            throw new IllegalArgumentException("Multiple calendars have the same name: name='" + name + "'");
        }

        put(name, cal);
    }

    /**
     * Adds a calendar to the collection from the {@code filePath}
     *
     * @param filePath must be xml format
     */
    public void addCalendarFromFile(final String filePath) {
        addCalendarFromFile(new File(filePath));
    }

    /**
     * Adds a calendar to the collection from the {@code file}
     *
     * @param file must be xml format
     */
    public void addCalendarFromFile(final File file) {
        if (file.getAbsolutePath().endsWith(".calendar")) {
            final BusinessCalendar cal = DefaultBusinessCalendar.getInstance(file);
            addCalendar(cal);
        } else {
            throw new UnsupportedOperationException("Calendar file must be in .calendar format");
        }
    }

    @Override
    public int size() {
        return calendars.size();
    }

    @Override
    public boolean isEmpty() {
        return calendars.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return !(key == null || !key.getClass().isAssignableFrom(String.class)) && calendars.containsKey(((String) key).toUpperCase());

    }

    @Override
    public boolean containsValue(Object value) {
        return calendars.containsValue(value);
    }

    @Override
    public BusinessCalendar get(Object key) {
        return calendars.get(key);
    }

    @Override
    public BusinessCalendar put(String key, BusinessCalendar value) {
        Require.neqNull(key, "key");
        key = key.toUpperCase();
        return calendars.put(key, value);
    }

    @Override
    public BusinessCalendar remove(Object key) {
        return calendars.remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends BusinessCalendar> m) {
        calendars.putAll(m);
    }

    @Override
    public void clear() {
        calendars.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return calendars.keySet();
    }

    @NotNull
    @Override
    public Collection<BusinessCalendar> values() {
        return calendars.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, BusinessCalendar>> entrySet() {
        return calendars.entrySet();
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
}
