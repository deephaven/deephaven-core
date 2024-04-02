//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * TimeZoneAliases provides a service to look up time zones based on alias names and to format time zones to their
 * aliased names.
 */
public class TimeZoneAliases {

    private static final Logger logger = LoggerFactory.getLogger(TimeZoneAliases.class);
    private static final TimeZoneAliases.Cache INSTANCE = load("timezone.aliases");

    /**
     * Creates a new time zone alias cache.
     */
    private static class Cache {
        final Map<String, String> aliasMap = new HashMap<>();
        final Map<String, String> reverseAliasMap = new HashMap<>();
        final Map<String, ZoneId> allZones = new HashMap<>();

        /**
         * Creates a new time zone alias cache initialized with all of the available time zones and no aliases.
         */
        private Cache() {
            for (String tz : ZoneId.getAvailableZoneIds()) {
                allZones.put(tz, ZoneId.of(tz));
            }
        }

        /**
         * Adds a new time zone alias.
         *
         * @param alias alias name
         * @param zoneId time zone id name
         * @throws IllegalArgumentException if the alias already exists or the time zone is invalid
         */
        public void addAlias(@NotNull final String alias, @NotNull final String zoneId) {
            Require.neqNull(alias, "alias");
            Require.neqNull(zoneId, "zoneId");

            if (allZones.containsKey(alias)) {
                throw new IllegalArgumentException("Time zone already exists with the alias name: alias=" + alias);
            }

            if (reverseAliasMap.containsKey(zoneId)) {
                throw new IllegalArgumentException("Alias for time zone is already present: time_zone=" + zoneId);
            }

            final ZoneId tz;

            try {
                tz = ZoneId.of(zoneId);
            } catch (Exception ex) {
                throw new IllegalArgumentException("Invalid time zone id: " + zoneId, ex);
            }

            aliasMap.put(alias, zoneId);
            reverseAliasMap.put(zoneId, alias);
            allZones.put(alias, tz);
        }

        /**
         * Removes a time zone alias.
         *
         * @param alias alias name.
         * @return true if the alias was present; false if the alias was not present.
         * @throws IllegalArgumentException if the alias already exists or the time zone is invalid
         */
        public boolean rmAlias(@NotNull final String alias) {
            Require.neqNull(alias, "alias");

            final ZoneId zid = allZones.remove(alias);

            if (zid == null) {
                return false;
            }

            final String zidStr = aliasMap.remove(alias);
            reverseAliasMap.remove(zidStr);

            return true;
        }

        /**
         * Gets the time zone id for a time zone name.
         *
         * @param timeZone time zone name.
         * @return time zone.
         */
        @NotNull
        public ZoneId zoneId(@NotNull final String timeZone) {
            Require.neqNull(timeZone, "timeZone");
            return ZoneId.of(timeZone, aliasMap);
        }

        /**
         * Gets the name for a time zone. If an alias is present, the alias is returned. If not, the zone id name is
         * returned.
         *
         * @param timeZone time zone.
         * @return name for the time zone. If an alias is present, the alias is returned. If not, the zone id name is
         *         returned.
         */
        @NotNull
        public String zoneName(@NotNull final ZoneId timeZone) {
            Require.neqNull(timeZone, "timeZone");
            return reverseAliasMap.getOrDefault(timeZone.getId(), timeZone.getId());
        }

        /**
         * Gets a map of all time zone names to their respective time zones.
         *
         * @return map of all time zone names to their respective time zones.
         */
        @NotNull
        public Map<String, ZoneId> getAllZones() {
            return allZones;
        }
    }

    /**
     * Gets a reader from a property. If the property points to a file, a reader to that file is returned; otherwise, a
     * reader to a resource in the JAR is returned.
     *
     * @param property property.
     * @return If the property points to a file, a reader to that file is returned; otherwise, a reader to a resource in
     *         the JAR is returned.
     * @throws RuntimeException if no reader can be returned.
     */
    private static Reader propertyToReader(final String property) {
        Require.neqNull(property, "property");
        final String location = Configuration.getInstance().getProperty(property);

        try {
            return new FileReader(location);
        } catch (FileNotFoundException ignored) {
        }

        final InputStream stream = TimeZoneAliases.class.getResourceAsStream(location);

        if (stream != null) {
            return new InputStreamReader(stream);
        }

        logger.error("Unable to open time zone alias property file: property=" + property + " location=" + location);
        throw new RuntimeException(
                "Unable to open time zone alias property file: property=" + property + " location=" + location);
    }

    /**
     * Loads a TimeZoneAlias#Cache from a property.
     *
     * @param property property specifying where the cache configuration is located.
     * @return cache
     * @throws RuntimeException if the cache can not be created.
     */
    @SuppressWarnings("SameParameterValue")
    private static Cache load(final String property) {
        final Cache cache = new Cache();
        final String location = Configuration.getInstance().getStringWithDefault(property, null);

        try {
            try (final BufferedReader br = new BufferedReader(propertyToReader(property))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("#") || line.startsWith("//") || line.isBlank()) {
                        continue;
                    }

                    String[] values = line.split(",");
                    if (values.length == 2) {
                        cache.addAlias(values[0], values[1]);
                    } else if (values.length == 1) {
                        throw new IllegalArgumentException("Line contains too few values: property=" + property
                                + " location=" + location + " line=" + line + " values=" + Arrays.toString(values));
                    } else if (values.length > 2) {
                        throw new IllegalArgumentException("Line contains too many values: property=" + property
                                + " location=" + location + " line=" + line + " values=" + Arrays.toString(values));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to load time zone aliases: property=" + property + "  location=" + location, e);
        }

        return cache;
    }

    /**
     * Gets the time zone id for a time zone name.
     *
     * @param timeZone time zone name.
     * @return time zone.
     */
    @NotNull
    public static ZoneId zoneId(@NotNull final String timeZone) {
        return INSTANCE.zoneId(timeZone);
    }

    /**
     * Gets the name for a time zone. If an alias is present, the alias is returned. If not, the zone id name is
     * returned.
     *
     * @param timeZone time zone.
     * @return name for the time zone. If an alias is present, the alias is returned. If not, the zone id name is
     *         returned.
     */
    @NotNull
    public static String zoneName(@NotNull final ZoneId timeZone) {
        return INSTANCE.zoneName(timeZone);
    }

    /**
     * Gets a map of all time zone names to their respective time zones.
     *
     * @return map of all time zone names to their respective time zones.
     */
    @NotNull
    public static Map<String, ZoneId> getAllZones() {
        return INSTANCE.getAllZones();
    }

    /**
     * Adds a new time zone alias.
     *
     * @param alias alias name
     * @param zoneId time zone id name
     * @throws IllegalArgumentException if the alias already exists or the time zone is invalid
     */
    public static void addAlias(@NotNull final String alias, @NotNull final String zoneId) {
        INSTANCE.addAlias(alias, zoneId);
    }

    /**
     * Removes a time zone alias.
     *
     * @param alias alias name.
     * @return true if the alias was present; false if the alias was not present.
     * @throws IllegalArgumentException if the alias already exists or the time zone is invalid
     */
    public static boolean rmAlias(@NotNull final String alias) {
        return INSTANCE.rmAlias(alias);
    }
}
