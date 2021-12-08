package io.deephaven.integrations.python;

import java.util.Map;
import java.util.function.Function;

/**
 * Functions to support interfacing to python from java
 */
public class PythonTools {
    /**
     * Convert a map to a function.
     *
     * @param map the map to convert to function; the caller gives up ownership to the returned object
     * @param defaultValue the value to map to any keys not in the map; may be null
     * @return the resulting function
     */
    @SuppressWarnings("unused")
    public static Function<String, String> functionFromMapWithDefault(
            final Map<String, String> map,
            final String defaultValue) {
        if (map == null) {
            throw new IllegalArgumentException("Null map");
        }
        return (final String key) -> map.getOrDefault(key, defaultValue);
    }

    /**
     * Convert a map to a function, mapping any nonexistent keys in the map to themselves.
     *
     * @param map the map to convert to function; the caller gives up ownership to the returned object
     * @return the resulting function
     */
    @SuppressWarnings("unused")
    public static Function<String, String> functionFromMapWithIdentityDefaults(final Map<String, String> map) {
        if (map == null) {
            throw new IllegalArgumentException("Null map");
        }
        return (final String key) -> map.getOrDefault(key, key);
    }
}
