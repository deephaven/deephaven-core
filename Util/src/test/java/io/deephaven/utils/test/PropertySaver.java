package io.deephaven.utils.test;

import io.deephaven.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple utility class for use in unit tests that need to adjust properties, and then put them
 * back. Meant to be used in a try { } finally { } block; because java has no good RAII.
 */
public class PropertySaver {
    private final Map<String, String> savedProperties = new HashMap<>();

    public PropertySaver() {}

    public PropertySaver setAll(Map<String, String> values) {
        values.forEach(this::setProperty);
        return this;
    }

    public PropertySaver setProperty(String property, String value) {
        if (Configuration.getInstance().hasProperty(property)) {
            savedProperties.put(property, Configuration.getInstance().getProperty(property));
        } else {
            savedProperties.put(property, null);
        }
        Configuration.getInstance().setProperty(property, value);
        return this;
    }

    public void remove(String property) {
        if (Configuration.getInstance().hasProperty(property)) {
            savedProperties.put(property,
                Configuration.getInstance().getProperties().remove(property).toString());
        }
    }

    public void restore() {
        savedProperties.forEach((k, v) -> Configuration.getInstance().setProperty(k, v));
        savedProperties.clear();
    }
}
