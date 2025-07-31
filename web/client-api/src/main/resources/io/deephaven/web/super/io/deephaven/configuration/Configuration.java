package io.deephaven.configuration;

/**
 * Simple configuration implementation that can only support methods with default values, and does no IO. Currently just
 * the most-used subset of supportable methods from io.deephaven.configuration.PropertyFile.
 */
public class Configuration {

    public static Configuration getInstance() {
        return new Configuration();
    }

    public boolean getBooleanWithDefault(final String propertyName, final boolean defaultValue) {
        return defaultValue;
    }

    public int getIntegerWithDefault(final String propertyName, final int defaultValue) {
        return defaultValue;
    }

    public long getLongWithDefault(final String propertyName, final long defaultValue) {
        return defaultValue;
    }

    public double getDoubleWithDefault(final String propertyName, final double defaultValue) {
        return defaultValue;
    }

    public String getStringWithDefault(final String propertyName, final String defaultValue) {
        return defaultValue;
    }

    public boolean getBooleanForClassWithDefault(final Class<?> clazz, final String propertyName,
            final boolean defaultValue) {
        return defaultValue;
    }

    public int getIntegerForClassWithDefault(final Class<?> clazz, final String propertyName, final int defaultValue) {
        return defaultValue;
    }

    public long getLongForClassWithDefault(final Class<?> clazz, final String propertyName, final long defaultValue) {
        return defaultValue;
    }

    public double getDoubleForClassWithDefault(final Class<?> clazz, final String propertyName,
            final double defaultValue) {
        return defaultValue;
    }

    public String getStringForClassWithDefault(final Class<?> clazz, final String propertyName,
            final String defaultValue) {
        return defaultValue;
    }
}
