package io.deephaven.configuration;

import java.nio.file.Path;

public class DataDir {
    public static final String PROPERTY = "deephaven.dataDir";
    @Deprecated
    private static final String WORKSPACE_PROPERTY = "workspace";
    private static final String DEFAULT_DATA_DIR = ".";

    /**
     * Gets the data directory, first by the system property {@value #PROPERTY} if present, next by the system property
     * {@value #WORKSPACE_PROPERTY} if present, and otherwise {@value #DEFAULT_DATA_DIR}.
     *
     * @return the data directory
     */
    public static Path get() {
        return Path.of(System.getProperty(PROPERTY, System.getProperty(WORKSPACE_PROPERTY, DEFAULT_DATA_DIR)));
    }

    /**
     * Gets the data directory if the system property {@value #PROPERTY} or {@value WORKSPACE_PROPERTY} is present,
     * otherwise sets the system property {@value #PROPERTY} to {@code defaultValue} and returns {@code defaultValue}.
     *
     * @param defaultValue the value to set if none is present
     * @return the data directory
     */
    public static Path getOrSet(String defaultValue) {
        final String existing = System.getProperty(PROPERTY, System.getProperty(WORKSPACE_PROPERTY, null));
        if (existing != null) {
            return Path.of(existing);
        }
        System.setProperty(PROPERTY, defaultValue);
        return Path.of(defaultValue);
    }
}
