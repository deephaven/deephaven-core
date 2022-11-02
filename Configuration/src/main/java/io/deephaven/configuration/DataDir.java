package io.deephaven.configuration;

import java.nio.file.Path;
import java.util.Optional;

public class DataDir {
    public static final String PROPERTY = "deephaven.dataDir";
    @Deprecated
    private static final String WORKSPACE_PROPERTY = "workspace";
    public static final String ENV_VAR = "DEEPHAVEN_DATA_DIR";

    private static final String DEFAULT_DATA_DIR = ".";

    /**
     * Gets the data directory, first by the system property {@value #PROPERTY} if present, next by the system property
     * {@value #WORKSPACE_PROPERTY} if present, next by the environment variable {@value #ENV_VAR} if present, and
     * otherwise {@value #DEFAULT_DATA_DIR}.
     *
     * @return the data directory
     */
    public static Path get() {
        return Path.of(viaProperty()
                .or(DataDir::viaWorkspace)
                .or(DataDir::viaEnvironmentVariable)
                .orElse(DEFAULT_DATA_DIR));
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

    private static Optional<String> viaProperty() {
        return Optional.ofNullable(System.getProperty(PROPERTY));
    }

    private static Optional<String> viaWorkspace() {
        return Optional.ofNullable(System.getProperty(WORKSPACE_PROPERTY));
    }

    private static Optional<String> viaEnvironmentVariable() {
        return Optional.ofNullable(System.getenv(ENV_VAR));
    }
}
