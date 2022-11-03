/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.configuration;

import java.nio.file.Path;
import java.util.Optional;

/**
 * The cache directory is a directory that the application may use for storing data with "cache-like" semantics.
 * Cache-like data is data that may be preserved across restarts, but the application logic should not make the
 * assumptions that the data will be available.
 */
public final class CacheDir {
    public static final String PROPERTY = "deephaven.cacheDir";
    public static final String ENV_VAR = "DEEPHAVEN_CACHE_DIR";

    private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

    /**
     * Return the system property value for {@value CacheDir#PROPERTY} if it is present.
     *
     * <p>
     * Otherwise, return the environment value for {@value CacheDir#ENV_VAR} if it is present.
     *
     * <p>
     * Otherwise, return "%s/deephaven/cache", parameterized from the value of the system property
     * {@value JAVA_IO_TMPDIR}.
     *
     * @return the cache dir
     */
    public static Path get() {
        return viaProperty()
                .or(CacheDir::viaEnvVar)
                .map(Path::of)
                .orElseGet(CacheDir::viaTmpDir);
    }

    /**
     * Gets the cache directory if the system property {@value #PROPERTY} or environment variable {@value #ENV_VAR} is
     * present, otherwise sets the system property {@value #PROPERTY} to {@code defaultValue} and returns
     * {@code defaultValue}.
     *
     * @param defaultValue the value to set if none is present
     * @return the cache directory
     */
    public static Path getOrSet(String defaultValue) {
        final String existing = viaProperty().or(CacheDir::viaEnvVar).orElse(null);
        if (existing != null) {
            return Path.of(existing);
        }
        System.setProperty(PROPERTY, defaultValue);
        return Path.of(defaultValue);
    }

    private static Optional<String> viaProperty() {
        return Optional.ofNullable(System.getProperty(PROPERTY));
    }

    private static Optional<String> viaEnvVar() {
        return Optional.ofNullable(System.getenv(ENV_VAR));
    }

    private static Path viaTmpDir() {
        return Path.of(System.getProperty(JAVA_IO_TMPDIR), "deephaven", "cache");
    }
}
