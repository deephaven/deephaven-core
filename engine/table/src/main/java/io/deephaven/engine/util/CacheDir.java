package io.deephaven.engine.util;

import io.deephaven.configuration.Configuration;

import java.nio.file.Path;

/**
 * The cache directory is a directory that the application may use for storing data with "cache-like" semantics.
 * Cache-like data is data that may be preserved across restarts, but the application logic should not make the
 * assumptions that the data will be available.
 */
public final class CacheDir {
    private static final String DEEPHAVEN_CACHE_DIR = "deephaven.cache.dir";
    private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

    private static final Path cacheDir;

    static {
        final Configuration config = Configuration.getInstance();
        if (config.hasProperty(DEEPHAVEN_CACHE_DIR)) {
            cacheDir = Path.of(config.getProperty(DEEPHAVEN_CACHE_DIR));
        } else {
            cacheDir = Path.of(System.getProperty(JAVA_IO_TMPDIR), "deephaven", "cache");
        }
    }

    /**
     * Return the value for the configuration {@value DEEPHAVEN_CACHE_DIR} if it is present.
     *
     * <p>
     * Otherwise, return "%s/deephaven/cache", parameterized from the value of the system property
     * {@value JAVA_IO_TMPDIR}.
     *
     * @return the cache dir
     */
    public static Path get() {
        return cacheDir;
    }
}
