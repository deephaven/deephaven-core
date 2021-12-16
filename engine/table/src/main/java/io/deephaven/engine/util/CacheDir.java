package io.deephaven.engine.util;

import java.nio.file.Path;

public final class CacheDir {
    private static final String DEEPHAVEN_CACHE_DIR = "deephaven.cache.dir";
    private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

    private static final Path cacheDir;

    static {
        final String deephavenCacheDir = System.getProperty(DEEPHAVEN_CACHE_DIR);
        if (deephavenCacheDir != null) {
            cacheDir = Path.of(deephavenCacheDir);
        } else {
            cacheDir = Path.of(System.getProperty(JAVA_IO_TMPDIR), "deephaven-cache");
        }
    }

    /**
     * Return the value for the system property {@value DEEPHAVEN_CACHE_DIR} if it is present.
     *
     * <p>
     * Otherwise, return "%s/deephaven-cache", parameterized from the value of the system property
     * {@value JAVA_IO_TMPDIR}.
     *
     * @return the cache dir
     */
    public static Path get() {
        return cacheDir;
    }
}
