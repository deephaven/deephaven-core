package io.deephaven.util;

/**
 * {@link AutoCloseable} sub-interface that does not throw a checked exception.
 */
public interface SafeCloseable extends AutoCloseable {

    static void closeArray(SafeCloseable... safeCloseables) {
        for (SafeCloseable sc : safeCloseables) {
            if (sc != null) {
                sc.close();
            }
        }
    }

    @Override
    void close();
}
