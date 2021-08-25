package io.deephaven.util;

/**
 * {@link SafeCloseable} that will close non-null values inside of an array.
 *
 * The common use case is to create an array; use the SafeCloseableArray in an ignored try-with-resources variable, and
 * then populate the array within the loop. If you fail before populating the array nothing is closed, if you fail
 * during or after populating the array the created values are closed.
 */
public class SafeCloseableArray<T extends SafeCloseable> implements SafeCloseable {

    private final T[] array;

    public SafeCloseableArray(T[] entries) {
        array = entries;
    }

    @Override
    public final void close() {
        for (int ii = 0; ii < array.length; ii++) {
            if (array[ii] != null) {
                array[ii].close();
                array[ii] = null;
            }
        }
    }
}
