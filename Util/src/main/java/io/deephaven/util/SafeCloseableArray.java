/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link SafeCloseable} that will close non-null values inside of an array.
 * <p>
 * The common use case is to create an array; use the SafeCloseableArray in an ignored try-with-resources variable, and
 * then populate the array within the loop. If you fail before populating the array nothing is closed, if you fail
 * during or after populating the array the created values are closed.
 */
public class SafeCloseableArray<SCT extends SafeCloseable> implements SafeCloseable {

    private final SCT[] array;

    public SafeCloseableArray(SCT[] entries) {
        array = entries;
    }

    @Override
    public final void close() {
        close(array);
    }

    /**
     * Close an array of {@link SafeCloseable} entries, ignoring {@code null} elements and assigning elements to
     * {@code null} as they are cleared.
     * 
     * @param array The array to operate one
     */
    public static <SCT extends SafeCloseable> void close(@NotNull final SCT[] array) {
        List<Exception> exceptions = null;
        for (int ii = 0; ii < array.length; ii++) {
            try (final SafeCloseable ignored = array[ii]) {
                array[ii] = null;
            } catch (Exception e) {
                (exceptions = new ArrayList<>()).add(e);
            }
        }
        // noinspection ConstantConditions
        if (exceptions != null) {
            throw new UncheckedDeephavenException("Exception while closing resources",
                    MultiException.maybeWrapInMultiException("Close exceptions for multiple resources", exceptions));
        }
    }
}
