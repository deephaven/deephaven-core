//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link SafeCloseable} that will close non-null values inside an array.
 * <p>
 * The common use case is to create an array; use the SafeCloseableArray in an ignored try-with-resources variable, and
 * then populate the array within the loop. If the operation fails before populating the array nothing is closed. If the
 * operation fails during or after populating the array the populated values are closed.
 */
public class SafeCloseableArray<ACT extends AutoCloseable> implements SafeCloseable {

    private final ACT[] array;

    public SafeCloseableArray(ACT[] entries) {
        array = entries;
    }

    @Override
    public final void close() {
        close(array);
    }

    /**
     * Close an array of {@link AutoCloseable} entries, ignoring {@code null} elements and assigning elements to
     * {@code null} as they are cleared.
     *
     * @param array The array to operate one
     */
    public static <ACT extends AutoCloseable> void close(final ACT @NotNull [] array) {
        final int length = array.length;
        List<Exception> exceptions = null;
        for (int ii = 0; ii < length; ii++) {
            try (final AutoCloseable ignored = array[ii]) {
                array[ii] = null;
            } catch (Exception e) {
                if (exceptions == null) {
                    exceptions = new ArrayList<>();
                }
                exceptions.add(e);
            }
        }
        // noinspection ConstantConditions
        if (exceptions != null) {
            throw new UncheckedDeephavenException("Exception while closing resources",
                    MultiException.maybeWrapInMultiException("Close exceptions for multiple resources", exceptions));
        }
    }
}
