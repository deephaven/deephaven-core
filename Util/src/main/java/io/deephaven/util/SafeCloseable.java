/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link AutoCloseable} sub-interface that does not throw a checked exception.
 */
public interface SafeCloseable extends AutoCloseable {

    /**
     * {@link #close() Close} all non-{@code null} SafeCloseable arguments.
     *
     * @param safeCloseables SafeCloseables to {@link #close() close}
     */
    static void closeAll(@NotNull final SafeCloseable... safeCloseables) {
        List<Exception> exceptions = null;
        for (final SafeCloseable safeCloseable : safeCloseables) {
            if (safeCloseable == null) {
                continue;
            }
            try {
                safeCloseable.close();
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

    /**
     * {@link #close() Close} a single SafeCloseable argument if it is non-{@code null}.
     *
     * @param safeCloseable The SafeCloseable to {@link #close() close}
     */
    static void closeIfNonNull(@Nullable final SafeCloseable safeCloseable) {
        if (safeCloseable != null) {
            safeCloseable.close();
        }
    }

    @Override
    void close();
}
