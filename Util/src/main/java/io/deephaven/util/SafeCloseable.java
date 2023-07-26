/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

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
        closeAll(Arrays.asList(safeCloseables).iterator());
    }

    /**
     * {@link #close() Close} all non-{@code null} SafeCloseable elements. Terminates the {@code stream}.
     *
     * @param stream the stream of SafeCloseables to {@link #close() close}
     * @param <SCT> the safe closable type
     */
    static <SCT extends SafeCloseable> void closeAll(@NotNull final Stream<SCT> stream) {
        closeAll(stream.iterator());
    }

    /**
     * {@link #close() Close} all non-{@code null} SafeCloseable elements. Consumes the {@code iterator}.
     *
     * @param iterator the iterator of SafeCloseables to {@link #close() close}
     * @param <SCT> the safe closable type
     */
    static <SCT extends SafeCloseable> void closeAll(@NotNull final Iterator<SCT> iterator) {
        List<Exception> exceptions = null;
        while (iterator.hasNext()) {
            final SafeCloseable safeCloseable = iterator.next();
            if (safeCloseable == null) {
                continue;
            }
            try {
                safeCloseable.close();
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
