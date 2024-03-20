//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    @Override
    void close();

    /**
     * {@link #close() Close} all non-{@code null} {@link AutoCloseable} arguments.
     *
     * @param autoCloseables {@link AutoCloseable AutoCloseables} to {@link #close() close}
     */
    static void closeAll(@NotNull final AutoCloseable... autoCloseables) {
        closeAll(Arrays.asList(autoCloseables).iterator());
    }

    /**
     * {@link #close() Close} all non-{@code null} {@link AutoCloseable} elements. Terminates the {@code stream}.
     *
     * @param stream The stream of {@link AutoCloseable AutoCloseables} to {@link #close() close}
     * @param <ACT> the auto closable type
     */
    static <ACT extends AutoCloseable> void closeAll(@NotNull final Stream<ACT> stream) {
        closeAll(stream.iterator());
    }

    /**
     * {@link #close() Close} all non-{@code null} {@link AutoCloseable} elements. Consumes the {@code iterator}.
     *
     * @param iterator The iterator of {@link AutoCloseable AutoCloseables} to {@link #close() close}
     * @param <ACT> the auto closable type
     */
    static <ACT extends AutoCloseable> void closeAll(@NotNull final Iterator<ACT> iterator) {
        List<Exception> exceptions = null;
        while (iterator.hasNext()) {
            final AutoCloseable autoCloseable = iterator.next();
            if (autoCloseable == null) {
                continue;
            }
            try {
                autoCloseable.close();
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
     * {@link #close() Close} a single {@link AutoCloseable} argument if it is non-{@code null}.
     *
     * @param autoCloseable The {@link AutoCloseable} to {@link #close() close}
     */
    static void closeIfNonNull(@Nullable final AutoCloseable autoCloseable) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                throw new UncheckedDeephavenException("Exception while closing resource", e);
            }
        }
    }
}
