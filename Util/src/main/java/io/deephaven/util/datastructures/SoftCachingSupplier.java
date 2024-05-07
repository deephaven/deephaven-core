//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.function.Supplier;

/**
 * {@link Supplier} wrapper that caches the result in a {@link SoftReference}. Only suitable to wrap suppliers that are
 * safely repeatable and don't return {@code null}.
 *
 * @param <OUTPUT_TYPE> the type of results supplied by this supplier
 */
public final class SoftCachingSupplier<OUTPUT_TYPE> implements Supplier<OUTPUT_TYPE> {

    private final Supplier<OUTPUT_TYPE> internalSupplier;

    private volatile SoftReference<OUTPUT_TYPE> cachedResultRef;

    /**
     * Construct a {@link Supplier} wrapper.
     *
     * @param internalSupplier The {@link Supplier} to wrap. Must be safely repeatable and must not return {@code null}.
     */
    public SoftCachingSupplier(@NotNull final Supplier<OUTPUT_TYPE> internalSupplier) {
        this.internalSupplier = internalSupplier;
    }

    @Override
    public OUTPUT_TYPE get() {
        SoftReference<OUTPUT_TYPE> currentRef;
        OUTPUT_TYPE current;
        if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
            return current;
        }
        synchronized (this) {
            if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
                return current;
            }
            cachedResultRef = new SoftReference<>(current = internalSupplier.get());
        }
        return current;
    }
}
