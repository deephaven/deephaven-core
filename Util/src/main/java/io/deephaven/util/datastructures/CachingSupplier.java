//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link Supplier} wrapper that caches the result.
 *
 * @param <OUTPUT_TYPE> the type of results supplied by this supplier
 */
public final class CachingSupplier<OUTPUT_TYPE> implements Supplier<OUTPUT_TYPE> {

    private final Supplier<OUTPUT_TYPE> internalSupplier;

    private volatile boolean hasCachedResult;
    private OUTPUT_TYPE cachedResult;
    private RuntimeException errorResult;

    /**
     * Construct a {@link Supplier} wrapper.
     *
     * @param internalSupplier The {@link Supplier} to wrap.
     */
    public CachingSupplier(@NotNull final Supplier<OUTPUT_TYPE> internalSupplier) {
        this.internalSupplier = internalSupplier;
    }

    @Override
    public OUTPUT_TYPE get() {
        if (!hasCachedResult) {
            synchronized (this) {
                if (!hasCachedResult) {
                    try {
                        cachedResult = internalSupplier.get();
                    } catch (RuntimeException err) {
                        errorResult = err;
                    }
                    hasCachedResult = true;
                }
            }
        }

        if (errorResult != null) {
            throw errorResult;
        }
        return cachedResult;
    }

    public OUTPUT_TYPE getIfCached() {
        if (hasCachedResult) { // force a volatile read
            if (errorResult != null) {
                throw errorResult;
            }
            return cachedResult;
        }
        return null;
    }
}
