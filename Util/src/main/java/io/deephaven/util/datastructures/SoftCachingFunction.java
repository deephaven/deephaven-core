//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SoftCachingSupplier and run "./gradlew replicateCachingSupplier" to regenerate
//
// @formatter:off
package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.function.Function;

/**
 * {@link Function} wrapper that caches the result in a {@link SoftReference}. Only suitable to wrap functions that are
 * safely repeatable and don't return {@code null}.
 *
 * @param <OUTPUT_TYPE> the type of results supplied by this function
 */
public final class SoftCachingFunction<INPUT_TYPE, OUTPUT_TYPE> implements Function<INPUT_TYPE, OUTPUT_TYPE> {

    private final Function<INPUT_TYPE, OUTPUT_TYPE> internalFunction;

    private volatile SoftReference<OUTPUT_TYPE> cachedResultRef;

    /**
     * Construct a {@link Function} wrapper.
     *
     * @param internalFunction The {@link Function} to wrap. Must be safely repeatable and must not return {@code null}.
     */
    public SoftCachingFunction(@NotNull final Function<INPUT_TYPE, OUTPUT_TYPE> internalFunction) {
        this.internalFunction = internalFunction;
    }

    @Override
    public OUTPUT_TYPE apply(final INPUT_TYPE arg) {
        SoftReference<OUTPUT_TYPE> currentRef;
        OUTPUT_TYPE current;
        if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
            return current;
        }
        synchronized (this) {
            if ((currentRef = cachedResultRef) != null && (current = currentRef.get()) != null) {
                return current;
            }
            cachedResultRef = new SoftReference<>(current = internalFunction.apply(arg));
        }
        return current;
    }
}
