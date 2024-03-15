//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.reference;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * {@link SimpleReference} implementation that delegates to an internal {@link SimpleReference} which can be replaced
 * using the {@link #swapDelegate(SimpleReference, SimpleReference)} method.
 */
public class SwappableDelegatingReference<T> implements SimpleReference<T> {

    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<SwappableDelegatingReference, SimpleReference> DELEGATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    SwappableDelegatingReference.class, SimpleReference.class, "delegate");

    private volatile SimpleReference<T> delegate;

    public SwappableDelegatingReference(@NotNull final SimpleReference<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    /**
     * Swap the delegate assigned to this SwappableDelegatingReference.
     *
     * @param oldDelegate The delegate to swap out
     * @param newDelegate The delegate to swap in
     * @throws IllegalArgumentException if {@code oldDelegate} is not the current delegate value
     */
    public void swapDelegate(
            @NotNull final SimpleReference<T> oldDelegate,
            @NotNull final SimpleReference<T> newDelegate) {
        if (!DELEGATE_UPDATER.compareAndSet(this, oldDelegate, newDelegate)) {
            throw new IllegalArgumentException(
                    "Previous delegate mismatch: found " + delegate + ", expected " + oldDelegate);
        }
    }

    @Override
    public T get() {
        return delegate.get();
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
