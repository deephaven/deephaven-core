//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.pool.Pool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A pool that
 * <UL>
 * <LI>holds at max <code>maxSize</code> items,
 * <LI>starts with no items in the pool,
 * <LI>blocks (busily) whenever the pool overflows,
 * <LI>optionally clears the items given to it, and
 * <LI>is thread-safe
 * </UL>
 */
public final class ThreadSafeMaxSizePool<ELEMENT_TYPE> implements Pool<ELEMENT_TYPE> {

    private static final int DELEGATE_POOL_SEGMENT_CAPACITY = 10;

    private final Semaphore semaphore;
    private final SegmentedSoftPool<ELEMENT_TYPE> delegatePool;

    public ThreadSafeMaxSizePool(
            final int maxSize,
            @NotNull final Supplier<ELEMENT_TYPE> factory,
            @Nullable final Consumer<ELEMENT_TYPE> clearingProcedure) {
        this.semaphore = new Semaphore(maxSize);
        this.delegatePool = new SegmentedSoftPool<>(DELEGATE_POOL_SEGMENT_CAPACITY, factory, clearingProcedure);
    }

    @Override
    public ELEMENT_TYPE take() {
        try {
            // This blocks if we've already got maximum number of elements taken out.
            semaphore.acquire();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedDeephavenException("Interrupted while trying to take from pool", e);
        }
        return delegatePool.take();
    }

    @Override
    public void give(@NotNull final ELEMENT_TYPE item) {
        delegatePool.give(item);
        semaphore.release();
    }
}
