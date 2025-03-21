//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.pool.Pool;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A thread-safe pool that holds a fixed maximum number of items.
 */
final class ThreadSafeMaxSizePool<ELEMENT_TYPE> implements Pool<ELEMENT_TYPE> {

    private static final int DELEGATE_POOL_SEGMENT_CAPACITY = 10;

    private final Semaphore semaphore;
    private final SegmentedSoftPool<ELEMENT_TYPE> delegatePool;

    ThreadSafeMaxSizePool(
            final int maxSize,
            @NotNull final Supplier<ELEMENT_TYPE> factory,
            @NotNull final Consumer<ELEMENT_TYPE> clearingProcedure) {
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
