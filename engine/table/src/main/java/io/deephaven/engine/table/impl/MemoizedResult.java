//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * An implementation class for a memoized result, which holds a weak reference.
 *
 * <p>
 * If multiple callers invoke getOrCompute simultaneously; only one caller computes the result; the others block until
 * the computation is complete. A caller that may not wait instead computes its own result, which is not cached.
 * </p>
 *
 * @param <R> the type of the result
 */
public class MemoizedResult<R> {
    private final ReentrantLock computeLock = new ReentrantLock();

    private volatile WeakReference<R> reference;

    /**
     * If the reference does not already hold a valid cached value, then execute the provided operation to compute the
     * value, cache it, and return it. Otherwise, return the cached value.
     *
     * @param operation the Supplier for our desired result
     * @return the cached or newly computed value
     */
    public R getOrCompute(Supplier<R> operation) {
        return getOrCompute(operation, true);
    }

    /**
     * If the reference does not already hold a valid cached value, then execute the provided operation to compute the
     * value, cache it, and return it. Otherwise, return the cached value.
     *
     * @param operation the Supplier for our desired result
     * @param mayWait Whether the caller may wait for a computation in progress on another thread; a caller that may not
     *        computes its own result while another computation is in progress, and that result is not cached
     * @return the cached or newly computed value
     */
    public R getOrCompute(final Supplier<R> operation, final boolean mayWait) {
        final R cachedResult = getIfValid();
        if (cachedResult != null) {
            return maybeMarkSystemic(cachedResult);
        }

        if (mayWait) {
            computeLock.lock();
        } else if (!computeLock.tryLock()) {
            return operation.get();
        }
        try {
            final R cachedResultLocked = getIfValid();
            if (cachedResultLocked != null) {
                return maybeMarkSystemic(cachedResultLocked);
            }

            final R result;
            result = operation.get();

            reference = new WeakReference<>(result);

            return result;
        } finally {
            computeLock.unlock();
        }
    }

    private R maybeMarkSystemic(R cachedResult) {
        if (cachedResult instanceof SystemicObject && SystemicObjectTracker.isSystemicThread()) {
            // noinspection unchecked
            return (R) ((SystemicObject<?>) cachedResult).markSystemic();
        }
        return cachedResult;
    }

    /**
     * Gets the saved result, if it is valid. To determine liveness, the result is managed with the current scope before
     * being returned. If the result is not valid, then null is returned.
     *
     * @return the result, managed with the current scope (or null if not valid)
     */
    @Nullable
    protected R getIfValid() {
        if (reference != null) {
            final R cachedResult = reference.get();
            if (!isFailed(cachedResult) && Liveness.verifyCachedObjectForReuse(cachedResult)) {
                return cachedResult;
            }
        }
        return null;
    }

    private boolean isFailed(R cachedResult) {
        if (cachedResult instanceof Table) {
            return ((Table) cachedResult).isFailed();
        }
        if (cachedResult instanceof PartitionedTable) {
            return ((PartitionedTable) cachedResult).table().isFailed();
        }
        return false;
    }
}
