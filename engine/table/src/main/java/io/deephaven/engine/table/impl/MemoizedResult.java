//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.systemicmarking.SystemicObject;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;

import java.lang.ref.WeakReference;
import java.util.function.Supplier;

/**
 * An implementation class for a memoized result, which holds a weak reference.
 *
 * <p>
 * If multiple callers invoke getOrCompute simultaneously; only one caller computes the result; the others block on this
 * object's monitor until the computation is complete.
 * </p>
 *
 * @param <R> the type of the result
 */
public class MemoizedResult<R> {
    private volatile WeakReference<R> reference;

    /**
     * If the reference does not already hold a valid cached value, then execute the provided operation to compute the
     * value, cache it, and return it. Otherwise, return the cached value.
     *
     * @param operation the Supplier for our desired result
     * @return the cached or newly computed value
     */
    public R getOrCompute(Supplier<R> operation) {
        final R cachedResult = getIfValid();
        if (cachedResult != null) {
            return maybeMarkSystemic(cachedResult);
        }

        synchronized (this) {
            final R cachedResultLocked = getIfValid();
            if (cachedResultLocked != null) {
                return maybeMarkSystemic(cachedResultLocked);
            }

            final R result;
            result = operation.get();

            reference = new WeakReference<>(result);

            return result;
        }
    }

    private R maybeMarkSystemic(R cachedResult) {
        if (cachedResult instanceof SystemicObject && SystemicObjectTracker.isSystemicThread()) {
            // noinspection unchecked
            return (R) ((SystemicObject) cachedResult).markSystemic();
        }
        return cachedResult;
    }

    private R getIfValid() {
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
