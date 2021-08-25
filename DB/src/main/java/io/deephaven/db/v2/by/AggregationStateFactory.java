/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;

/**
 * The aggregation state factory is passed to the by operation, and is used to create new aggregation states for each of
 * the output rows (or in case of byExternal, each of the output tables).
 *
 * The factory also returns the result column sources.
 */
public interface AggregationStateFactory {
    /**
     * Produces a MemoKey for this aggregation state factory.
     *
     * <p>
     * If two AggregationStateFactories have equal memoKeys, then {@link Table#by} operations that have the same group
     * by columns may be memoized. In that case instead of recomputing the result; the original result will be used.
     * </p>
     *
     * <p>
     * If null is returned, the operation will not be memoized.
     * </p>
     *
     * @return an AggregationMemoKey, null if this operation can not be memoized.
     */
    default AggregationMemoKey getMemoKey() {
        return null;
    }
}
