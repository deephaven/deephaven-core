/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.Table;

/**
 * AggregationSpecs are passed to {@link Table#groupBy} operation, and used to supply operation-specific instructions.
 */
public interface AggregationSpec {
    /**
     * Produces a MemoKey for this AggregationSpec.
     *
     * <p>
     * If two AggregationSpecs have equal memoKeys, then {@link Table#groupBy} operations that have the same group by
     * columns may be memoized. In that case instead of recomputing the result; the original result will be used.
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
