/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import static io.deephaven.db.v2.by.IterativeOperatorStateFactory.getMinMaxChunked;

/**
 * Factory for iterative sum aggregations.
 */
public class MinMaxIterativeOperatorFactory implements IterativeChunkedOperatorFactory {
    private final boolean minimum;
    private final boolean isAddOnly;

    public MinMaxIterativeOperatorFactory(boolean minimum, boolean isAddOnly) {
        this.minimum = minimum;
        this.isAddOnly = isAddOnly;
    }

    @Override
    public IterativeChunkedAggregationOperator getChunkedOperator(Class type, String name,
            boolean exposeInternalColumns) {
        return getMinMaxChunked(type, minimum, isAddOnly, name);
    }

    @Override
    public String toString() {
        return minimum ? "Min" : "Max";
    }
}
