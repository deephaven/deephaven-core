//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

/**
 * {@link RowSetBuilderRandom} implementation that uses an adaptive pattern based on workload.
 */
public class AdaptiveRowSetBuilderRandom implements RowSetBuilderRandom {

    private final OrderedLongSet.BuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();

    @Override
    public WritableRowSet build() {
        return new WritableRowSetImpl(builder.getOrderedLongSet());
    }

    @Override
    public void addKey(final long rowKey) {
        builder.addKey(rowKey);
    }

    @Override
    public void addRange(final long firstRowKey, final long lastRowKey) {
        builder.addRange(firstRowKey, lastRowKey);
    }
}
