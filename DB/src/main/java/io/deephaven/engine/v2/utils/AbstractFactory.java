/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.utils.singlerange.SingleRange;
import gnu.trove.list.array.TLongArrayList;

/**
 * This is the base class for factories that construct {@link TrackingMutableRowSet Indexes.
 */
public abstract class AbstractFactory implements RowSetFactory {
    @Override
    public TrackingMutableRowSet getRowSetByValues(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return getEmptyRowSet();
        }
        if (rowKeys.length == 1) {
            return getRowSetByRange(rowKeys[0], rowKeys[0]);
        }
        final RowSetBuilder indexBuilder = getRandomBuilder();
        for (long key : rowKeys) {
            indexBuilder.addKey(key);
        }
        return indexBuilder.build();
    }

    @Override
    public TrackingMutableRowSet getRowSetByValues(long key) {
        return getRowSetByRange(key, key);
    }

    @Override
    public TrackingMutableRowSet getRowSetByValues(final TLongArrayList list) {
        list.sort();
        final SequentialRowSetBuilder builder = getSequentialBuilder();
        list.forEach(builder);
        return builder.build();
    }

    @Override
    public TrackingMutableRowSet getRowSetByRange(final long firstRowKey, final long lastRowKey) {
        return new TrackingMutableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    @Override
    public TrackingMutableRowSet getFlatIndex(final long size) {
        return size <= 0 ? getEmptyRowSet() : getRowSetByRange(0, size - 1);
    }
}
