/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSet;

/**
 * A simple extension to the TreeMapSource that will not actually change any map values, and is thus immutable. We need
 * to have an immutable source available for use with the TestRowSetIndexer, and this fits the bill.
 */
public class ImmutableTreeMapSource<T> extends TreeMapSource<T> {
    public ImmutableTreeMapSource(Class<T> type, RowSet rowSet, T[] data) {
        super(type, rowSet, data);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public void add(RowSet rowSet, T[] data) {
        int i = 0;
        for (final RowSet.Iterator iterator = rowSet.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            if (this.data.containsKey(next))
                continue;
            this.data.put(next, data[i++]);
        }
    }

    @Override
    public void remove(RowSet rowSet) {}

    @Override
    public T getPrev(long index) {
        return data.get(index);
    }
}
