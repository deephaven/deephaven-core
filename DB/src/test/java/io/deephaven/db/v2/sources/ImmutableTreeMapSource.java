/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.utils.Index;

/**
 * A simple extension to the TreeMapSource that will not actually change any map values, and is thus
 * immutable. We need to have an immutable source available for use with the IndexGroupingTest, and
 * this fits the bill.
 */
public class ImmutableTreeMapSource<T> extends TreeMapSource<T> {
    public ImmutableTreeMapSource(Class<T> type, Index index, T[] data) {
        super(type, index, data);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public void add(Index index, T[] data) {
        int i = 0;
        for (final Index.Iterator iterator = index.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            if (this.data.containsKey(next))
                continue;
            this.data.put(next, data[i++]);
        }
    }

    @Override
    public void remove(Index index) {}

    @Override
    public T getPrev(long index) {
        return data.get(index);
    }
}
