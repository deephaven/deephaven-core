/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.structures.rowset;

import io.deephaven.engine.structures.rowset.singlerange.SingleRange;
import gnu.trove.list.array.TLongArrayList;

/**
 * This is the base class for factories that construct {@link Index Indexes.
 */
public abstract class AbstractFactory implements Index.Factory {
    @Override
    public Index getIndexByValues(final long... keys) {
        if (keys.length == 0) {
            return getEmptyIndex();
        }
        if (keys.length == 1) {
            return getIndexByRange(keys[0], keys[0]);
        }
        final Index.RandomBuilder indexBuilder = getRandomBuilder();
        for (long key : keys) {
            indexBuilder.addKey(key);
        }
        return indexBuilder.getIndex();
    }

    @Override
    public Index getIndexByValues(long key) {
        return getIndexByRange(key, key);
    }

    @Override
    public Index getIndexByValues(final TLongArrayList list) {
        list.sort();
        final Index.SequentialBuilder builder = getSequentialBuilder();
        list.forEach(builder);
        return builder.getIndex();
    }

    @Override
    public Index getIndexByRange(final long firstKey, final long lastKey) {
        return new TreeIndex(SingleRange.make(firstKey, lastKey));
    }

    @Override
    public Index getFlatIndex(final long size) {
        return size <= 0 ? getEmptyIndex() : getIndexByRange(0, size - 1);
    }
}
