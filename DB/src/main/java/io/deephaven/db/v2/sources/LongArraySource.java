/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Simple array source for Long.
 */
public class LongArraySource extends AbstractLongArraySource<Long> {

    public LongArraySource() {
        super(long.class);
    }

    @Override
    public void set(long key, Long value) {
        set(key, unbox(value));
    }

    @Override
    public Long get(long index) { return box(getLong(index)); }

    @Override
    public Long getPrev(long index) {
        return box(getPrevLong(index));
    }

    @Override
    public void copy(ColumnSource<? extends Long> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.getLong(sourceKey));
    }
}
