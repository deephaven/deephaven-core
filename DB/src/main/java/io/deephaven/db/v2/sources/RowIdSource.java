/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

@AbstractColumnSource.IsSerializable(value = true)
public class RowIdSource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {
    public static final RowIdSource INSTANCE = new RowIdSource();

    public RowIdSource() {
        super(Long.class);
    }

    @Override
    public long getLong(long index) {
        return index;
    }

    @Override
    public long getPrevLong(long index) {
        return index;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
