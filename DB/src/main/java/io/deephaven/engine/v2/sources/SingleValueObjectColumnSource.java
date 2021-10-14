/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.utils.Index;

/**
 * ColumnSource implementation for columns of a single unique value.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class SingleValueObjectColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE> implements ImmutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    private final DATA_TYPE value;

    public SingleValueObjectColumnSource(DATA_TYPE value) {
        //noinspection unchecked
        super((Class<DATA_TYPE>)value.getClass());
        this.value = value;
    }

    @Override
    public DATA_TYPE get(long index) {
        if(index == Index.NULL_KEY) {
            return null;
        }
        return value;
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }
}
