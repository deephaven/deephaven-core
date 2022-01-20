/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;

/**
 * ColumnSource implementation for columns of a single unique value.
 */
public class SingleValueObjectColumnSource<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE> implements ImmutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    private final DATA_TYPE value;

    public SingleValueObjectColumnSource(DATA_TYPE value) {
        //noinspection unchecked
        super((Class<DATA_TYPE>)value.getClass());
        this.value = value;
    }

    @Override
    public DATA_TYPE get(long index) {
        if(index == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return value;
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }
}
