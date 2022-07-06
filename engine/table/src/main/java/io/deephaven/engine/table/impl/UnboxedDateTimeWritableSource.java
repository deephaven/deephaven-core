/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.time.DateTime;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UnboxedDateTimeWritableSource extends UnboxedDateTimeColumnSource implements WritableColumnSource<Long> {
    private final WritableColumnSource<DateTime> alternateWritableSource;

    public UnboxedDateTimeWritableSource(WritableColumnSource<DateTime> alternateWritableSource) {
        super(alternateWritableSource);
        this.alternateWritableSource = alternateWritableSource;
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        alternateWritableSource.ensureCapacity(capacity, nullFill);
    }

    @Override
    public void set(long key, Long value) {
        alternateWritableSource.set(key, value);
    }

    @Override
    public void setNull(long key) {
        alternateWritableSource.set(key, NULL_LONG);
    }

    @Override
    public void set(long key, long value) {
        alternateWritableSource.set(key, value);
    }
}
