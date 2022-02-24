/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result for many {@link ColumnSource} implementations that internally represent {@link DateTime} values as
 * {@code long} values.
 */
public class UnboxedDateTimeColumnSource extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {

    private final ColumnSource<DateTime> alternateColumnSource;

    public UnboxedDateTimeColumnSource(ColumnSource<DateTime> alternateColumnSource) {
        super(long.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public long getLong(long index) {
        return alternateColumnSource.getLong(index);
    }

    @Override
    public long getPrevLong(long index) {
        return alternateColumnSource.getPrevLong(index);
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == DateTime.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    @Override
    public boolean preventsParallelism() {
        return alternateColumnSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return alternateColumnSource.isStateless();
    }
}
