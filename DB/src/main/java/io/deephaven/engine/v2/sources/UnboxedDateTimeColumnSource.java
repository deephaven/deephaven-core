/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.utils.DateTime;
import org.jetbrains.annotations.NotNull;

/**
 * Reinterpret result for many {@link ColumnSource} implementations that internally represent {@link DateTime} values
 * as {@code long} values.
 */
@AbstractColumnSource.IsSerializable(value = true)
public class UnboxedDateTimeColumnSource extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {

    private final ColumnSource<DateTime> alternateColumnSource;

    UnboxedDateTimeColumnSource(ColumnSource<DateTime> alternateColumnSource) {
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
}
