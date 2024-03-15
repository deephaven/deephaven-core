//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Reinterpret result for many {@link ColumnSource} implementations that internally represent {@link Instant} values as
 * {@code long} values.
 */
public class UnboxedLongBackedColumnSource<T> extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {
    private final ColumnSource<T> alternateColumnSource;

    public UnboxedLongBackedColumnSource(ColumnSource<T> alternateColumnSource) {
        super(long.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public long getLong(long rowKey) {
        return alternateColumnSource.getLong(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return alternateColumnSource.getPrevLong(rowKey);
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == alternateColumnSource.getType()) {
            // this is a trivial return conversion
            return true;
        }
        return alternateColumnSource.allowsReinterpret(alternateDataType);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        if (alternateDataType == alternateColumnSource.getType()) {
            // this is a trivial return conversion
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
        }
        return alternateColumnSource.reinterpret(alternateDataType);
    }
}

