//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Serial {@link ObjectColumnIterator} implementation for {@link ColumnSource column sources} of {@link Object objects}.
 */
public final class SerialObjectColumnIterator<DATA_TYPE>
        extends SerialColumnIterator<DATA_TYPE>
        implements ObjectColumnIterator<DATA_TYPE> {

    /**
     * Create a new SerialObjectColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     * @param firstRowKey The first row key from {@code rowSet} to iterate
     * @param length The total number of rows to iterate
     */
    public SerialObjectColumnIterator(
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final RowSet rowSet,
            final long firstRowKey,
            final long length) {
        super(columnSource, rowSet, firstRowKey, length);
    }

    /**
     * Create a new SerialObjectColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     */
    public SerialObjectColumnIterator(
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, rowSet.firstRowKey(), rowSet.size());
    }

    @Override
    public DATA_TYPE next() {
        return columnSource.get(advanceAndGetNextRowKey());
    }
}
