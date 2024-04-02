//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit SerialCharacterColumnIterator and run "./gradlew replicateColumnIterators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Serial {@link ShortColumnIterator} implementation for {@link ColumnSource column sources} of primitive shorts.
 */
public final class SerialShortColumnIterator
        extends SerialColumnIterator<Short>
        implements ShortColumnIterator {

    /**
     * Create a new SerialShortColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     * @param firstRowKey The first row key from {@code rowSet} to iterate
     * @param length The total number of rows to iterate
     */
    public SerialShortColumnIterator(
            @NotNull final ColumnSource<Short> columnSource,
            @NotNull final RowSet rowSet,
            final long firstRowKey,
            final long length) {
        super(columnSource, rowSet, firstRowKey, length);
    }

    /**
     * Create a new SerialShortColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     */
    public SerialShortColumnIterator(
            @NotNull final ColumnSource<Short> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, rowSet.firstRowKey(), rowSet.size());
    }

    @Override
    public short nextShort() {
        return columnSource.getShort(advanceAndGetNextRowKey());
    }
}
