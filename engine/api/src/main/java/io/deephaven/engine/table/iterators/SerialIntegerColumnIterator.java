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
 * Serial {@link IntegerColumnIterator} implementation for {@link ColumnSource column sources} of primitive ints.
 */
public final class SerialIntegerColumnIterator
        extends SerialColumnIterator<Integer>
        implements IntegerColumnIterator {

    /**
     * Create a new SerialIntegerColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     * @param firstRowKey The first row key from {@code rowSet} to iterate
     * @param length The total number of rows to iterate
     */
    public SerialIntegerColumnIterator(
            @NotNull final ColumnSource<Integer> columnSource,
            @NotNull final RowSet rowSet,
            final long firstRowKey,
            final long length) {
        super(columnSource, rowSet, firstRowKey, length);
    }

    /**
     * Create a new SerialIntegerColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     */
    public SerialIntegerColumnIterator(
            @NotNull final ColumnSource<Integer> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, rowSet.firstRowKey(), rowSet.size());
    }

    @Override
    public int nextInt() {
        return columnSource.getInt(advanceAndGetNextRowKey());
    }
}
