//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Serial {@link CharacterColumnIterator} implementation for {@link ColumnSource column sources} of primitive chars.
 */
public final class SerialCharacterColumnIterator
        extends SerialColumnIterator<Character>
        implements CharacterColumnIterator {

    /**
     * Create a new SerialCharacterColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     * @param firstRowKey The first row key from {@code rowSet} to iterate
     * @param length The total number of rows to iterate
     */
    public SerialCharacterColumnIterator(
            @NotNull final ColumnSource<Character> columnSource,
            @NotNull final RowSet rowSet,
            final long firstRowKey,
            final long length) {
        super(columnSource, rowSet, firstRowKey, length);
    }

    /**
     * Create a new SerialCharacterColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     */
    public SerialCharacterColumnIterator(
            @NotNull final ColumnSource<Character> columnSource,
            @NotNull final RowSet rowSet) {
        this(columnSource, rowSet, rowSet.firstRowKey(), rowSet.size());
    }

    @Override
    public char nextChar() {
        return columnSource.getChar(advanceAndGetNextRowKey());
    }
}
