/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.iterators;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Iteration support for objects (including boxed primitives) contained with a ColumnSource.
 */
public class ColumnIterator<TYPE> implements Iterator<TYPE> {

    protected final ColumnSource<TYPE> columnSource;

    protected final RowSet.Iterator indexIterator;

    /**
     * Create a new iterator.
     *
     * @param rowSet The {@link RowSet} to iterate over
     * @param columnSource The {@link ColumnSource} to fetch values from
     */
    public ColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<TYPE> columnSource) {
        this.columnSource = columnSource;
        indexIterator = rowSet.iterator();
    }

    /**
     * Create a new iterator.
     *
     * @param table table to create the iterator from
     * @param columnName column name for iteration
     */
    public ColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @Override
    public boolean hasNext() {
        return indexIterator.hasNext();
    }

    @Override
    public TYPE next() {
        return columnSource.get(indexIterator.nextLong());
    }
}
