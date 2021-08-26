/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.iterators;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * Iteration support for objects (including boxed primitives) contained with a ColumnSource.
 */
public class ColumnIterator<TYPE> implements Iterator<TYPE> {

    protected final ColumnSource<TYPE> columnSource;

    protected final Index.Iterator indexIterator;

    /**
     * Create a new iterator.
     *
     * @param index index for the column source
     * @param columnSource column source
     */
    public ColumnIterator(@NotNull final Index index, @NotNull final ColumnSource<TYPE> columnSource) {
        this.columnSource = columnSource;
        indexIterator = index.iterator();
    }

    /**
     * Create a new iterator.
     *
     * @param table table to create the iterator from
     * @param columnName column name for iteration
     */
    public ColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        // noinspection unchecked
        this(table.getIndex(), table.getColumnSource(columnName));
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
