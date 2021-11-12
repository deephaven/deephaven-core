/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.iterators;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive longs contained with a ColumnSource.
 */
public class LongColumnIterator extends ColumnIterator<Long> implements PrimitiveIterator.OfLong {

    public LongColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Long> columnSource) {
        super(rowSet, columnSource);
    }

    public LongColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        //noinspection unchecked
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @Override
    public long nextLong() {
        return columnSource.getLong(indexIterator.nextLong());
    }
}
