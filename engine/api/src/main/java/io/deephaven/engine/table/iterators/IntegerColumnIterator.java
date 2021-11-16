/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.iterators;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive integers contained with a ColumnSource.
 */
public class IntegerColumnIterator extends ColumnIterator<Integer> implements PrimitiveIterator.OfInt {

    public IntegerColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Integer> columnSource) {
        super(rowSet, columnSource);
    }

    public IntegerColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @Override
    public int nextInt() {
        return columnSource.getInt(indexIterator.nextLong());
    }
}
