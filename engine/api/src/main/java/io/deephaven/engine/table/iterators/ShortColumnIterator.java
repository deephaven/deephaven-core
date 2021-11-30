/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.iterators;

import io.deephaven.base.Procedure;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive shorts contained with a ColumnSource.
 */
public class ShortColumnIterator extends ColumnIterator<Short> implements PrimitiveIterator<Short, Procedure.UnaryShort> {

    public ShortColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Short> columnSource) {
        super(rowSet, columnSource);
    }

    public ShortColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @SuppressWarnings("WeakerAccess")
    public short nextShort() {
        return columnSource.getShort(indexIterator.nextLong());
    }

    @Override
    public void forEachRemaining(@NotNull final Procedure.UnaryShort action) {
        while (hasNext()) {
            action.call(nextShort());
        }
    }
}
