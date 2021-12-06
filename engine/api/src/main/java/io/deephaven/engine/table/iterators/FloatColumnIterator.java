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
 * Iteration support for boxed or primitive floats contained with a ColumnSource.
 */
public class FloatColumnIterator extends ColumnIterator<Float> implements PrimitiveIterator<Float, Procedure.UnaryFloat> {

    public FloatColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Float> columnSource) {
        super(rowSet, columnSource);
    }

    public FloatColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @SuppressWarnings("WeakerAccess")
    public float nextFloat() {
        return columnSource.getFloat(indexIterator.nextLong());
    }

    @Override
    public void forEachRemaining(@NotNull final Procedure.UnaryFloat action) {
        while (hasNext()) {
            action.call(nextFloat());
        }
    }
}
