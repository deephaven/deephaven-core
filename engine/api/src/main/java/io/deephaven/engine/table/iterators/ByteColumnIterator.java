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
 * Iteration support for boxed or primitive bytes contained with a ColumnSource.
 */
public class ByteColumnIterator extends ColumnIterator<Byte> implements PrimitiveIterator<Byte, Procedure.UnaryByte> {

    public ByteColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Byte> columnSource) {
        super(rowSet, columnSource);
    }

    public ByteColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        //noinspection unchecked
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @SuppressWarnings("WeakerAccess")
    public byte nextByte() {
        return columnSource.getByte(indexIterator.nextLong());
    }

    @Override
    public void forEachRemaining(@NotNull final Procedure.UnaryByte action) {
        while (hasNext()) {
            action.call(nextByte());
        }
    }
}
