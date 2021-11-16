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
 * Iteration support for boxed or primitive characters contained with a ColumnSource.
 */
public class CharacterColumnIterator extends ColumnIterator<Character> implements PrimitiveIterator<Character, Procedure.UnaryChar> {

    public CharacterColumnIterator(@NotNull final RowSet rowSet, @NotNull final ColumnSource<Character> columnSource) {
        super(rowSet, columnSource);
    }

    public CharacterColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        this(table.getRowSet(), table.getColumnSource(columnName));
    }

    @SuppressWarnings("WeakerAccess")
    public char nextChar() {
        return columnSource.getChar(indexIterator.nextLong());
    }

    @Override
    public void forEachRemaining(@NotNull final Procedure.UnaryChar action) {
        while (hasNext()) {
            action.call(nextChar());
        }
    }
}
