/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.iterators;

import io.deephaven.base.Procedure;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive characters contained with a ColumnSource.
 */
public class CharacterColumnIterator extends ColumnIterator<Character> implements PrimitiveIterator<Character, Procedure.UnaryChar> {

    public CharacterColumnIterator(@NotNull final Index index, @NotNull final ColumnSource<Character> columnSource) {
        super(index, columnSource);
    }

    public CharacterColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        //noinspection unchecked
        this(table.getIndex(), table.getColumnSource(columnName));
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
