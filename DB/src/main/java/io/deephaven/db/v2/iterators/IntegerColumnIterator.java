/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.iterators;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive integers contained with a ColumnSource.
 */
public class IntegerColumnIterator extends ColumnIterator<Integer> implements PrimitiveIterator.OfInt {

    public IntegerColumnIterator(@NotNull final Index index, @NotNull final ColumnSource<Integer> columnSource) {
        super(index, columnSource);
    }

    public IntegerColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        //noinspection unchecked
        this(table.getIndex(), table.getColumnSource(columnName));
    }

    @Override
    public int nextInt() {
        return columnSource.getInt(indexIterator.nextLong());
    }
}
