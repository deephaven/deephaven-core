/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.iterators;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.structures.rowset.Index;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

/**
 * Iteration support for boxed or primitive doubles contained with a ColumnSource.
 */
public class DoubleColumnIterator extends ColumnIterator<Double> implements PrimitiveIterator.OfDouble {

    public DoubleColumnIterator(@NotNull final Index index, @NotNull final ColumnSource<Double> columnSource) {
        super(index, columnSource);
    }

    public DoubleColumnIterator(@NotNull final Table table, @NotNull final String columnName) {
        //noinspection unchecked
        this(table.getIndex(), table.getColumnSource(columnName));
    }

    @Override
    public double nextDouble() {
        return columnSource.getDouble(indexIterator.nextLong());
    }
}
