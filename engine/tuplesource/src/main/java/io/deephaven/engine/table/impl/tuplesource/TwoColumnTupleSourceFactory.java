package io.deephaven.engine.table.impl.tuplesource;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TupleSource} factory for two {@link ColumnSource column sources}.
 */
public interface TwoColumnTupleSourceFactory<TUPLE_TYPE, COLUMN_SOURCE_1_TYPE, COLUMN_SOURCE_2_TYPE> {

    /**
     * Create a {@link TupleSource} of the appropriate type.
     *
     * @param columnSource1 The first column source
     * @param columnSource2 The second column source
     * @return The new tuple factory
     */
    TupleSource<TUPLE_TYPE> create(
            @NotNull ColumnSource<COLUMN_SOURCE_1_TYPE> columnSource1,
            @NotNull ColumnSource<COLUMN_SOURCE_2_TYPE> columnSource2);
}
