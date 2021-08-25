package io.deephaven.db.v2.tuples;

import io.deephaven.db.v2.sources.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TupleSource}Factory for three {@link ColumnSource}s.
 */
public interface ThreeColumnTupleSourceFactory<TUPLE_TYPE, COLUMN_SOURCE_1_TYPE, COLUMN_SOURCE_2_TYPE, COLUMN_SOURCE_3_TYPE> {

    /**
     * Create a {@link TupleSource} of the appropriate type.
     *
     * @param columnSource1 The first column source
     * @param columnSource2 The second column source
     * @param columnSource3 The third column source
     * @return The new tuple factory
     */
    TupleSource<TUPLE_TYPE> create(@NotNull ColumnSource<COLUMN_SOURCE_1_TYPE> columnSource1,
        @NotNull ColumnSource<COLUMN_SOURCE_2_TYPE> columnSource2,
        @NotNull ColumnSource<COLUMN_SOURCE_3_TYPE> columnSource3);
}
