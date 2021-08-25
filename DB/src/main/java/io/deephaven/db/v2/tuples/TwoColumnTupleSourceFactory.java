package io.deephaven.db.v2.tuples;

import io.deephaven.db.v2.sources.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TupleSource}Factory for two {@link io.deephaven.db.v2.sources.ColumnSource}s.
 */
public interface TwoColumnTupleSourceFactory<TUPLE_TYPE, COLUMN_SOURCE_1_TYPE, COLUMN_SOURCE_2_TYPE> {

    /**
     * Create a {@link TupleSource} of the appropriate type.
     *
     * @param columnSource1 The first column source
     * @param columnSource2 The second column source
     * @return The new tuple factory
     */
    TupleSource<TUPLE_TYPE> create(@NotNull ColumnSource<COLUMN_SOURCE_1_TYPE> columnSource1,
            @NotNull ColumnSource<COLUMN_SOURCE_2_TYPE> columnSource2);
}
