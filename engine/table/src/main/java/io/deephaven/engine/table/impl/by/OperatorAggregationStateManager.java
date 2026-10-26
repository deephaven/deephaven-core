//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

interface OperatorAggregationStateManager {

    int maxTableSize();

    SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize);

    void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources, MutableInt nextOutputPosition,
            WritableIntChunk<RowKeys> outputPositions);

    ColumnSource[] getKeyHashTableSources();

    int UNKNOWN_ROW = AggregationRowLookup.DEFAULT_UNKNOWN_ROW;

    /**
     * Implement a lookup in order to support {@link AggregationRowLookup#get(Object)}.
     * 
     * @param key The opaque group-by key to find the row position/key for
     * @return The row position/key for {@code key} in the result table, or {@value #UNKNOWN_ROW} if not found
     */
    int findPositionForKey(Object key);

    /**
     * Implement a chunked lookup in order to support {@link AggregationRowLookup#get(Chunk[], WritableLongChunk)}.
     *
     * @param keyChunks The group-by keys, one chunk per group-by column, at least one, reinterpreted as for the build
     * @param positions Receives the row position/key of each key, or {@value #UNKNOWN_ROW} for a key not found; its
     *        size is set to the number of keys
     */
    void findPositionsForKeys(@NotNull Chunk<? extends Values>[] keyChunks,
            @NotNull WritableLongChunk<RowKeys> positions);
}
