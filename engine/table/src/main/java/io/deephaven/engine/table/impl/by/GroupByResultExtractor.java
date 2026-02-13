//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;

import java.util.Map;

/**
 * Shared class for the {@link GroupByReaggregateOperator} and {@link GroupByChunkedOperator} that gets results from the
 * original operator and adds them as new result columns, so that only one heavy-weight operator is needed but results
 * can be in any order when multiple {@link io.deephaven.api.agg.spec.AggSpecGroup}s are specified.
 */
abstract class GroupByResultExtractor implements IterativeChunkedAggregationOperator {
    final Map<String, ? extends ColumnSource<?>> resultColumns;
    final String[] inputColumnNames;

    GroupByResultExtractor(Map<String, ? extends ColumnSource<?>> resultColumns, String[] inputColumnNames) {
        this.resultColumns = resultColumns;
        this.inputColumnNames = inputColumnNames;
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {}

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {}

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {}

    @Override
    public void startTrackingPrevValues() {}
}
