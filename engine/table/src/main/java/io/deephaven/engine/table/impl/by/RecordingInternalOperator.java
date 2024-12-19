//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;

import java.util.Collections;
import java.util.Map;

/**
 * Implements a generic recording operator that stores a chunk for retrieval by other operators. This must be added to
 * the operator chain before the operator that will use the chunk.
 * <p>
 * Recording operators are generally used to allow an operator to access more than one input column data.
 */
class RecordingInternalOperator implements IterativeChunkedAggregationOperator {
    private Context currentContext = null;
    private final String inputColumnName;
    private final ColumnSource<?> inputColumnSource;

    class Context implements SingletonContext, BucketedContext {
        private Chunk<? extends Values> valuesChunk;
        private Chunk<? extends Values> prevValuesChunk;

        private Context(int size) {
            Assert.eqNull(currentContext, "currentContext.getValue()");
            currentContext = this;
        }

        void setValues(Chunk<? extends Values> values) {
            valuesChunk = values;
        }

        void setPrevValues(Chunk<? extends Values> values) {
            prevValuesChunk = values;
        }

        @Override
        public void close() {
            Assert.eq(currentContext, "currentContext", this, "this");
            currentContext = null;
        }
    }

    public RecordingInternalOperator(String inputColumnName, ColumnSource<?> inputColumnSource) {
        this.inputColumnName = inputColumnName;
        this.inputColumnSource = inputColumnSource;
    }

    @Override
    public void addChunk(
            final BucketedContext context, Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            final WritableBooleanChunk<Values> stateModified) {
        ((Context) context).setValues(values);
    }

    @Override
    public boolean addChunk(
            final SingletonContext context,
            final int chunkSize,
            final Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        ((Context) context).setValues(values);
        return false;
    }

    @Override
    public void removeChunk(
            final BucketedContext context,
            final Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys,
            final IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length,
            final WritableBooleanChunk<Values> stateModified) {
        ((Context) context).setValues(values);
    }

    @Override
    public boolean removeChunk(
            final SingletonContext context,
            final int chunkSize,
            final Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys,
            final long destination) {
        ((Context) context).setValues(values);
        return false;
    }

    @Override
    public void modifyChunk(
            final BucketedContext context,
            final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            final LongChunk<? extends RowKeys> postShiftRowKeys,
            final IntChunk<RowKeys> destinations,
            final IntChunk<ChunkPositions> startPositions,
            final IntChunk<ChunkLengths> length,
            final WritableBooleanChunk<Values> stateModified) {
        ((Context) context).setPrevValues(previousValues);
        ((Context) context).setValues(newValues);
    }

    @Override
    public boolean modifyChunk(
            final SingletonContext context,
            final int chunkSize,
            final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        ((Context) context).setPrevValues(previousValues);
        ((Context) context).setValues(newValues);
        return false;
    }

    @Override
    public void ensureCapacity(final long tableSize) {}

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void startTrackingPrevValues() {}

    public Chunk<? extends Values> getValueChunk() {
        return currentContext.valuesChunk;
    }

    public Chunk<? extends Values> getPrevValueChunk() {
        return currentContext.prevValuesChunk;
    }

    public String getInputColumnName() {
        return inputColumnName;
    }

    public ColumnSource<?> getInputColumnSource() {
        return inputColumnSource;
    }

    @Override
    public BucketedContext makeBucketedContext(final int size) {
        return new Context(size);
    }

    @Override
    public SingletonContext makeSingletonContext(final int size) {
        return new Context(size);
    }
}
