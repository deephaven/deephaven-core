package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.hashing.ToLongCast;
import io.deephaven.db.v2.hashing.ToLongFunctor;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;

import java.util.Collections;
import java.util.Map;

class LongWeightRecordingInternalOperator implements IterativeChunkedAggregationOperator {
    private final ChunkType chunkType;
    private Context currentContext = null;

    LongWeightRecordingInternalOperator(ChunkType chunkType) {
        this.chunkType = chunkType;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        ((Context)context).add(values);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        ((Context)context).remove(values);
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        ((Context)context).add(values);
        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        ((Context)context).remove(values);
        return false;
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        ((Context)context).add(newValues);
        ((Context)context).remove(previousValues);
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        ((Context)context).add(newValues);
        ((Context)context).remove(previousValues);
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void startTrackingPrevValues() {
    }

    class Context implements SingletonContext, BucketedContext {
        private final ToLongFunctor<? extends Values> addCastKernel;
        private final ToLongFunctor<? extends Values> removeCastKernel;
        private LongChunk<? extends Values> weightAddChunk;
        private LongChunk<? extends Values> weightRemoveChunk;

        private Context(ChunkType chunkType, int size) {
            addCastKernel = ToLongCast.makeToLongCast(chunkType, size, 0);
            removeCastKernel = ToLongCast.makeToLongCast(chunkType, size, 0);
            Assert.eqNull(currentContext, "currentContext.getValue()");
            currentContext = this;
        }

        void add(Chunk<? extends Values> values) {
            weightAddChunk = addCastKernel.apply((Chunk)values);
        }

        void remove(Chunk<? extends Values> values) {
            weightRemoveChunk = removeCastKernel.apply((Chunk)values);
        }

        @Override
        public void close() {
            Assert.eq(currentContext, "currentContext", this, "this");
            currentContext = null;
            addCastKernel.close();
            removeCastKernel.close();
        }
    }


    LongChunk<? extends Values> getAddedWeights() {
        return currentContext.weightAddChunk;
    }

    LongChunk<? extends Values> getRemovedWeights() {
        return currentContext.weightRemoveChunk;
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new Context(chunkType, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new Context(chunkType, size);
    }
}
