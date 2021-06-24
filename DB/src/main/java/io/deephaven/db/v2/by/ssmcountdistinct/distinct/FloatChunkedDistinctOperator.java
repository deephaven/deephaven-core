/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedDistinctOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by.ssmcountdistinct.distinct;

import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.db.v2.by.ssmcountdistinct.FloatSsmBackedSource;
import io.deephaven.db.v2.by.ssmcountdistinct.DistinctOperatorFactory;
import io.deephaven.db.v2.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.ReadableIndex;
import io.deephaven.db.v2.utils.UpdateCommitter;
import io.deephaven.db.v2.utils.compact.FloatCompactKernel;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator computes the set of distinct values within the source.
 */
public class FloatChunkedDistinctOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final FloatSsmBackedSource internalResult;
    private final ColumnSource<?> externalResult;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNull;
    private final boolean exposeInternal;
    private Index touchedStates;
    private UpdateCommitter<FloatChunkedDistinctOperator> prevFlusher = null;

    public FloatChunkedDistinctOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNulls, boolean exposeInternal) {
        this.name = name;
        this.countNull = countNulls;
        this.exposeInternal = exposeInternal;
        // region SsmCreation
        this.internalResult = new FloatSsmBackedSource();
        // endregion SsmCreation
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment

        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(DistinctOperatorFactory.NODE_SIZE);
    }

    //region Bucketed Updates
    @NotNull
    private BucketSsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, BucketedContext bucketedContext) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext)bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        FloatCompactKernel.compactAndCount((WritableFloatChunk<? extends Values>) context.valueCopy, context.counts, startPositions, context.lengthCopy, countNull);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            stateModified.set(ii, ssm.insert(valueSlice, countSlice));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            stateModified.set(ii, ssm.remove(removeContext, valueSlice, countSlice));
            if (ssm.size() == 0) {
                clearSsm(destination);
            }
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(preValues, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        context.ssmsToMaybeClear.fillWithValue(0, startPositions.size(), false);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                context.ssmsToMaybeClear.set(ii, true);
            }
        }

        getAndUpdateContext(postValues, startPositions, length, context);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            if (runLength == 0) {
                final FloatSegmentedSortedMultiset ssm = internalResult.getCurrentSsm(destination);
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination);
                }

                stateModified.set(ii, ssm != null && (ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0));
                continue;
            }

            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.insert(valueSlice, countSlice);

            stateModified.set(ii, ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0);
        }
    }
    //endregion

    //region Singleton Updates
    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        FloatCompactKernel.compactAndCount((WritableFloatChunk<? extends Values>) context.valueCopy, context.counts, countNull);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            return ssm.insert(context.valueCopy, context.counts);
        }

        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }

        final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final boolean removed = ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.size() == 0) {
            clearSsm(destination);
        }
        return removed;
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(preValues, singletonContext);
        if (context.valueCopy.size() > 0) {
            FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(context.removeContext, context.valueCopy, context.counts);
        }

        getAndUpdateContext(postValues, context);
        FloatSegmentedSortedMultiset ssm = internalResult.getCurrentSsm(destination);
        if (context.valueCopy.size() > 0) {
            if (ssm == null) {
                ssm = ssmForSlot(destination);
            }
            ssm.insert(context.valueCopy, context.counts);
        } else if (ssm != null && ssm.size() == 0) {
            clearSsm(destination);
        } else if (ssm == null) {
            return false;
        }

        return ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0;
    }
    //endregion

    //region IterativeOperator / DistinctAggregationOperator
    @Override
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream, @NotNull ReadableIndex newDestinations) {
        if (touchedStates != null) {
            prevFlusher.maybeActivate();
            touchedStates.clear();
            touchedStates.insert(downstream.added);
            touchedStates.insert(downstream.modified);
        }
    }

    private static void flushPrevious(FloatChunkedDistinctOperator op) {
        if(op.touchedStates == null || op.touchedStates.empty()) {
            return;
        }

        op.internalResult.clearDeltas(op.touchedStates);
        op.touchedStates.clear();
    }

    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if(exposeInternal) {
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(name, externalResult);
            columns.put(name + ComboAggregateFactory.ROLLUP_DISTINCT_SSM_COLUMN_ID + ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX, internalResult.getUnderlyingSource());
            return columns;
        }

        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        if(exposeInternal) {
            if (prevFlusher != null) {
                throw new IllegalStateException("startTrackingPrevValues must only be called once");
            }

            prevFlusher = new UpdateCommitter<>(this, FloatChunkedDistinctOperator::flushPrevious);
            touchedStates = Index.CURRENT_FACTORY.getEmptyIndex();
        }
    }

    //endregion

    //region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Float, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Float, size);
    }
    //endregion

    //region Private Helpers
    private FloatSegmentedSortedMultiset ssmForSlot(long destination) {
        return internalResult.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        internalResult.clear(destination);
    }
    //endregion
}
