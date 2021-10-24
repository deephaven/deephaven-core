/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedDistinctOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by.ssmcountdistinct.distinct;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.by.ssmcountdistinct.DbDateTimeSsmSourceWrapper;

import io.deephaven.engine.v2.ShiftAwareListener;
import io.deephaven.engine.v2.by.ComboAggregateFactory;
import io.deephaven.engine.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.v2.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.engine.v2.by.ssmcountdistinct.LongSsmBackedSource;
import io.deephaven.engine.v2.by.ssmcountdistinct.DistinctOperatorFactory;
import io.deephaven.engine.v2.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.v2.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.UpdateCommitter;
import io.deephaven.engine.v2.utils.compact.LongCompactKernel;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator computes the set of distinct values within the source.
 */
public class LongChunkedDistinctOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final LongSsmBackedSource internalResult;
    private final ColumnSource<?> externalResult;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNull;
    private final boolean exposeInternal;
    private TrackingMutableRowSet touchedStates;
    private UpdateCommitter<LongChunkedDistinctOperator> prevFlusher = null;

    public LongChunkedDistinctOperator(
            // region Constructor
            Class<?> type,
            // endregion Constructor
            String name, boolean countNulls, boolean exposeInternal) {
        this.name = name;
        this.countNull = countNulls;
        this.exposeInternal = exposeInternal;
        // region SsmCreation
        this.internalResult = new LongSsmBackedSource();
        // endregion SsmCreation
        // region ResultAssignment
        if(type == DBDateTime.class) {
            externalResult = new DbDateTimeSsmSourceWrapper(internalResult);
        } else {
            externalResult = internalResult;
        }
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

        LongCompactKernel.compactAndCount((WritableLongChunk<? extends Values>) context.valueCopy, context.counts, startPositions, context.lengthCopy, countNull);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends Attributes.RowKeys> inputIndices, IntChunk<Attributes.RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            stateModified.set(ii, ssm.insert(valueSlice, countSlice));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends Attributes.RowKeys> inputIndices, IntChunk<Attributes.RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            stateModified.set(ii, ssm.remove(removeContext, valueSlice, countSlice));
            if (ssm.size() == 0) {
                clearSsm(destination);
            }
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftIndices, IntChunk<Attributes.RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
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

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
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
                final LongSegmentedSortedMultiset ssm = internalResult.getCurrentSsm(destination);
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination);
                }

                stateModified.set(ii, ssm != null && (ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0));
                continue;
            }

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
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
        LongCompactKernel.compactAndCount((WritableLongChunk<? extends Values>) context.valueCopy, context.counts, countNull);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends Attributes.RowKeys> inputIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            return ssm.insert(context.valueCopy, context.counts);
        }

        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends Attributes.RowKeys> inputIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }

        final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final boolean removed = ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.size() == 0) {
            clearSsm(destination);
        }
        return removed;
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends Attributes.RowKeys> postShiftIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(preValues, singletonContext);
        if (context.valueCopy.size() > 0) {
            LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(context.removeContext, context.valueCopy, context.counts);
        }

        getAndUpdateContext(postValues, context);
        LongSegmentedSortedMultiset ssm = internalResult.getCurrentSsm(destination);
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
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream, @NotNull RowSet newDestinations) {
        if (touchedStates != null) {
            prevFlusher.maybeActivate();
            touchedStates.clear();
            touchedStates.insert(downstream.added);
            touchedStates.insert(downstream.modified);
        }
    }

    private static void flushPrevious(LongChunkedDistinctOperator op) {
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

            prevFlusher = new UpdateCommitter<>(this, LongChunkedDistinctOperator::flushPrevious);
            touchedStates = TrackingMutableRowSet.CURRENT_FACTORY.getEmptyRowSet();
        }
    }

    //endregion

    //region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Long, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Long, size);
    }
    //endregion

    //region Private Helpers
    private LongSegmentedSortedMultiset ssmForSlot(long destination) {
        return internalResult.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        internalResult.clear(destination);
    }
    //endregion
}
