/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedUniqueOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by.ssmcountdistinct.unique;

import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.db.v2.by.ssmcountdistinct.FloatSsmBackedSource;
import io.deephaven.db.v2.by.ssmcountdistinct.DistinctOperatorFactory;
import io.deephaven.db.v2.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.db.v2.sources.FloatArraySource;
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
 * This operator computes the single unique value of a particular aggregated state.  If there are no values at all
 * the 'no value key' is used.  If there are more than one values for the state,  the 'non unique key' is used.
 */
public class FloatChunkedUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNull;
    private final boolean exposeInternal;
    private Index touchedStates;
    private UpdateCommitter<FloatChunkedUniqueOperator> prevFlusher = null;

    private final FloatSsmBackedSource ssms;
    private final FloatArraySource internalResult;
    private final ColumnSource<?> externalResult;
    private final float noValueKey;
    private final float nonUniqueKey;

    public FloatChunkedUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNulls, boolean exposeInternal, float noValueKey, float nonUniqueKey) {
        this.name = name;
        this.countNull = countNulls;
        this.exposeInternal = exposeInternal;
        this.noValueKey = noValueKey;
        this.nonUniqueKey = nonUniqueKey;

        // region SsmCreation
        this.ssms = new FloatSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new FloatArraySource();
        // endregion ResultCreation
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
            ssm.insert(valueSlice, countSlice);
            stateModified.set(ii, setResult(ssm, destination));
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
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                clearSsm(destination);
            }

            stateModified.set(ii, setResult(ssm, destination));
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
            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            if (runLength == 0) {
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination);
                }

                stateModified.set(ii, setResult(ssm, destination));
                continue;
            }

            final WritableChunk<? extends Attributes.Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy, startPosition, runLength);
            final WritableIntChunk<Attributes.ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts, startPosition, runLength);
            ssm.insert(valueSlice, countSlice);
            stateModified.set(ii, setResult(ssm, destination));
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
            ssm.insert(context.valueCopy, context.counts);
        }
        return setResult(ssm, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }

        final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
        ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.size() == 0) {
            clearSsm(destination);
        }

        return setResult(ssm, destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(preValues, singletonContext);
        if (context.valueCopy.size() > 0) {
            final FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(context.removeContext, context.valueCopy, context.counts);
        }

        getAndUpdateContext(postValues, context);
        FloatSegmentedSortedMultiset ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            ssm.insert(context.valueCopy, context.counts);
        } else if (ssm.size() == 0) {
            clearSsm(destination);
        }

        return setResult(ssm, destination);
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

    private static void flushPrevious(FloatChunkedUniqueOperator op) {
        if(op.touchedStates == null || op.touchedStates.empty()) {
            return;
        }

        op.ssms.clearDeltas(op.touchedStates);
        op.touchedStates.clear();
    }

    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if(exposeInternal) {
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(name, externalResult);
            columns.put(name + ComboAggregateFactory.ROLLUP_DISTINCT_SSM_COLUMN_ID + ComboAggregateFactory.ROLLUP_COLUMN_SUFFIX, ssms.getUnderlyingSource());
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

            ssms.startTrackingPrevValues();
            prevFlusher = new UpdateCommitter<>(this, FloatChunkedUniqueOperator::flushPrevious);
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
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }

    private boolean setResult(FloatSegmentedSortedMultiset ssm, long destination) {
        final boolean resultChanged;
        if(ssm.isEmpty()) {
            resultChanged = internalResult.getAndSetUnsafe(destination, noValueKey) != noValueKey;
        } else if(ssm.size() == 1) {
            final float newValue = ssm.get(0);
            resultChanged = internalResult.getAndSetUnsafe(destination, newValue) != newValue;
        } else {
            resultChanged = internalResult.getAndSetUnsafe(destination, nonUniqueKey) != nonUniqueKey;
        }

        return resultChanged || (exposeInternal && (ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0));
    }
    //endregion
}
