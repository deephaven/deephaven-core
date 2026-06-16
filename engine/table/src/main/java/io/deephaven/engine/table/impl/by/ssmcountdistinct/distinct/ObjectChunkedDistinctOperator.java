//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedDistinctOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.ObjectSsmBackedSource;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.ObjectCompactModifications;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.util.compact.ObjectCompactKernel;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator computes the set of distinct values within the source.
 */
public class ObjectChunkedDistinctOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final ObjectSsmBackedSource internalResult;
    private final ColumnSource<?> externalResult;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNullNaN;
    private final boolean exposeInternal;
    private WritableRowSet touchedStates;
    private UpdateCommitter<ObjectChunkedDistinctOperator> prevFlusher = null;

    public ObjectChunkedDistinctOperator(
            // region Constructor
            Class<?> type,
            // endregion Constructor
            String name, boolean countNullNaN, boolean exposeInternal) {
        this.name = name;
        this.countNullNaN = countNullNaN;
        this.exposeInternal = exposeInternal;
        // region SsmCreation
        this.internalResult = new ObjectSsmBackedSource(type);
        // endregion SsmCreation
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment

        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(SsmDistinctContext.NODE_SIZE);
    }

    // region Bucketed Updates
    @NotNull
    private BucketSsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, BucketedContext bucketedContext) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext) bucketedContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());

        context.lengthCopy.setSize(length.size());
        context.lengthCopy.copyFromChunk(length, 0, 0, length.size());

        ObjectCompactKernel.compactAndCount((WritableObjectChunk<Object, ? extends Values>) context.valueCopy, context.counts,
                startPositions, context.lengthCopy, countNullNaN, countNullNaN);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final WritableObjectChunk<Object, ? extends Values> valueCopy = context.valueCopy.asWritableObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
            stateModified.set(ii, ssm.insert(valueCopy, context.counts, startPosition, runLength));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableObjectChunk<Object, ? extends Values> valueCopy = context.valueCopy.asWritableObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
            stateModified.set(ii, ssm.remove(removeContext, valueCopy, context.counts, startPosition, runLength));
            if (ssm.isEmpty()) {
                clearSsm(destination);
            }
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = (BucketSsmDistinctContext) bucketedContext;
        // a modify produces one pre and one post value per row, so the two ranges share start positions and lengths
        context.valueCopy.setSize(preValues.size());
        context.valueCopy.copyFromChunk(preValues, 0, 0, preValues.size());
        context.postValues.setSize(postValues.size());
        context.postValues.copyFromChunk(postValues, 0, 0, postValues.size());

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableObjectChunk<Object, ? extends Values> preValueCopy =
                (WritableObjectChunk<Object, ? extends Values>) context.valueCopy;
        final WritableObjectChunk<Object, ? extends Values> postValueCopy =
                (WritableObjectChunk<Object, ? extends Values>) context.postValues;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            // an unchanged or empty bucket must not create an ssm, so look up the existing one unless we have work to
            // do
            ObjectSegmentedSortedMultiset ssm;
            if (runLength == 0) {
                ssm = internalResult.getCurrentSsm(destination);
            } else {
                // reduce the bucket's modify to its net effect, cancelling the unchanged overlap
                ObjectCompactModifications.compactAndCountModifications(preValueCopy, context.counts,
                        postValueCopy, context.postCounts, startPosition, runLength, startPosition, runLength,
                        countNullNaN, countNullNaN, context.removedSize, context.addedSize);
                final int removed = context.removedSize.get();
                final int added = context.addedSize.get();
                if (removed == 0 && added == 0) {
                    ssm = internalResult.getCurrentSsm(destination);
                } else {
                    ssm = ssmForSlot(destination);
                    if (removed > 0) {
                        ssm.remove(removeContext, preValueCopy, context.counts, startPosition, removed);
                    }
                    if (added > 0) {
                        ssm.insert(postValueCopy, context.postCounts, startPosition, added);
                    }
                    if (ssm.isEmpty()) {
                        clearSsm(destination);
                    }
                }
            }

            stateModified.set(ii, ssm != null && (ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0));
        }
    }
    // endregion

    // region Singleton Updates
    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        ObjectCompactKernel.compactAndCount((WritableObjectChunk<Object, ? extends Values>) context.valueCopy, context.counts,
                countNullNaN, countNullNaN);
        return context;
    }

    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        // a modify produces one pre and one post value per row, so the two ranges share a length
        final int length = preValues.size();
        context.valueCopy.setSize(length);
        context.valueCopy.copyFromChunk(preValues, 0, 0, length);
        context.postValues.setSize(length);
        context.postValues.copyFromChunk(postValues, 0, 0, length);
        ObjectCompactModifications.compactAndCountModifications(
                (WritableObjectChunk<Object, ? extends Values>) context.valueCopy, context.counts,
                (WritableObjectChunk<Object, ? extends Values>) context.postValues, context.postCounts,
                0, length, 0, length, countNullNaN, countNullNaN, context.removedSize, context.addedSize);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            return ssm.insert(context.valueCopy, context.counts);
        }

        return false;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }

        final ObjectSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final boolean removed = ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.isEmpty()) {
            clearSsm(destination);
        }
        return removed;
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(preValues, postValues, singletonContext);
        final int removed = context.removedSize.get();
        final int added = context.addedSize.get();
        // an unchanged modify must not create an ssm, so look up the existing one unless we have work to do
        ObjectSegmentedSortedMultiset ssm = internalResult.getCurrentSsm(destination);
        if (removed > 0 || added > 0) {
            if (ssm == null) {
                ssm = ssmForSlot(destination);
            }
            if (removed > 0) {
                ssm.remove(context.removeContext, context.valueCopy, context.counts, 0, removed);
            }
            if (added > 0) {
                ssm.insert(context.postValues, context.postCounts, 0, added);
            }
            if (ssm.isEmpty()) {
                clearSsm(destination);
            }
        } else if (ssm == null) {
            return false;
        }

        return ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0;
    }
    // endregion

    // region IterativeOperator / DistinctAggregationOperator
    @Override
    public void propagateUpdates(@NotNull TableUpdate downstream, @NotNull RowSet newDestinations) {
        if (touchedStates != null) {
            prevFlusher.maybeActivate();
            touchedStates.clear();
            touchedStates.insert(downstream.added());
            touchedStates.insert(downstream.modified());
        }
    }

    private static void flushPrevious(ObjectChunkedDistinctOperator op) {
        if (op.touchedStates == null || op.touchedStates.isEmpty()) {
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
        if (exposeInternal) {
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(name, externalResult);
            columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                    internalResult.getUnderlyingSource());
            return columns;
        }

        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        if (prevFlusher != null) {
            throw new IllegalStateException("startTrackingPrevValues must only be called once");
        }

        prevFlusher = new UpdateCommitter<>(this, ExecutionContext.getContext().getUpdateGraph(),
                ObjectChunkedDistinctOperator::flushPrevious);
        touchedStates = RowSetFactory.empty();
    }

    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Object, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Object, size);
    }
    // endregion

    // region Private Helpers
    private ObjectSegmentedSortedMultiset ssmForSlot(long destination) {
        return internalResult.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        internalResult.clear(destination);
    }
    // endregion
}
