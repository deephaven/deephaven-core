//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.BucketSsmDistinctContext;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.DoubleSsmBackedSource;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.SsmDistinctContext;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.DoubleCompactModifications;
import io.deephaven.engine.table.impl.util.compact.DoubleCompactKernel;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator computes the single unique value of a particular aggregated state. If there are no values at all the
 * 'no value key' is used. If there are more than one values for the state, the 'non unique key' is used.
 */
public class DoubleChunkedUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNullNaN;
    private final boolean exposeInternal;
    private WritableRowSet touchedStates;
    private UpdateCommitter<DoubleChunkedUniqueOperator> prevFlusher = null;

    private final DoubleSsmBackedSource ssms;
    private final DoubleArraySource internalResult;
    private final ColumnSource<?> externalResult;
    private final double onlyNullsSentinel;
    private final double nonUniqueSentinel;

    public DoubleChunkedUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name, boolean countNullNaN, boolean exposeInternal, double onlyNullsSentinel, double nonUniqueSentinel) {
        this.name = name;
        this.countNullNaN = countNullNaN;
        this.exposeInternal = exposeInternal;
        this.onlyNullsSentinel = onlyNullsSentinel;
        this.nonUniqueSentinel = nonUniqueSentinel;

        // region SsmCreation
        this.ssms = new DoubleSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new DoubleArraySource();
        // endregion ResultCreation
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

        DoubleCompactKernel.compactAndCount((WritableDoubleChunk<? extends Values>) context.valueCopy, context.counts,
                startPositions, context.lengthCopy, countNullNaN, countNullNaN);
        return context;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final WritableDoubleChunk<? extends Values> valueCopy = context.valueCopy.asWritableDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.insert(valueCopy, context.counts, startPosition, runLength);
            stateModified.set(ii, setResult(ssm, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctContext context = getAndUpdateContext(values, startPositions, length, bucketedContext);
        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableDoubleChunk<? extends Values> valueCopy = context.valueCopy.asWritableDoubleChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(removeContext, valueCopy, context.counts, startPosition, runLength);
            if (ssm.isEmpty()) {
                clearSsm(destination);
            }

            stateModified.set(ii, setResult(ssm, destination));
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
        final WritableDoubleChunk<? extends Values> preValueCopy =
                (WritableDoubleChunk<? extends Values>) context.valueCopy;
        final WritableDoubleChunk<? extends Values> postValueCopy =
                (WritableDoubleChunk<? extends Values>) context.postValues;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);

            final int runLength = length.get(ii);
            if (runLength != 0) {
                // reduce the bucket's modify to its net effect: net removals compacted into preValueCopy and net
                // additions into postValueCopy, each beginning at startPosition, with the unchanged overlap cancelled
                DoubleCompactModifications.compactAndCountModifications(preValueCopy, context.counts,
                        postValueCopy, context.postCounts, startPosition, runLength, startPosition, runLength,
                        countNullNaN, countNullNaN, context.removedSize, context.addedSize);
                final int removed = context.removedSize.get();
                if (removed > 0) {
                    ssm.remove(removeContext, preValueCopy, context.counts, startPosition, removed);
                }
                final int added = context.addedSize.get();
                if (added > 0) {
                    ssm.insert(postValueCopy, context.postCounts, startPosition, added);
                }
                if (ssm.isEmpty()) {
                    clearSsm(destination);
                }
            }

            stateModified.set(ii, setResult(ssm, destination));
        }
    }
    // endregion

    // region Singleton Updates
    @NotNull
    private SsmDistinctContext getAndUpdateContext(Chunk<? extends Values> values, SingletonContext singletonContext) {
        final SsmDistinctContext context = (SsmDistinctContext) singletonContext;

        context.valueCopy.setSize(values.size());
        context.valueCopy.copyFromChunk(values, 0, 0, values.size());
        DoubleCompactKernel.compactAndCount((WritableDoubleChunk<? extends Values>) context.valueCopy, context.counts,
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
        DoubleCompactModifications.compactAndCountModifications(
                (WritableDoubleChunk<? extends Values>) context.valueCopy, context.counts,
                (WritableDoubleChunk<? extends Values>) context.postValues, context.postCounts,
                0, length, 0, length, countNullNaN, countNullNaN, context.removedSize, context.addedSize);
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);
        if (context.valueCopy.size() > 0) {
            ssm.insert(context.valueCopy, context.counts);
        }
        return setResult(ssm, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctContext context = getAndUpdateContext(values, singletonContext);
        if (context.valueCopy.size() == 0) {
            return false;
        }

        final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);
        ssm.remove(context.removeContext, context.valueCopy, context.counts);
        if (ssm.isEmpty()) {
            clearSsm(destination);
        }

        return setResult(ssm, destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // reduce the modify to its net effect: valueCopy/counts hold the values to remove, postValues/postCounts the
        // values to add, with the unchanged overlap cancelled out
        final SsmDistinctContext context = getAndUpdateContext(preValues, postValues, singletonContext);
        final DoubleSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final int removed = context.removedSize.get();
        if (removed > 0) {
            ssm.remove(context.removeContext, context.valueCopy, context.counts, 0, removed);
        }
        final int added = context.addedSize.get();
        if (added > 0) {
            ssm.insert(context.postValues, context.postCounts, 0, added);
        }
        if (ssm.isEmpty()) {
            clearSsm(destination);
        }

        return setResult(ssm, destination);
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

    private static void flushPrevious(DoubleChunkedUniqueOperator op) {
        if (op.touchedStates == null || op.touchedStates.isEmpty()) {
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
        if (exposeInternal) {
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(name, externalResult);
            columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                    ssms.getUnderlyingSource());
            return columns;
        }

        return Collections.<String, ColumnSource<?>>singletonMap(name, externalResult);
    }

    @Override
    public void startTrackingPrevValues() {
        internalResult.startTrackingPrevValues();
        if (exposeInternal) {
            if (prevFlusher != null) {
                throw new IllegalStateException("startTrackingPrevValues must only be called once");
            }

            ssms.startTrackingPrevValues();
            prevFlusher = new UpdateCommitter<>(this, ExecutionContext.getContext().getUpdateGraph(),
                    DoubleChunkedUniqueOperator::flushPrevious);
            touchedStates = RowSetFactory.empty();
        }
    }

    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctContext(ChunkType.Double, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctContext(ChunkType.Double, size);
    }
    // endregion

    // region Private Helpers
    private DoubleSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }

    private boolean setResult(DoubleSegmentedSortedMultiset ssm, long destination) {
        final boolean resultChanged;
        if (ssm.isEmpty()) {
            resultChanged = internalResult.getAndSetUnsafe(destination, onlyNullsSentinel) != onlyNullsSentinel;
        } else if (ssm.size() == 1) {
            final double newValue = ssm.get(0);
            resultChanged = internalResult.getAndSetUnsafe(destination, newValue) != newValue;
        } else {
            resultChanged = internalResult.getAndSetUnsafe(destination, nonUniqueSentinel) != nonUniqueSentinel;
        }

        return resultChanged || (exposeInternal && (ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0));
    }
    // endregion
}
