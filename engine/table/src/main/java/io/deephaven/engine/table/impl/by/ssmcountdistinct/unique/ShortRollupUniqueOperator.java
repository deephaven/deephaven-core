//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharRollupUniqueOperator and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.unique;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.*;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.ShortCompactModifications;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedIntChunk;
import io.deephaven.engine.table.impl.ssms.ShortSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.util.compact.ShortCompactKernel;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator computes the single unique value of a particular aggregated state. If there are no values at all the
 * 'no value key' is used. If there are more than one values for the state, the 'non unique key' is used.
 *
 * it is intended to be used at the second, and higher levels of rollup.
 */
public class ShortRollupUniqueOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final ShortSsmBackedSource ssms;
    private final ShortArraySource internalResult;
    private final ColumnSource<?> externalResult;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final boolean countNullNaN;
    private final short onlyNullsSentinel;
    private final short nonUniqueSentinel;

    private UpdateCommitter<ShortRollupUniqueOperator> prevFlusher = null;
    private WritableRowSet touchedStates;

    public ShortRollupUniqueOperator(
            // region Constructor
            // endregion Constructor
            String name,
            boolean countNullNaN,
            short onlyNullsSentinel,
            short nonUniqueSentinel) {
        this.name = name;
        this.countNullNaN = countNullNaN;
        this.nonUniqueSentinel = nonUniqueSentinel;
        this.onlyNullsSentinel = onlyNullsSentinel;
        // region SsmCreation
        this.ssms = new ShortSsmBackedSource();
        // endregion SsmCreation
        // region ResultCreation
        this.internalResult = new ShortArraySource();
        // endregion ResultCreation
        // region ResultAssignment
        this.externalResult = internalResult;
        // endregion ResultAssignment
        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(SsmDistinctContext.NODE_SIZE);
    }

    // region Bucketed Updates
    private BucketSsmDistinctRollupContext updateAddValues(BucketSsmDistinctRollupContext bucketedContext,
            Chunk<? extends Values> inputs,
            IntChunk<ChunkPositions> starts,
            IntChunk<ChunkLengths> lengths) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        bucketedContext.lengthCopy.setSize(lengths.size());
        bucketedContext.starts.setSize(lengths.size());
        if (bucketedContext.valueCopy.get() != null) {
            bucketedContext.valueCopy.get().setSize(0);
            bucketedContext.counts.get().setSize(0);
        }

        // Now fill the valueCopy set with the expanded underlying SSMs
        int currentPos = 0;
        for (int ii = 0; ii < starts.size(); ii++) {
            bucketedContext.starts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for (int kk = startPos; kk < startPos + curLength; kk++) {
                final ShortSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if (ssm == null || (size = ssm.intSize()) == 0) {
                    continue;
                }

                bucketedContext.valueCopy.ensureCapacityPreserve(currentPos + newLength + size);
                ssm.fillKeyChunk(bucketedContext.valueCopy.get(), currentPos + newLength);

                newLength += size;
                // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                bucketedContext.valueCopy.get().setSize(currentPos + newLength);
            }

            // If we wrote anything into values, compact and count them, and recompute the updated length
            if (newLength > 0) {
                bucketedContext.counts.ensureCapacityPreserve(currentPos + newLength);
                bucketedContext.counts.get().setSize(currentPos + newLength);
                newLength = ShortCompactKernel.compactAndCount(bucketedContext.valueCopy.get().asWritableShortChunk(),
                        bucketedContext.counts.get(), currentPos, newLength, countNullNaN, countNullNaN);
            }

            bucketedContext.lengthCopy.set(ii, newLength);
            currentPos += newLength;
        }

        return bucketedContext;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context =
                updateAddValues((BucketSsmDistinctRollupContext) bucketedContext, values, startPositions, length);

        final WritableShortChunk<? extends Values> valueCopy =
                context.valueCopy.get() == null ? null : context.valueCopy.get().asWritableShortChunk();
        final WritableIntChunk<ChunkLengths> counts = context.counts.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = context.starts.get(ii);
            final int origStartPos = startPositions.get(ii);
            final long destination = destinations.get(origStartPos);

            final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final boolean anyAdded = ssm.insert(valueCopy, counts, startPosition, runLength);
            updateResult(ssm, destination);
            stateModified.set(ii, anyAdded);
        }
    }

    private BucketSsmDistinctRollupContext updateRemoveValues(BucketSsmDistinctRollupContext context,
            Chunk<? extends Values> inputs,
            IntChunk<ChunkPositions> starts,
            IntChunk<ChunkLengths> lengths) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        context.lengthCopy.setSize(lengths.size());
        context.starts.setSize(lengths.size());
        if (context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }

        // Now fill the valueCopy set with the expanded underlying SSMs
        int currentPos = 0;
        for (int ii = 0; ii < starts.size(); ii++) {
            context.starts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for (int kk = startPos; kk < startPos + curLength; kk++) {
                final ShortSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if (ssm == null || (size = ssm.getRemovedSize()) == 0) {
                    continue;
                }

                context.valueCopy.ensureCapacityPreserve(currentPos + newLength + size);
                ssm.fillRemovedChunk(context.valueCopy.get().asWritableShortChunk(), currentPos + newLength);

                newLength += size;
                // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                context.valueCopy.get().setSize(currentPos + newLength);
            }

            // If we wrote anything into values, compact and count them, and recompute the updated length
            if (newLength > 0) {
                context.counts.ensureCapacityPreserve(currentPos + newLength);
                context.counts.get().setSize(currentPos + newLength);
                newLength = ShortCompactKernel.compactAndCount(context.valueCopy.get().asWritableShortChunk(),
                        context.counts.get(), currentPos, newLength, countNullNaN, countNullNaN);
            }

            context.lengthCopy.set(ii, newLength);
            currentPos += newLength;
        }

        return context;
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context =
                updateRemoveValues((BucketSsmDistinctRollupContext) bucketedContext, values, startPositions, length);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableShortChunk<? extends Values> valueCopy =
                context.valueCopy.get() == null ? null : context.valueCopy.get().asWritableShortChunk();
        final WritableIntChunk<ChunkLengths> counts = context.counts.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = context.starts.get(ii);
            final int origStartPos = startPositions.get(ii);
            final long destination = destinations.get(origStartPos);

            final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);
            ssm.remove(removeContext, valueCopy, counts, startPosition, runLength);
            if (ssm.isEmpty()) {
                clearSsm(destination);
            }

            updateResult(ssm, destination);
            stateModified.set(ii, ssm.getRemovedSize() > 0);
        }
    }

    /**
     * Flatten each child's per-cycle removed (or added) delta values for every bucket into a single contiguous run,
     * without compacting: the per-bucket run begins at {@code targetStarts[ii]} and has length
     * {@code targetLengths[ii]} (the sum of the buckets' child delta sizes). The values are left uncompacted because
     * {@link ShortCompactModifications#compactAndCountModifications} sorts, counts, and diffs each run in place. The
     * destination value/count chunks are grown and sized to hold every bucket's run.
     */
    private void flattenDeltas(SizedChunk<Values> targetValues, SizedIntChunk<ChunkLengths> targetCounts,
            WritableIntChunk<ChunkPositions> targetStarts, WritableIntChunk<ChunkLengths> targetLengths,
            Chunk<? extends Values> inputs, IntChunk<ChunkPositions> starts, IntChunk<ChunkLengths> lengths,
            boolean removed) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        targetLengths.setSize(lengths.size());
        targetStarts.setSize(lengths.size());
        if (targetValues.get() != null) {
            targetValues.get().setSize(0);
        }

        int currentPos = 0;
        for (int ii = 0; ii < starts.size(); ii++) {
            targetStarts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for (int kk = startPos; kk < startPos + curLength; kk++) {
                final ShortSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if (ssm == null || (size = removed ? ssm.getRemovedSize() : ssm.getAddedSize()) == 0) {
                    continue;
                }

                targetValues.ensureCapacityPreserve(currentPos + newLength + size);
                if (removed) {
                    ssm.fillRemovedChunk(targetValues.get().asWritableShortChunk(), currentPos + newLength);
                } else {
                    ssm.fillAddedChunk(targetValues.get().asWritableShortChunk(), currentPos + newLength);
                }

                newLength += size;
                // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                targetValues.get().setSize(currentPos + newLength);
            }

            targetLengths.set(ii, newLength);
            currentPos += newLength;
        }

        // ensure the value and count chunks exist and are sized so the kernel can compact each run in place
        targetValues.ensureCapacityPreserve(currentPos);
        targetCounts.ensureCapacityPreserve(currentPos);
        targetCounts.get().setSize(currentPos);
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context = (BucketSsmDistinctRollupContext) bucketedContext;
        // flatten each side's child deltas, then diff them per bucket so only the net change touches the parent ssm
        flattenDeltas(context.valueCopy, context.counts, context.starts, context.lengthCopy, preValues, startPositions,
                length, true);
        flattenDeltas(context.postValues, context.postCounts, context.postStarts, context.postLengthCopy, postValues,
                startPositions, length, false);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        final WritableShortChunk<? extends Values> preValueCopy = context.valueCopy.get().asWritableShortChunk();
        final WritableIntChunk<ChunkLengths> removedCounts = context.counts.get();
        final WritableShortChunk<? extends Values> postValueCopy = context.postValues.get().asWritableShortChunk();
        final WritableIntChunk<ChunkLengths> addedCounts = context.postCounts.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int origStartPosition = startPositions.get(ii);
            final long destination = destinations.get(origStartPosition);
            final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);

            final int removedRunLength = context.lengthCopy.get(ii);
            final int addedRunLength = context.postLengthCopy.get(ii);
            if (removedRunLength != 0 || addedRunLength != 0) {
                ShortCompactModifications.compactAndCountModifications(preValueCopy, removedCounts, postValueCopy,
                        addedCounts, context.starts.get(ii), removedRunLength, context.postStarts.get(ii),
                        addedRunLength, countNullNaN, countNullNaN, context.removedSize, context.addedSize);
                final int removed = context.removedSize.get();
                if (removed > 0) {
                    ssm.remove(removeContext, preValueCopy, removedCounts, context.starts.get(ii), removed);
                }
                final int added = context.addedSize.get();
                if (added > 0) {
                    ssm.insert(postValueCopy, addedCounts, context.postStarts.get(ii), added);
                }
                if (ssm.isEmpty()) {
                    clearSsm(destination);
                }
            }

            updateResult(ssm, destination);
            stateModified.set(ii, ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0);
        }
    }
    // endregion

    // region Singleton Updates
    private SsmDistinctRollupContext updateAddValues(SsmDistinctRollupContext context,
            Chunk<? extends Values> inputs) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if (context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }

        if (values.size() == 0) {
            return context;
        }

        int currentPos = 0;
        for (int ii = 0; ii < values.size(); ii++) {
            final ShortSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if (ssm == null || (size = ssm.intSize()) == 0) {
                continue;
            }
            context.valueCopy.ensureCapacityPreserve(currentPos + size);
            ssm.fillKeyChunk(context.valueCopy.get(), currentPos);
            currentPos += size;
            // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            context.valueCopy.get().setSize(currentPos);
        }

        if (currentPos > 0) {
            context.counts.ensureCapacityPreserve(currentPos);
            context.counts.get().setSize(currentPos);
            ShortCompactKernel.compactAndCount(context.valueCopy.get().asWritableShortChunk(), context.counts.get(),
                    countNullNaN, countNullNaN);
        }
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctRollupContext context = updateAddValues((SsmDistinctRollupContext) singletonContext, values);
        final WritableChunk<? extends Values> updatedValues = context.valueCopy.get();
        if (updatedValues == null || updatedValues.size() == 0) {
            return false;
        }

        final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final boolean anyAdded = ssm.insert(updatedValues, context.counts.get());
        updateResult(ssm, destination);

        return anyAdded;
    }

    private SsmDistinctRollupContext updateRemoveValues(SsmDistinctRollupContext context,
            Chunk<? extends Values> inputs) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if (context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }
        if (values.size() == 0) {
            return context;
        }

        int currentPos = 0;
        for (int ii = 0; ii < values.size(); ii++) {
            final ShortSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if (ssm == null || (size = ssm.getRemovedSize()) == 0) {
                continue;
            }

            context.valueCopy.ensureCapacityPreserve(currentPos + size);
            ssm.fillRemovedChunk(context.valueCopy.get().asWritableShortChunk(), currentPos);
            currentPos += size;
            // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            context.valueCopy.get().setSize(currentPos);
        }

        if (currentPos > 0) {
            context.counts.ensureCapacityPreserve(currentPos);
            context.counts.get().setSize(currentPos);
            ShortCompactKernel.compactAndCount(context.valueCopy.get().asWritableShortChunk(), context.counts.get(),
                    countNullNaN, countNullNaN);
        }
        return context;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctRollupContext context =
                updateRemoveValues((SsmDistinctRollupContext) singletonContext, values);
        final WritableChunk<? extends Values> updatedValues = context.valueCopy.get();
        if (updatedValues == null || updatedValues.size() == 0) {
            return false;
        }

        final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);
        ssm.remove(context.removeContext, updatedValues, context.counts.get());
        if (ssm.isEmpty()) {
            clearSsm(destination);
        }

        updateResult(ssm, destination);
        return ssm.getRemovedSize() > 0;
    }

    /**
     * Flatten each child's per-cycle removed (or added) delta values into {@code targetValues} without compacting,
     * returning the number flattened. The values are left uncompacted because
     * {@link ShortCompactModifications#compactAndCountModifications} sorts, counts, and diffs the run in place.
     */
    private int flattenDeltas(SizedChunk<Values> targetValues, SizedIntChunk<ChunkLengths> targetCounts,
            Chunk<? extends Values> inputs, boolean removed) {
        final ObjectChunk<ShortSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if (targetValues.get() != null) {
            targetValues.get().setSize(0);
        }

        int currentPos = 0;
        for (int ii = 0; ii < values.size(); ii++) {
            final ShortSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if (ssm == null || (size = removed ? ssm.getRemovedSize() : ssm.getAddedSize()) == 0) {
                continue;
            }

            targetValues.ensureCapacityPreserve(currentPos + size);
            if (removed) {
                ssm.fillRemovedChunk(targetValues.get().asWritableShortChunk(), currentPos);
            } else {
                ssm.fillAddedChunk(targetValues.get().asWritableShortChunk(), currentPos);
            }
            currentPos += size;
            // we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            targetValues.get().setSize(currentPos);
        }

        // ensure the value and count chunks exist and are sized so the kernel can compact the run in place
        targetValues.ensureCapacityPreserve(currentPos);
        targetCounts.ensureCapacityPreserve(currentPos);
        targetCounts.get().setSize(currentPos);
        return currentPos;
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues,
            Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SsmDistinctRollupContext context = (SsmDistinctRollupContext) singletonContext;
        // flatten each side's child deltas, then diff them so only the net change touches the parent ssm
        final int removedTotal = flattenDeltas(context.valueCopy, context.counts, preValues, true);
        final int addedTotal = flattenDeltas(context.postValues, context.postCounts, postValues, false);
        if (removedTotal == 0 && addedTotal == 0) {
            return false;
        }

        ShortCompactModifications.compactAndCountModifications(context.valueCopy.get().asWritableShortChunk(),
                context.counts.get(), context.postValues.get().asWritableShortChunk(), context.postCounts.get(),
                0, removedTotal, 0, addedTotal, countNullNaN, countNullNaN, context.removedSize, context.addedSize);
        final ShortSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final int removed = context.removedSize.get();
        if (removed > 0) {
            ssm.remove(context.removeContext, context.valueCopy.get(), context.counts.get(), 0, removed);
        }
        final int added = context.addedSize.get();
        if (added > 0) {
            ssm.insert(context.postValues.get(), context.postCounts.get(), 0, added);
        }
        if (ssm.isEmpty()) {
            clearSsm(destination);
        }
        updateResult(ssm, destination);
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

    @Override
    public void ensureCapacity(long tableSize) {
        internalResult.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put(name, externalResult);
        columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COLUMN_ID + RollupConstants.ROLLUP_COLUMN_SUFFIX,
                ssms.getUnderlyingSource());
        return columns;
    }

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException("startTrackingPrevValues must only be called once");
        }

        prevFlusher = new UpdateCommitter<>(this, ExecutionContext.getContext().getUpdateGraph(),
                ShortRollupUniqueOperator::flushPrevious);
        touchedStates = RowSetFactory.empty();
        ssms.startTrackingPrevValues();
        internalResult.startTrackingPrevValues();
    }

    private static void flushPrevious(ShortRollupUniqueOperator op) {
        if (op.touchedStates == null || op.touchedStates.isEmpty()) {
            return;
        }

        op.ssms.clearDeltas(op.touchedStates);
        op.touchedStates.clear();
    }
    // endregion

    // region Private Helpers
    private void updateResult(ShortSegmentedSortedMultiset ssm, long destination) {
        if (ssm.isEmpty()) {
            internalResult.set(destination, onlyNullsSentinel);
        } else if (ssm.size() == 1) {
            internalResult.set(destination, ssm.get(0));
        } else {
            internalResult.set(destination, nonUniqueSentinel);
        }
    }

    private ShortSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    // endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctRollupContext(ChunkType.Short, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctRollupContext(ChunkType.Short);
    }

    // endregion
}
