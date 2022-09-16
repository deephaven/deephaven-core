/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharRollupCountDistinctOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct.count;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.by.RollupConstants;
import io.deephaven.engine.table.impl.by.ssmcountdistinct.*;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.engine.table.impl.util.compact.LongCompactKernel;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This operator counts the distinct values within the source.
 */
public class LongRollupCountDistinctOperator implements IterativeChunkedAggregationOperator {
    private final String name;

    private final LongSsmBackedSource ssms;
    private final Supplier<SegmentedSortedMultiSet.RemoveContext> removeContextFactory;
    private final LongArraySource resultColumn;
    private final boolean countNull;

    private UpdateCommitter<LongRollupCountDistinctOperator> prevFlusher = null;
    private WritableRowSet touchedStates;

    public LongRollupCountDistinctOperator(
                                           // region Constructor
                                           // endregion Constructor
                                           String name,
                                           boolean countNulls) {
        this.name = name;
        this.countNull = countNulls;
        this.resultColumn = new LongArraySource();

        // region SsmCreation
        this.ssms = new LongSsmBackedSource();
        // endregion SsmCreation

        removeContextFactory = SegmentedSortedMultiSet.makeRemoveContextFactory(SsmDistinctContext.NODE_SIZE);
    }

    //region Bucketed Updates
    private BucketSsmDistinctRollupContext updateAddValues(BucketSsmDistinctRollupContext bucketedContext,
                                                           Chunk<? extends Values> inputs,
                                                           IntChunk<ChunkPositions> starts,
                                                           IntChunk<ChunkLengths> lengths) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        bucketedContext.lengthCopy.setSize(lengths.size());
        bucketedContext.starts.setSize(lengths.size());
        if(bucketedContext.valueCopy.get() != null) {
            bucketedContext.valueCopy.get().setSize(0);
            bucketedContext.counts.get().setSize(0);
        }

        // Now fill the valueCopy set with the expanded underlying SSMs
        int currentPos = 0;
        for(int ii = 0; ii< starts.size(); ii++) {
            bucketedContext.starts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for(int kk = startPos; kk < startPos + curLength; kk++) {
                final LongSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if(ssm == null || (size = ssm.intSize()) == 0) {
                    continue;
                }

                bucketedContext.valueCopy.ensureCapacityPreserve(currentPos + newLength + size);
                ssm.fillKeyChunk(bucketedContext.valueCopy.get(), currentPos + newLength);

                newLength += size;
                //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                bucketedContext.valueCopy.get().setSize(currentPos + newLength);
            }

            // If we wrote anything into values, compact and count them, and recompute the updated length
            if(newLength > 0) {
                bucketedContext.counts.ensureCapacityPreserve(currentPos + newLength);
                bucketedContext.counts.get().setSize(currentPos + newLength);
                newLength = LongCompactKernel.compactAndCount(bucketedContext.valueCopy.get().asWritableLongChunk(), bucketedContext.counts.get(), currentPos, newLength, countNull);
            }

            bucketedContext.lengthCopy.set(ii, newLength);
            currentPos += newLength;
        }

        return bucketedContext;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context = updateAddValues((BucketSsmDistinctRollupContext)bucketedContext, values, startPositions, length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = context.starts.get(ii);
            final int origStartPos = startPositions.get(ii);
            final long destination = destinations.get(origStartPos);

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy.get(), startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts.get(), startPosition, runLength);
            final boolean anyAdded = ssm.insert(valueSlice, countSlice);
            updateResult(ssm.size(), destination);

            stateModified.set(ii, anyAdded);
        }
    }

    private BucketSsmDistinctRollupContext updateRemoveValues(BucketSsmDistinctRollupContext context,
                                                              Chunk<? extends Values> inputs,
                                                              IntChunk<ChunkPositions> starts,
                                                              IntChunk<ChunkLengths> lengths) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        context.lengthCopy.setSize(lengths.size());
        context.starts.setSize(lengths.size());
        if(context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }

        // Now fill the valueCopy set with the expanded underlying SSMs
        int currentPos = 0;
        for(int ii = 0; ii< starts.size(); ii++) {
            context.starts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for(int kk = startPos; kk < startPos + curLength; kk++) {
                final LongSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if(ssm == null || (size = ssm.getRemovedSize()) == 0) {
                    continue;
                }

                context.valueCopy.ensureCapacityPreserve(currentPos + newLength + size);
                ssm.fillRemovedChunk(context.valueCopy.get().asWritableLongChunk(), currentPos + newLength);

                newLength += size;
                //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                context.valueCopy.get().setSize(currentPos + newLength);
            }

            // If we wrote anything into values, compact and count them, and recompute the updated length
            if(newLength > 0) {
                context.counts.ensureCapacityPreserve(currentPos + newLength);
                context.counts.get().setSize(currentPos + newLength);
                newLength = LongCompactKernel.compactAndCount(context.valueCopy.get().asWritableLongChunk(), context.counts.get(), currentPos, newLength, countNull);
            }

            context.lengthCopy.set(ii, newLength);
            currentPos += newLength;
        }

        return context;
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context = updateRemoveValues((BucketSsmDistinctRollupContext)bucketedContext, values, startPositions, length);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }

            final int startPosition = context.starts.get(ii);
            final int origStartPos = startPositions.get(ii);
            final long destination = destinations.get(origStartPos);

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy.get(), startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts.get(), startPosition, runLength);
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                clearSsm(destination);
            }

            updateResult(ssm.size(), destination);
            stateModified.set(ii, ssm.getRemovedSize() > 0);
        }
    }

    private void updateModifyAddValues(BucketSsmDistinctRollupContext context,
                                       Chunk<? extends Values> inputs,
                                       IntChunk<ChunkPositions> starts,
                                       IntChunk<ChunkLengths> lengths) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> inputValues = inputs.asObjectChunk();

        context.lengthCopy.setSize(lengths.size());
        context.starts.setSize(lengths.size());
        if(context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }

        // Now fill the valueCopy set with the expanded underlying SSMs
        int currentPos = 0;
        for(int ii = 0; ii< starts.size(); ii++) {
            context.starts.set(ii, currentPos);

            final int startPos = starts.get(ii);
            final int curLength = lengths.get(ii);
            int newLength = 0;
            for(int kk = startPos; kk < startPos + curLength; kk++) {
                final LongSegmentedSortedMultiset ssm = inputValues.get(kk);
                final int size;
                if(ssm == null || (size = ssm.getAddedSize()) == 0) {
                    continue;
                }

                context.valueCopy.ensureCapacityPreserve(currentPos + newLength + size);
                ssm.fillAddedChunk(context.valueCopy.get().asWritableLongChunk(), currentPos + newLength);

                newLength += size;
                //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
                context.valueCopy.get().setSize(currentPos + newLength);
            }

            // If we wrote anything into values, compact and count them, and recompute the updated length
            if(newLength > 0) {
                context.counts.ensureCapacityPreserve(currentPos + newLength);
                context.counts.get().setSize(currentPos + newLength);
                newLength = LongCompactKernel.compactAndCount(context.valueCopy.get().asWritableLongChunk(), context.counts.get(), currentPos, newLength, countNull);
            }

            context.lengthCopy.set(ii, newLength);
            currentPos += newLength;
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final BucketSsmDistinctRollupContext context = updateRemoveValues((BucketSsmDistinctRollupContext)bucketedContext, preValues, startPositions, length);

        final SegmentedSortedMultiSet.RemoveContext removeContext = removeContextFactory.get();
        context.ssmsToMaybeClear.fillWithValue(0, destinations.size(), false);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            if (runLength == 0) {
                continue;
            }
            final int startPosition = context.starts.get(ii);
            final int origStartPosition = startPositions.get(ii);
            final long destination = destinations.get(origStartPosition);

            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
            final WritableChunk<? extends Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy.get(), startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts.get(), startPosition, runLength);
            ssm.remove(removeContext, valueSlice, countSlice);
            if (ssm.size() == 0) {
                context.ssmsToMaybeClear.set(ii, true);
            }
        }

        updateModifyAddValues(context, postValues, startPositions, length);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int runLength = context.lengthCopy.get(ii);
            final int startPosition = context.starts.get(ii);
            final int origStartPosition = startPositions.get(ii);
            final long destination = destinations.get(origStartPosition);
            final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);

            if (runLength == 0) {
                long newSize = 0;
                if (context.ssmsToMaybeClear.get(ii)) {
                    // we may have deleted this position on the last round, really get rid of it
                    clearSsm(destination);
                } else {
                    newSize = ssm.size();
                }

                updateResult(newSize, destination);
                stateModified.set(ii, ssm.getRemovedSize() > 0);
                continue;
            }

            final WritableChunk<? extends Values> valueSlice = context.valueResettable.resetFromChunk(context.valueCopy.get(), startPosition, runLength);
            final WritableIntChunk<ChunkLengths> countSlice = context.countResettable.resetFromChunk(context.counts.get(), startPosition, runLength);
            ssm.insert(valueSlice, countSlice);
            updateResult(ssm.size(), destination);
            stateModified.set(ii, ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0);
        }
    }
    //endregion

    //region Singleton Updates
    private SsmDistinctRollupContext updateAddValues(SsmDistinctRollupContext context,
                                                     Chunk<? extends Values> inputs) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if(context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }

        if(values.size() == 0) {
            return context;
        }

        int currentPos = 0;
        for(int ii = 0; ii < values.size(); ii++) {
            final LongSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if(ssm == null || (size = ssm.intSize()) == 0) {
                continue;
            }
            context.valueCopy.ensureCapacityPreserve(currentPos + size);
            ssm.fillKeyChunk(context.valueCopy.get(), currentPos);
            currentPos += size;
            //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            context.valueCopy.get().setSize(currentPos);
        }

        if(currentPos > 0) {
            context.counts.ensureCapacityPreserve(currentPos);
            context.counts.get().setSize(currentPos);
            LongCompactKernel.compactAndCount(context.valueCopy.get().asWritableLongChunk(), context.counts.get(), countNull);
        }
        return context;
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctRollupContext context = updateAddValues((SsmDistinctRollupContext)singletonContext, values);
        final WritableChunk<? extends Values> updatedValues = context.valueCopy.get();
        if (updatedValues == null || updatedValues.size() == 0) {
            return false;
        }

        final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
        final boolean anyAdded = ssm.insert(updatedValues, context.counts.get());
        updateResult(ssm.size(), destination);

        return anyAdded;
    }

    private SsmDistinctRollupContext updateRemoveValues(SsmDistinctRollupContext context,
                                                        Chunk<? extends Values> inputs) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if(context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }
        if(values.size() == 0) {
            return context;
        }

        int currentPos = 0;
        for(int ii = 0; ii < values.size(); ii++) {
            final LongSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if(ssm == null || (size = ssm.getRemovedSize()) == 0) {
                continue;
            }

            context.valueCopy.ensureCapacityPreserve(currentPos + size);
            ssm.fillRemovedChunk(context.valueCopy.get().asWritableLongChunk(), currentPos);
            currentPos += size;
            //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            context.valueCopy.get().setSize(currentPos);
        }

        if(currentPos > 0) {
            context.counts.ensureCapacityPreserve(currentPos);
            context.counts.get().setSize(currentPos);
            LongCompactKernel.compactAndCount(context.valueCopy.get().asWritableLongChunk(), context.counts.get(), countNull);
        }
        return context;
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SsmDistinctRollupContext context = updateRemoveValues((SsmDistinctRollupContext)singletonContext, values);
        final WritableChunk<? extends Values> updatedValues = context.valueCopy.get();
        if (updatedValues == null || updatedValues.size() == 0) {
            return false;
        }

        final LongSegmentedSortedMultiset ssm = ssmForSlot(destination);
        ssm.remove(context.removeContext, updatedValues, context.counts.get());
        if (ssm.size() == 0) {
            clearSsm(destination);
        }

        updateResult(ssm.size(), destination);
        return ssm.getRemovedSize() > 0;
    }

    private void updateModifyAddValues(SsmDistinctRollupContext context,
                                       Chunk<? extends Values> inputs) {
        final ObjectChunk<LongSegmentedSortedMultiset, ? extends Values> values = inputs.asObjectChunk();

        if(context.valueCopy.get() != null) {
            context.valueCopy.get().setSize(0);
            context.counts.get().setSize(0);
        }
        if(values.size() == 0) {
            return;
        }

        int currentPos = 0;
        for(int ii = 0; ii < values.size(); ii++) {
            final LongSegmentedSortedMultiset ssm = values.get(ii);
            final int size;
            if(ssm == null || (size = ssm.getAddedSize()) == 0) {
                continue;
            }

            context.valueCopy.ensureCapacityPreserve(currentPos + size);
            ssm.fillAddedChunk(context.valueCopy.get().asWritableLongChunk(), currentPos);
            currentPos += size;
            //we have to do this every time otherwise ensureCapacityPreserve will not do anything.
            context.valueCopy.get().setSize(currentPos);
        }

        if(currentPos > 0) {
            context.counts.ensureCapacityPreserve(currentPos);
            context.counts.get().setSize(currentPos);
            LongCompactKernel.compactAndCount(context.valueCopy.get().asWritableLongChunk(), context.counts.get(), countNull);
        }
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> preValues, Chunk<? extends Values> postValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SsmDistinctRollupContext context = updateRemoveValues((SsmDistinctRollupContext)singletonContext, preValues);
        LongSegmentedSortedMultiset ssm = null;
        WritableChunk<? extends Values> updatedValues = context.valueCopy.get();
        if (updatedValues != null && updatedValues.size() > 0) {
            ssm = ssmForSlot(destination);
            ssm.remove(context.removeContext, updatedValues, context.counts.get());
        }

        updateModifyAddValues(context, postValues);
        updatedValues = context.valueCopy.get();
        long newSize = 0;
        if (updatedValues != null && updatedValues.size() > 0) {
            if (ssm == null) {
                ssm = ssmForSlot(destination);
            }
            ssm.insert(updatedValues, context.counts.get());
            newSize = ssm.size();
        } else if (ssm != null && ssm.size() == 0) {
            clearSsm(destination);
        } else if (ssm == null) {
            return false;
        } else {
            newSize = ssm.size();
        }
        updateResult(newSize, destination);
        return ssm.getAddedSize() > 0 || ssm.getRemovedSize() > 0;
    }
    //endregion

    //region IterativeOperator / DistinctAggregationOperator
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
        resultColumn.ensureCapacity(tableSize);
        ssms.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put(name, resultColumn);
        columns.put(name + RollupConstants.ROLLUP_DISTINCT_SSM_COLUMN_ID + RollupConstants.ROLLUP_INTERNAL_COLUMN_SUFFIX, ssms.getUnderlyingSource());
        return columns;
    }

    @Override
    public boolean requiresRunFinds() {
        return true;
    }

    @Override
    public void startTrackingPrevValues() {
        if(prevFlusher != null) {
            throw new IllegalStateException("startTrackingPrevValues must only be called once");
        }

        prevFlusher = new UpdateCommitter<>(this, LongRollupCountDistinctOperator::flushPrevious);
        touchedStates = RowSetFactory.empty();
        ssms.startTrackingPrevValues();
        resultColumn.startTrackingPrevValues();
    }

    private static void flushPrevious(LongRollupCountDistinctOperator op) {
        if(op.touchedStates == null || op.touchedStates.isEmpty()) {
            return;
        }

        op.ssms.clearDeltas(op.touchedStates);
        op.touchedStates.clear();
    }
    //endregion

    //region Private Helpers
    private void updateResult(long count, long destination) {
        resultColumn.getAndSetUnsafe(destination, count == 0 ? QueryConstants.NULL_LONG : count);
    }

    private LongSegmentedSortedMultiset ssmForSlot(long destination) {
        return ssms.getOrCreate(destination);
    }

    private void clearSsm(long destination) {
        ssms.clear(destination);
    }
    //endregion

    // region Contexts
    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new BucketSsmDistinctRollupContext(ChunkType.Long, size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new SsmDistinctRollupContext(ChunkType.Long);
    }

    //endregion
}
