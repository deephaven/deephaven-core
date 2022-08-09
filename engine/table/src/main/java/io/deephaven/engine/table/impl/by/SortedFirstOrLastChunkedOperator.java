/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sort.permute.LongPermuteKernel;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SortedFirstOrLastChunkedOperator
        extends BasicStateChangeRecorder
        implements IterativeChunkedAggregationOperator {
    private final ChunkType chunkType;
    private final boolean isFirst;
    private final Supplier<SegmentedSortedArray> ssaFactory;
    private final LongArraySource redirections;
    private final LongColumnSourceWritableRowRedirection rowRedirection;
    private final Map<String, ColumnSource<?>> resultColumns;
    private final ObjectArraySource<SegmentedSortedArray> ssas;

    SortedFirstOrLastChunkedOperator(ChunkType chunkType, boolean isFirst, MatchPair[] resultNames,
            Table originalTable) {
        this.chunkType = chunkType;
        this.isFirst = isFirst;
        this.ssaFactory = SegmentedSortedArray.makeFactory(chunkType, false, 1024);
        this.redirections = new LongArraySource();
        this.rowRedirection = new LongColumnSourceWritableRowRedirection(redirections);
        this.ssas = new ObjectArraySource<>(SegmentedSortedArray.class);

        this.resultColumns = new LinkedHashMap<>();
        for (final MatchPair mp : resultNames) {
            // noinspection unchecked,rawtypes
            resultColumns.put(mp.leftColumn(),
                    new RedirectedColumnSource(rowRedirection, originalTable.getColumnSource(mp.rightColumn())));
        }
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context = (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = inputRowKeys.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(inputRowKeys, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions, length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongChunk<RowKeys> indexSlice =
                    context.indexResettable.resetFromTypedChunk(context.sortedIndices, startPosition, length.get(ii));
            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, length.get(ii));

            stateModified.set(ii, addSortedChunk(valuesSlice, indexSlice, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context = (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = inputRowKeys.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(inputRowKeys, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions, length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongChunk<RowKeys> indexSlice =
                    context.indexResettable.resetFromTypedChunk(context.sortedIndices, startPosition, length.get(ii));
            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, length.get(ii));

            stateModified.set(ii, removeSortedChunk(valuesSlice, indexSlice, destination));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context = (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = postShiftRowKeys.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(postShiftRowKeys, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions, length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final LongChunk<RowKeys> indexSlice =
                    context.indexResettable.resetFromTypedChunk(context.sortedIndices, startPosition, runLength);
            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.remove(valuesSlice, indexSlice);
        }

        context.sortedIndices.copyFromTypedChunk(postShiftRowKeys, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions, length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final LongChunk<RowKeys> indexSlice =
                    context.indexResettable.resetFromTypedChunk(context.sortedIndices, startPosition, runLength);
            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.insert(valuesSlice, indexSlice);

            final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);

            if (oldValue != newValue) {
                stateModified.set(ii, true);
            } else {
                stateModified.set(ii,
                        hasRedirection(postShiftRowKeys, newValue, startPosition, startPosition + runLength));
            }
        }
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context = (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = newValues.size();

        final WritableLongChunk<RowKeys> sortedPreIndices = context.sortedIndices;
        sortedPreIndices.setSize(inputSize);

        context.sortedPositions.setSize(inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues, startPositions, length);

        // now permute the indices according to sortedPosition
        LongPermuteKernel.permuteInput(preShiftRowKeys, context.sortedPositions, sortedPreIndices);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.remove(valuesSlice,
                    context.indexResettable.resetFromTypedChunk(sortedPreIndices, startPosition, runLength));
        }

        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues, startPositions, length);
        LongPermuteKernel.permuteInput(postShiftRowKeys, context.sortedPositions, context.sortedPostIndices);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final Chunk<Values> valuesSlice =
                    context.valuesResettable.resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.insert(valuesSlice,
                    context.indexResettable.resetFromTypedChunk(context.sortedPostIndices, startPosition, runLength));

            final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
            final boolean changed = newValue != oldValue;
            // if we just shifted something, then this is not a true modification (and modifyRowKeys will catch it
            // later);
            // if on the other hand, our row key changed, then we must mark the state as modified
            final int chunkLocationOfRelevance = isFirst ? startPosition : startPosition + runLength - 1;
            final long chunkNewValue = context.sortedPostIndices.get(chunkLocationOfRelevance);
            if (chunkNewValue == newValue) {
                final int chunkIndex =
                        binarySearch(postShiftRowKeys, chunkNewValue, startPosition, startPosition + runLength);
                final long chunkOldValue = preShiftRowKeys.get(chunkIndex);
                // if the row key was modified, then we must set modification to true; otherwise we depend on the
                // modifyRowKeys call to catch if the row was modified
                if (chunkOldValue != oldValue) {
                    stateModified.set(ii, true);
                }
            } else {
                stateModified.set(ii, changed);
            }
        }
    }

    @Override
    public void modifyRowKeys(BucketedContext context, LongChunk<? extends RowKeys> inputRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int slotSize = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long redirectedRow = redirections.getUnsafe(destination);
            stateModified.set(ii, hasRedirection(inputRowKeys, redirectedRow, startPosition, startPosition + slotSize));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SortedFirstOrLastSingletonContext context = (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = inputRowKeys.size();

        context.sortedIndices.copyFromTypedChunk(inputRowKeys, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);
        return addSortedChunk(context.sortedValues, context.sortedIndices, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final SortedFirstOrLastSingletonContext context = (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = inputRowKeys.size();

        context.sortedIndices.copyFromTypedChunk(inputRowKeys, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);
        return removeSortedChunk(context.sortedValues, context.sortedIndices, destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SortedFirstOrLastSingletonContext context = (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = postShiftRowKeys.size();

        context.sortedIndices.copyFromTypedChunk(postShiftRowKeys, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);

        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(context.sortedValues, context.sortedIndices);

        context.sortedIndices.copyFromTypedChunk(postShiftRowKeys, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);

        ssa.insert(context.sortedValues, context.sortedIndices);

        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);

        // if it changed
        if (oldValue != newValue) {
            return true;
        }

        // if we have modified the critical value in our modification; we are modified
        return hasRedirection(postShiftRowKeys, newValue, 0, inputSize);
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> preShiftRowKeys,
            LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final SortedFirstOrLastSingletonContext context = (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = preShiftRowKeys.size();

        context.sortedPositions.setSize(inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues);

        // now permute the indices according to sortedPosition
        context.sortedIndices.setSize(inputSize);
        LongPermuteKernel.permuteInput(preShiftRowKeys, context.sortedPositions, context.sortedIndices);

        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(context.sortedValues, context.sortedIndices);

        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues);

        // now permute the indices according to sortedPosition
        LongPermuteKernel.permuteInput(postShiftRowKeys, context.sortedPositions, context.sortedIndices);
        ssa.insert(context.sortedValues, context.sortedIndices);

        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);

        final long chunkNewValue;
        if (isFirst) {
            chunkNewValue = context.sortedIndices.get(0);
        } else {
            chunkNewValue = context.sortedIndices.get(inputSize - 1);
        }

        if (chunkNewValue == newValue) {
            // We are the new value; we need to determine if we were also the old value
            final int newChunkIndex = binarySearch(postShiftRowKeys, chunkNewValue, 0, inputSize);
            final long oldChunkValue = preShiftRowKeys.get(newChunkIndex);
            // if the row key changed, then we are modified; for cases where the row key did not change, then we are
            // depending on the modifyRowKeys call to catch this row's modification
            return oldChunkValue != oldValue;
        }

        // our new value was not the chunk's value so any change is not just shifting our new value somewhere
        return oldValue != newValue;
    }

    @Override
    public boolean modifyRowKeys(SingletonContext context, LongChunk<? extends RowKeys> rowKeys, long destination) {
        if (rowKeys.size() == 0) {
            return false;
        }
        final long redirectedRow = redirections.getUnsafe(destination);
        // if indices contains redirectedRow, the we are modified, otherwise not
        return hasRedirection(rowKeys, redirectedRow, 0, rowKeys.size());
    }

    private static boolean hasRedirection(LongChunk<? extends RowKeys> indices, long redirectedRow, int lo, int hi) {
        while (lo < hi) {
            final int mid = (lo + hi) / 2;
            final long candidate = indices.get(mid);
            if (candidate == redirectedRow) {
                return true;
            }
            if (candidate < redirectedRow) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return false;
    }

    private static int binarySearch(LongChunk<? extends RowKeys> indices, long searchValue, int lo, int hi) {
        while (lo < hi) {
            final int mid = (lo + hi) / 2;
            final long candidate = indices.get(mid);
            if (candidate == searchValue) {
                return mid;
            }
            if (candidate < searchValue) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        throw new IllegalStateException();
    }

    private boolean addSortedChunk(Chunk<Values> values, LongChunk<RowKeys> indices, long destination) {
        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.insert(values, indices);
        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
        if (oldValue == RowSequence.NULL_ROW_KEY && newValue != RowSequence.NULL_ROW_KEY) {
            onReincarnated(destination);
        }
        return oldValue != newValue;
    }

    private SegmentedSortedArray ssaForSlot(long destination) {
        SegmentedSortedArray ssa = ssas.getUnsafe(destination);
        if (ssa == null) {
            ssas.set(destination, ssa = ssaFactory.get());
        }
        return ssa;
    }

    private boolean removeSortedChunk(Chunk<Values> values, LongChunk<RowKeys> indices, long destination) {
        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(values, indices);
        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
        if (oldValue != RowSequence.NULL_ROW_KEY && newValue == RowSequence.NULL_ROW_KEY) {
            onEmptied(destination);
        }
        return oldValue != newValue;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        ssas.ensureCapacity(tableSize);
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        rowRedirection.startTrackingPrevValues();
    }

    @Override
    public boolean requiresRowKeys() {
        return true;
    }

    private static class SortedFirstOrLastSingletonContext implements SingletonContext {
        private final WritableLongChunk<RowKeys> sortedIndices;
        private final WritableChunk<Values> sortedValues;
        private final WritableIntChunk<ChunkPositions> sortedPositions;
        private final LongSortKernel<Values, RowKeys> longSortKernel;
        private final IntSortKernel<Values, ChunkPositions> intSortKernel;

        private SortedFirstOrLastSingletonContext(ChunkType chunkType, int size) {
            sortedIndices = WritableLongChunk.makeWritableChunk(size);
            sortedValues = chunkType.makeWritableChunk(size);
            sortedPositions = WritableIntChunk.makeWritableChunk(size);
            longSortKernel = LongSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
            intSortKernel = IntSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
        }

        @Override
        public void close() {
            sortedIndices.close();
            sortedValues.close();
            sortedPositions.close();
            longSortKernel.close();
            intSortKernel.close();
        }
    }

    @Override
    public SortedFirstOrLastSingletonContext makeSingletonContext(int size) {
        return new SortedFirstOrLastSingletonContext(chunkType, size);
    }

    private static class SortedFirstOrLastBucketedContext implements BucketedContext {
        final WritableLongChunk<RowKeys> sortedIndices;
        final WritableLongChunk<RowKeys> sortedPostIndices;
        final WritableChunk<Values> sortedValues;
        final ResettableLongChunk<RowKeys> indexResettable;
        final ResettableReadOnlyChunk<Values> valuesResettable;
        final LongSortKernel<Values, RowKeys> longSortKernel;
        final IntSortKernel<Values, ChunkPositions> intSortKernel;
        final WritableIntChunk<ChunkPositions> sortedPositions;

        private SortedFirstOrLastBucketedContext(ChunkType chunkType, int size) {
            sortedIndices = WritableLongChunk.makeWritableChunk(size);
            sortedPostIndices = WritableLongChunk.makeWritableChunk(size);
            sortedValues = chunkType.makeWritableChunk(size);
            indexResettable = ResettableLongChunk.makeResettableChunk();
            valuesResettable = chunkType.makeResettableReadOnlyChunk();
            longSortKernel = LongSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
            intSortKernel = IntSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
            sortedPositions = WritableIntChunk.makeWritableChunk(size);
        }

        @Override
        public void close() {
            sortedIndices.close();
            sortedPostIndices.close();
            sortedValues.close();
            indexResettable.close();
            valuesResettable.close();
            longSortKernel.close();
            intSortKernel.close();
            sortedPositions.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new SortedFirstOrLastBucketedContext(chunkType, size);
    }
}
