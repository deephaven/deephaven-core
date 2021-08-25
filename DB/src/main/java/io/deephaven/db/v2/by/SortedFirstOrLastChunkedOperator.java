package io.deephaven.db.v2.by;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sort.IntSortKernel;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sort.permute.LongPermuteKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.ssa.SegmentedSortedArray;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.LongColumnSourceRedirectionIndex;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class SortedFirstOrLastChunkedOperator implements IterativeChunkedAggregationOperator {
    private final ChunkType chunkType;
    private final boolean isFirst;
    private final Supplier<SegmentedSortedArray> ssaFactory;
    private final LongArraySource redirections;
    private final LongColumnSourceRedirectionIndex redirectionIndex;
    private final Map<String, ColumnSource<?>> resultColumns;
    private final ObjectArraySource<SegmentedSortedArray> ssas;

    SortedFirstOrLastChunkedOperator(ChunkType chunkType, boolean isFirst, MatchPair[] resultNames,
        Table originalTable) {
        this.chunkType = chunkType;
        this.isFirst = isFirst;
        this.ssaFactory = SegmentedSortedArray.makeFactory(chunkType, false, 1024);
        this.redirections = new LongArraySource();
        this.redirectionIndex = new LongColumnSourceRedirectionIndex(redirections);
        this.ssas = new ObjectArraySource<>(SegmentedSortedArray.class);

        this.resultColumns = new LinkedHashMap<>();
        for (final MatchPair mp : resultNames) {
            // noinspection unchecked,rawtypes
            resultColumns.put(mp.left(), new ReadOnlyRedirectedColumnSource(redirectionIndex,
                originalTable.getColumnSource(mp.right())));
        }
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context =
            (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = inputIndices.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(inputIndices, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions,
            length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongChunk<KeyIndices> indexSlice = context.indexResettable
                .resetFromTypedChunk(context.sortedIndices, startPosition, length.get(ii));
            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, length.get(ii));

            stateModified.set(ii, addSortedChunk(valuesSlice, indexSlice, destination));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
        LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context =
            (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = inputIndices.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(inputIndices, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions,
            length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);

            final LongChunk<KeyIndices> indexSlice = context.indexResettable
                .resetFromTypedChunk(context.sortedIndices, startPosition, length.get(ii));
            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, length.get(ii));

            stateModified.set(ii, removeSortedChunk(valuesSlice, indexSlice, destination));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
        Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
        IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
        IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context =
            (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = postShiftIndices.size();

        context.sortedIndices.setSize(inputSize);
        context.sortedIndices.copyFromTypedChunk(postShiftIndices, 0, 0, inputSize);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions,
            length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final LongChunk<KeyIndices> indexSlice = context.indexResettable
                .resetFromTypedChunk(context.sortedIndices, startPosition, runLength);
            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.remove(valuesSlice, indexSlice);
        }

        context.sortedIndices.copyFromTypedChunk(postShiftIndices, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        context.longSortKernel.sort(context.sortedIndices, context.sortedValues, startPositions,
            length);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final LongChunk<KeyIndices> indexSlice = context.indexResettable
                .resetFromTypedChunk(context.sortedIndices, startPosition, runLength);
            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.insert(valuesSlice, indexSlice);

            final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);

            if (oldValue != newValue) {
                stateModified.set(ii, true);
            } else {
                stateModified.set(ii, hasRedirection(postShiftIndices, newValue, startPosition,
                    startPosition + runLength));
            }
        }
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
        Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices,
        LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations,
        IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
        WritableBooleanChunk<Values> stateModified) {
        final SortedFirstOrLastBucketedContext context =
            (SortedFirstOrLastBucketedContext) bucketedContext;
        final int inputSize = newValues.size();

        final WritableLongChunk<KeyIndices> sortedPreIndices = context.sortedIndices;
        sortedPreIndices.setSize(inputSize);

        context.sortedPositions.setSize(inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues, startPositions,
            length);

        // now permute the indices according to sortedPosition
        LongPermuteKernel.permuteInput(preShiftIndices, context.sortedPositions, sortedPreIndices);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.remove(valuesSlice, context.indexResettable.resetFromTypedChunk(sortedPreIndices,
                startPosition, runLength));
        }

        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues, startPositions,
            length);
        LongPermuteKernel.permuteInput(postShiftIndices, context.sortedPositions,
            context.sortedPostIndices);

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final int runLength = length.get(ii);

            final Chunk<Values> valuesSlice = context.valuesResettable
                .resetFromChunk(context.sortedValues, startPosition, runLength);

            final SegmentedSortedArray ssa = ssaForSlot(destination);
            ssa.insert(valuesSlice, context.indexResettable
                .resetFromTypedChunk(context.sortedPostIndices, startPosition, runLength));

            final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
            final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
            final boolean changed = newValue != oldValue;
            // if we just shifted something, then this is not a true modification (and modifyIndices
            // will catch it later);
            // if on the other hand, our index changed, then we must mark the state as modified
            final int chunkLocationOfRelevance =
                isFirst ? startPosition : startPosition + runLength - 1;
            final long chunkNewValue = context.sortedPostIndices.get(chunkLocationOfRelevance);
            if (chunkNewValue == newValue) {
                final int chunkIndex = binarySearch(postShiftIndices, chunkNewValue, startPosition,
                    startPosition + runLength);
                final long chunkOldValue = preShiftIndices.get(chunkIndex);
                // if the index was modified, then we must set modification to true; otherwise we
                // depend on the
                // modifyIndices call to catch if the row was modified
                if (chunkOldValue != oldValue) {
                    stateModified.set(ii, true);
                }
            } else {
                stateModified.set(ii, changed);
            }
        }
    }

    @Override
    public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices,
        IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions,
        IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int slotSize = length.get(ii);
            final long destination = destinations.get(startPosition);

            final long redirectedRow = redirections.getUnsafe(destination);
            stateModified.set(ii, hasRedirection(inputIndices, redirectedRow, startPosition,
                startPosition + slotSize));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        final SortedFirstOrLastSingletonContext context =
            (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = inputIndices.size();

        context.sortedIndices.copyFromTypedChunk(inputIndices, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);
        return addSortedChunk(context.sortedValues, context.sortedIndices, destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices,
        long destination) {
        final SortedFirstOrLastSingletonContext context =
            (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = inputIndices.size();

        context.sortedIndices.copyFromTypedChunk(inputIndices, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(values, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);
        return removeSortedChunk(context.sortedValues, context.sortedIndices, destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize,
        Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues,
        LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final SortedFirstOrLastSingletonContext context =
            (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = postShiftIndices.size();

        context.sortedIndices.copyFromTypedChunk(postShiftIndices, 0, 0, inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.sortedIndices.setSize(inputSize);
        context.sortedValues.setSize(inputSize);

        context.longSortKernel.sort(context.sortedIndices, context.sortedValues);

        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(context.sortedValues, context.sortedIndices);

        context.sortedIndices.copyFromTypedChunk(postShiftIndices, 0, 0, inputSize);
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
        return hasRedirection(postShiftIndices, newValue, 0, inputSize);
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext,
        Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues,
        LongChunk<? extends KeyIndices> preInputIndices,
        LongChunk<? extends KeyIndices> postInputIndices, long destination) {
        final SortedFirstOrLastSingletonContext context =
            (SortedFirstOrLastSingletonContext) singletonContext;
        final int inputSize = preInputIndices.size();

        context.sortedPositions.setSize(inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.sortedValues.setSize(inputSize);
        context.sortedValues.copyFromChunk(previousValues, 0, 0, inputSize);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues);

        // now permute the indices according to sortedPosition
        context.sortedIndices.setSize(inputSize);
        LongPermuteKernel.permuteInput(preInputIndices, context.sortedPositions,
            context.sortedIndices);

        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(context.sortedValues, context.sortedIndices);

        context.sortedValues.copyFromChunk(newValues, 0, 0, inputSize);
        ChunkUtils.fillInOrder(context.sortedPositions);
        context.intSortKernel.sort(context.sortedPositions, context.sortedValues);

        // now permute the indices according to sortedPosition
        LongPermuteKernel.permuteInput(postInputIndices, context.sortedPositions,
            context.sortedIndices);
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
            final int newChunkIndex = binarySearch(postInputIndices, chunkNewValue, 0, inputSize);
            final long oldChunkValue = preInputIndices.get(newChunkIndex);
            // if the index changed, then we are modified; for cases where the index did not change,
            // then we are
            // depending on the modifyIndices call to catch this row's modification
            return oldChunkValue != oldValue;
        }

        // our new value was not the chunk's value so any change is not just shifting our new value
        // somewhere
        return oldValue != newValue;
    }

    @Override
    public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices,
        long destination) {
        if (indices.size() == 0) {
            return false;
        }
        final long redirectedRow = redirections.getUnsafe(destination);
        // if indices contains redirectedRow, the we are modified, otherwise not
        return hasRedirection(indices, redirectedRow, 0, indices.size());
    }

    private static boolean hasRedirection(LongChunk<? extends KeyIndices> indices,
        long redirectedRow, int lo, int hi) {
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

    private static int binarySearch(LongChunk<? extends KeyIndices> indices, long searchValue,
        int lo, int hi) {
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

    private boolean addSortedChunk(Chunk<Values> values, LongChunk<KeyIndices> indices,
        long destination) {
        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.insert(values, indices);
        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
        return oldValue != newValue;
    }

    private SegmentedSortedArray ssaForSlot(long destination) {
        SegmentedSortedArray ssa = ssas.getUnsafe(destination);
        if (ssa == null) {
            ssas.set(destination, ssa = ssaFactory.get());
        }
        return ssa;
    }

    private boolean removeSortedChunk(Chunk<Values> values, LongChunk<KeyIndices> indices,
        long destination) {
        final SegmentedSortedArray ssa = ssaForSlot(destination);
        ssa.remove(values, indices);
        final long newValue = isFirst ? ssa.getFirst() : ssa.getLast();
        final long oldValue = redirections.getAndSetUnsafe(destination, newValue);
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
        redirectionIndex.startTrackingPrevValues();
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }

    private static class SortedFirstOrLastSingletonContext implements SingletonContext {
        private final WritableLongChunk<KeyIndices> sortedIndices;
        private final WritableChunk<Values> sortedValues;
        private final WritableIntChunk<ChunkPositions> sortedPositions;
        private final LongSortKernel<Values, KeyIndices> longSortKernel;
        private final IntSortKernel<Values, ChunkPositions> intSortKernel;

        private SortedFirstOrLastSingletonContext(ChunkType chunkType, int size) {
            sortedIndices = WritableLongChunk.makeWritableChunk(size);
            sortedValues = chunkType.makeWritableChunk(size);
            sortedPositions = WritableIntChunk.makeWritableChunk(size);
            longSortKernel =
                LongSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
            intSortKernel =
                IntSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
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
        final WritableLongChunk<KeyIndices> sortedIndices;
        final WritableLongChunk<KeyIndices> sortedPostIndices;
        final WritableChunk<Values> sortedValues;
        final ResettableLongChunk<KeyIndices> indexResettable;
        final ResettableReadOnlyChunk<Values> valuesResettable;
        final LongSortKernel<Values, KeyIndices> longSortKernel;
        final IntSortKernel<Values, ChunkPositions> intSortKernel;
        final WritableIntChunk<ChunkPositions> sortedPositions;

        private SortedFirstOrLastBucketedContext(ChunkType chunkType, int size) {
            sortedIndices = WritableLongChunk.makeWritableChunk(size);
            sortedPostIndices = WritableLongChunk.makeWritableChunk(size);
            sortedValues = chunkType.makeWritableChunk(size);
            indexResettable = ResettableLongChunk.makeResettableChunk();
            valuesResettable = chunkType.makeResettableReadOnlyChunk();
            longSortKernel =
                LongSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
            intSortKernel =
                IntSortKernel.makeContext(chunkType, SortingOrder.Ascending, size, true);
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
