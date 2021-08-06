package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.ShiftedOrderedKeys;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A firstBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamFirstByChunkedOperator implements IterativeChunkedAggregationOperator {

    private static final int COPY_CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private final int numResultColumns;
    private final ColumnSource<?>[] inputColumns;
    private final WritableSource<?>[] outputColumns;

    private final Map<String, ArrayBackedColumnSource<?>> resultColumns;

    private long previousTableSize;
    private SoftReference<LongArraySource> cachedRedirections;
    private LongArraySource redirections;

    StreamFirstByChunkedOperator(@NotNull final MatchPair[] resultPairs, @NotNull final Table streamTable) {
        numResultColumns = resultPairs.length;
        inputColumns = new ColumnSource[numResultColumns];
        outputColumns = new WritableSource[numResultColumns];
        final Map<String, ArrayBackedColumnSource<?>> resultColumnsMutable = new LinkedHashMap<>(numResultColumns);
        for (int ci = 0; ci < numResultColumns; ++ci) {
            final MatchPair resultPair = resultPairs[ci];
            final ColumnSource<?> streamSource = streamTable.getColumnSource(resultPair.left());
            final ArrayBackedColumnSource<?> resultSource = ArrayBackedColumnSource.getMemoryColumnSource(0, streamSource.getType(), streamSource.getComponentType());
            resultColumnsMutable.put(resultPair.left(), resultSource);
            inputColumns[ci] = ReinterpretUtilities.maybeConvertToPrimitive(streamSource);
            outputColumns[ci] = (WritableSource<?>) ReinterpretUtilities.maybeConvertToPrimitive(resultSource);
            Assert.eq(inputColumns[ci].getChunkType(), "inputColumns[ci].getChunkType()", outputColumns[ci].getChunkType(), "outputColumns[ci].getChunkType()");
        }
        resultColumns = Collections.unmodifiableMap(resultColumnsMutable);
        cachedRedirections = new SoftReference<>(redirections = new LongArraySource());
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        redirections.ensureCapacity(tableSize - previousTableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        Arrays.stream(outputColumns).forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }

    @Override
    public boolean unchunkedIndex() {
        return true;
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        if ((redirections = cachedRedirections.get()) == null) {
            cachedRedirections = new SoftReference<>(redirections = new LongArraySource());
        }
    }

    @Override
    public void addChunk(final BucketedContext context, // Unused
                         final Chunk<? extends Values> values, // Unused
                         @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                         @NotNull final IntChunk<KeyIndices> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         final IntChunk<ChunkLengths> length, // Unused
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            redirections.set(destination - previousTableSize, inputIndices.get(startPosition));
            stateModified.set(ii, true);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext context, // Unused
                            final int chunkSize,
                            final Chunk<? extends Values> values, // Unused
                            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                            final long destination) {
        if (chunkSize == 0) {
            return false;
        }
        redirections.set(destination - previousTableSize, inputIndices.get(0));
        return true;
    }

    @Override
    public boolean addIndex(final SingletonContext context,
                            @NotNull final Index index,
                            final long destination) {
        if (index.isEmpty()) {
            return false;
        }
        redirections.set(destination - previousTableSize, index.firstKey());
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getIndex());
        redirections = null;
        previousTableSize = resultTable.size();
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
                                 @NotNull final ReadOnlyIndex newDestinations) {
        Assert.assertion(downstream.modified.empty() && downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.modified.empty() && downstream.removed.empty() && downstream.shifted.empty()");
        copyStreamToResult(downstream.added);
        redirections = null;
        previousTableSize = downstream.added.lastKey() + 1;
    }

    /**
     * <p>For each destination slot, map to the latest source index key and copy source values to destination slots for
     * all result columns.
     *
     * <p>This implementation proceeds chunk-wise in the following manner:
     * <ol>
     *     <li>Get a chunk of destination slots</l1>
     *     <li>Get a chunk of source indices</li>
     *     <lI>For each input column: get a chunk of input values and then fill the output column</li>
     * </ol>
     *
     * @param destinations The changed (added or modified) destination slots as an {@link OrderedKeys}
     */
    private void copyStreamToResult(@NotNull final OrderedKeys destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final OrderedKeys.Iterator destinationsIterator = toClose.add(destinations.getOrderedKeysIterator());
            final ShiftedOrderedKeys shiftedSliceDestinations = toClose.add(new ShiftedOrderedKeys());
            final ChunkSource.GetContext redirectionsContext = toClose.add(redirections.makeGetContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final WritableChunkSink.FillFromContext[] outputContexts = toClose.addArray(new WritableChunkSink.FillFromContext[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastKey() + 1);
            }

            while (destinationsIterator.hasMore()) {
                final OrderedKeys sliceDestinations = destinationsIterator.getNextOrderedKeysWithLength(COPY_CHUNK_SIZE);
                shiftedSliceDestinations.reset(sliceDestinations, -previousTableSize);
                final LongChunk<OrderedKeyIndices> sourceIndices = Chunk.<Values, OrderedKeyIndices>downcast(redirections.getChunk(redirectionsContext, shiftedSliceDestinations)).asLongChunk();

                try (final OrderedKeys sliceSources = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(sourceIndices)) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Values> inputChunk = inputColumns[ci].getChunk(inputContexts[ci], sliceSources);
                        outputColumns[ci].fillFromChunk(outputContexts[ci], inputChunk, sliceDestinations);
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Unsupported / illegal operations
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preShiftIndices, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyIndices(BucketedContext context, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(SingletonContext singletonContext, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> preInputIndices, LongChunk<? extends KeyIndices> postInputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyIndices(SingletonContext context, LongChunk<? extends KeyIndices> indices, long destination) {
        throw new UnsupportedOperationException();
    }
}
