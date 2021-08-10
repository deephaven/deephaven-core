package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sort.timsort.LongIntTimsortKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * A lastBy aggregation operator for stream tables.
 *
 * @see Table#STREAM_TABLE_ATTRIBUTE
 */
public class StreamLastByChunkedOperator extends StreamFirstOrLastByChunkedOperator {

    /**
     * Permute kernels, parallel to {@link #outputColumns}.
     */
    private final PermuteKernel[] permuteKernels;

    StreamLastByChunkedOperator(@NotNull final MatchPair[] resultPairs, @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
        permuteKernels = new PermuteKernel[numResultColumns];
        for (int ci = 0; ci < numResultColumns; ++ci) {
            permuteKernels[ci] = PermuteKernel.makePermuteKernel(outputColumns[ci].getChunkType());
        }
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        redirections.ensureCapacity(tableSize);
    }

    @Override
    public void addChunk(final BucketedContext context, // Unused
                         final Chunk<? extends Values> values, // Unused
                         @NotNull final LongChunk<? extends KeyIndices> inputIndices,
                         @NotNull final IntChunk<KeyIndices> destinations,
                         @NotNull final IntChunk<ChunkPositions> startPositions,
                         @NotNull final IntChunk<ChunkLengths> length,
                         @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            redirections.set(destination, inputIndices.get(startPosition + runLength - 1));
            stateModified.set(ii, true);
        }
    }

    @Override
    public final void startTrackingPrevValues() {
        Arrays.stream(outputColumns).forEach(ColumnSource::startTrackingPrevValues);
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
        redirections.set(destination, inputIndices.get(chunkSize - 1));
        return true;
    }

    @Override
    public boolean addIndex(final SingletonContext context,
                            @NotNull final Index index,
                            final long destination) {
        if (index.isEmpty()) {
            return false;
        }
        redirections.set(destination, index.lastKey());
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        copyStreamToResult(resultTable.getIndex());
        redirections = null;
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
                                 @NotNull final ReadOnlyIndex newDestinations) {
        Assert.assertion(downstream.removed.empty() && downstream.shifted.empty(),
                "downstream.removed.empty() && downstream.shifted.empty()");
        try (final ReadOnlyIndex changedDestinations = downstream.modified.union(downstream.added)) {
            copyStreamToResult(changedDestinations);
        }
        redirections = null;
    }

    /**
     * <p>For each destination slot, map to the latest source index key and copy source values to destination slots for
     * all result columns.
     *
     * <p>This implementation proceeds chunk-wise in the following manner:
     * <ol>
     *     <li>Get a chunk of destination slots</l1>
     *     <li>Fill a chunk of source indices</li>
     *     <li>Sort the chunk of source indices</li>
     *     <lI>For each input column: get a chunk of input values, permute it into a chunk of destination values, and then fill the output column</lI>
     * </ol>
     *
     * @param destinations The changed (added or modified) destination slots as an {@link OrderedKeys}
     */
    private void copyStreamToResult(@NotNull final OrderedKeys destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final OrderedKeys.Iterator destinationsIterator = toClose.add(destinations.getOrderedKeysIterator());
            final ChunkSource.FillContext redirectionsContext = toClose.add(redirections.makeFillContext(COPY_CHUNK_SIZE));
            final WritableLongChunk<KeyIndices> sourceIndices = toClose.add(WritableLongChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final WritableIntChunk<ChunkPositions> sourceIndicesOrder = toClose.add(WritableIntChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final LongIntTimsortKernel.LongIntSortKernelContext<KeyIndices, ChunkPositions> sortKernelContext = toClose.add(LongIntTimsortKernel.createContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts = toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final WritableChunkSink.FillFromContext[] outputContexts = toClose.addArray(new WritableChunkSink.FillFromContext[numResultColumns]);
            //noinspection unchecked
            final WritableChunk<Values>[] outputChunks = toClose.addArray(new WritableChunk[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputChunks[ci] = outputColumn.getChunkType().makeWritableChunk(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastKey() + 1);
            }

            while (destinationsIterator.hasMore()) {
                final OrderedKeys sliceDestinations = destinationsIterator.getNextOrderedKeysWithLength(COPY_CHUNK_SIZE);
                redirections.fillChunk(redirectionsContext, WritableLongChunk.upcast(sourceIndices), sliceDestinations);
                sourceIndicesOrder.setSize(sourceIndices.size());
                ChunkUtils.fillInOrder(sourceIndicesOrder);
                LongIntTimsortKernel.sort(sortKernelContext, sourceIndicesOrder, sourceIndices);

                try (final OrderedKeys sliceSources = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(WritableLongChunk.downcast(sourceIndices))) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Values> inputChunk = inputColumns[ci].getChunk(inputContexts[ci], sliceSources);
                        permuteKernels[ci].permute(inputChunk, sourceIndicesOrder, outputChunks[ci]);
                        outputColumns[ci].fillFromChunk(outputContexts[ci], outputChunks[ci], sliceDestinations);
                    }
                }
            }
        }
    }
}
