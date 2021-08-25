package io.deephaven.db.v2.by;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.sort.permute.PermuteKernel;
import io.deephaven.db.v2.sort.timsort.LongIntTimsortKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.ChunkUtils;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Base-class for stream first/last-by chunked operators that need to copy data from source columns to result columns
 * with a permutation on the redirected indices.
 */
public abstract class CopyingPermutedStreamFirstOrLastChunkedOperator extends BaseStreamFirstOrLastChunkedOperator {
    /**
     * Permute kernels, parallel to {@link #outputColumns}.
     */
    protected final PermuteKernel[] permuteKernels;

    public CopyingPermutedStreamFirstOrLastChunkedOperator(@NotNull final MatchPair[] resultPairs,
            @NotNull final Table streamTable) {
        super(resultPairs, streamTable);
        permuteKernels = new PermuteKernel[numResultColumns];
        for (int ci = 0; ci < numResultColumns; ++ci) {
            permuteKernels[ci] = PermuteKernel.makePermuteKernel(outputColumns[ci].getChunkType());
        }
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        redirections.ensureCapacity(tableSize, false);
    }

    @Override
    public final void startTrackingPrevValues() {
        Arrays.stream(outputColumns).forEach(ColumnSource::startTrackingPrevValues);
    }

    /**
     * <p>
     * For each destination slot, map to the latest source index key and copy source values to destination slots for all
     * result columns.
     *
     * <p>
     * This implementation proceeds chunk-wise in the following manner:
     * <ol>
     * <li>Get a chunk of destination slots</l1>
     * <li>Fill a chunk of source indices</li>
     * <li>Sort the chunk of source indices</li>
     * <lI>For each input column: get a chunk of input values, permute it into a chunk of destination values, and then
     * fill the output column</lI>
     * </ol>
     *
     * @param destinations The changed (added or modified) destination slots as an {@link OrderedKeys}
     */
    protected void copyStreamToResult(@NotNull final OrderedKeys destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final OrderedKeys.Iterator destinationsIterator = toClose.add(destinations.getOrderedKeysIterator());
            final ChunkSource.FillContext redirectionsContext =
                    toClose.add(redirections.makeFillContext(COPY_CHUNK_SIZE));
            final WritableLongChunk<Attributes.KeyIndices> sourceIndices =
                    toClose.add(WritableLongChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final WritableIntChunk<Attributes.ChunkPositions> sourceIndicesOrder =
                    toClose.add(WritableIntChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final LongIntTimsortKernel.LongIntSortKernelContext<Attributes.KeyIndices, Attributes.ChunkPositions> sortKernelContext =
                    toClose.add(LongIntTimsortKernel.createContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts =
                    toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final WritableChunkSink.FillFromContext[] outputContexts =
                    toClose.addArray(new WritableChunkSink.FillFromContext[numResultColumns]);
            // noinspection unchecked
            final WritableChunk<Attributes.Values>[] outputChunks =
                    toClose.addArray(new WritableChunk[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputChunks[ci] = outputColumn.getChunkType().makeWritableChunk(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastKey() + 1, false);
            }

            while (destinationsIterator.hasMore()) {
                final OrderedKeys sliceDestinations =
                        destinationsIterator.getNextOrderedKeysWithLength(COPY_CHUNK_SIZE);
                redirections.fillChunk(redirectionsContext, WritableLongChunk.upcast(sourceIndices), sliceDestinations);
                sourceIndicesOrder.setSize(sourceIndices.size());
                ChunkUtils.fillInOrder(sourceIndicesOrder);
                LongIntTimsortKernel.sort(sortKernelContext, sourceIndicesOrder, sourceIndices);

                try (final OrderedKeys sliceSources =
                        OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(WritableLongChunk.downcast(sourceIndices))) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Attributes.Values> inputChunk =
                                inputColumns[ci].getChunk(inputContexts[ci], sliceSources);
                        permuteKernels[ci].permute(inputChunk, sourceIndicesOrder, outputChunks[ci]);
                        outputColumns[ci].fillFromChunk(outputContexts[ci], outputChunks[ci], sliceDestinations);
                    }
                    inputSharedContext.reset();
                }
            }
        }
    }
}
