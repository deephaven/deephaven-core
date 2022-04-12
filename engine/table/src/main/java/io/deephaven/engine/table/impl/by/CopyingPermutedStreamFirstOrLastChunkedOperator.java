package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
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
        redirections.ensureCapacity(tableSize, true);
    }

    @Override
    public final void startTrackingPrevValues() {
        Arrays.stream(outputColumns).forEach(ColumnSource::startTrackingPrevValues);
    }

    /**
     * <p>
     * For each destination slot, map to the latest source row key and copy source values to destination slots for all
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
     * @param destinations The changed (added or modified) destination slots as an {@link RowSequence}
     */
    protected void copyStreamToResult(@NotNull final RowSequence destinations) {
        try (final SafeCloseableList toClose = new SafeCloseableList()) {
            final RowSequence.Iterator destinationsIterator = toClose.add(destinations.getRowSequenceIterator());
            final ChunkSource.FillContext redirectionsContext =
                    toClose.add(redirections.makeFillContext(COPY_CHUNK_SIZE));
            final WritableLongChunk<RowKeys> sourceIndices =
                    toClose.add(WritableLongChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final WritableIntChunk<ChunkPositions> sourceIndicesOrder =
                    toClose.add(WritableIntChunk.makeWritableChunk(COPY_CHUNK_SIZE));
            final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext =
                    toClose.add(LongIntTimsortKernel.createContext(COPY_CHUNK_SIZE));
            final SharedContext inputSharedContext = toClose.add(SharedContext.makeSharedContext());
            final ChunkSource.GetContext[] inputContexts =
                    toClose.addArray(new ChunkSource.GetContext[numResultColumns]);
            final ChunkSink.FillFromContext[] outputContexts =
                    toClose.addArray(new ChunkSink.FillFromContext[numResultColumns]);
            // noinspection unchecked
            final WritableChunk<Values>[] outputChunks =
                    toClose.addArray(new WritableChunk[numResultColumns]);

            for (int ci = 0; ci < numResultColumns; ++ci) {
                inputContexts[ci] = inputColumns[ci].makeGetContext(COPY_CHUNK_SIZE, inputSharedContext);
                final WritableColumnSource<?> outputColumn = outputColumns[ci];
                outputContexts[ci] = outputColumn.makeFillFromContext(COPY_CHUNK_SIZE);
                outputChunks[ci] = outputColumn.getChunkType().makeWritableChunk(COPY_CHUNK_SIZE);
                outputColumn.ensureCapacity(destinations.lastRowKey() + 1, false);
            }

            while (destinationsIterator.hasMore()) {
                final RowSequence sliceDestinations =
                        destinationsIterator.getNextRowSequenceWithLength(COPY_CHUNK_SIZE);
                redirections.fillChunk(redirectionsContext, WritableLongChunk.upcast(sourceIndices), sliceDestinations);
                sourceIndicesOrder.setSize(sourceIndices.size());
                ChunkUtils.fillInOrder(sourceIndicesOrder);
                LongIntTimsortKernel.sort(sortKernelContext, sourceIndicesOrder, sourceIndices);

                try (final RowSequence sliceSources =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(sourceIndices))) {
                    for (int ci = 0; ci < numResultColumns; ++ci) {
                        final Chunk<? extends Values> inputChunk =
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
