/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

public class WrappedRowSetWritableRowRedirection implements WritableRowRedirection {

    /**
     * {@link TrackingRowSet} used to map from outer key (position in the RowSet) to inner key.
     */
    private final TrackingRowSet wrappedRowSet;

    public WrappedRowSetWritableRowRedirection(final TrackingRowSet wrappedRowSet) {
        this.wrappedRowSet = wrappedRowSet;
    }

    @Override
    public synchronized long put(long outerRowKey, long innerRowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long get(long outerRowKey) {
        return wrappedRowSet.get(outerRowKey);
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        return wrappedRowSet.getPrev(outerRowKey);
    }

    private static final class FillContext implements ChunkSource.FillContext {

        private final WritableLongChunk<RowKeys> indexPositions;

        private FillContext(final int chunkCapacity) {
            indexPositions = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            indexPositions.close();
        }
    }

    /*
     * TODO: Switch to this version if we ever uncomment the override for fillChunkUnordered. private static final class
     * FillContext implements MutableChunkSource.FillContext {
     * 
     * private final int chunkCapacity; private final WritableLongChunk<RowKeys> indexPositions;
     * 
     * private LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext; private
     * WritableIntChunk<ChunkPositions> unorderedFillChunkPositions; private WritableLongChunk<RowKeys>
     * unorderedFillMappedKeys;
     * 
     * private FillContext(final int chunkCapacity) { this.chunkCapacity = chunkCapacity; indexPositions =
     * WritableLongChunk.makeWritableChunk(chunkCapacity); }
     * 
     * private void ensureUnorderedFillFieldsInitialized() { if (sortKernelContext == null) { sortKernelContext =
     * LongIntTimsortKernel.createContext(chunkCapacity); } if (unorderedFillChunkPositions == null) {
     * unorderedFillChunkPositions = WritableIntChunk.makeWritableChunk(chunkCapacity); } if (unorderedFillMappedKeys ==
     * null) { unorderedFillMappedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity); } }
     * 
     * @Override public void close() { indexPositions.close(); if (sortKernelContext != null) {
     * sortKernelContext.close(); } if (unorderedFillChunkPositions != null) { unorderedFillChunkPositions.close(); } if
     * (unorderedFillMappedKeys != null) { unorderedFillMappedKeys.close(); } } }
     */

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // NB: No need to implement sharing at this level. RedirectedColumnSource uses a SharedContext to share
        // WritableRowRedirection lookup results.
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<RowKeys> indexPositions = ((FillContext) fillContext).indexPositions;
        outerRowKeys.fillRowKeyChunk(indexPositions);
        wrappedRowSet.getKeysForPositions(new LongChunkIterator(indexPositions), new LongChunkAppender(innerRowKeys));
        innerRowKeys.setSize(outerRowKeys.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableLongChunk<? extends RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<RowKeys> indexPositions = ((FillContext) fillContext).indexPositions;
        outerRowKeys.fillRowKeyChunk(indexPositions);
        try (final RowSet prevWrappedIndex = wrappedRowSet.copyPrev()) {
            prevWrappedIndex.getKeysForPositions(new LongChunkIterator(indexPositions),
                    new LongChunkAppender(innerRowKeys));
        }
        innerRowKeys.setSize(outerRowKeys.intSize());
    }

    /*
     * TODO: Uncomment and test this if we ever start using WrappedRowSetWritableRowRedirection for unordered reads.
     * 
     * @Override public void fillChunkUnordered(@NotNull final MutableChunkSource.FillContext fillContext,
     * 
     * @NotNull final WritableLongChunk<RowKeys> mappedKeysOut,
     * 
     * @NotNull final LongChunk<RowKeys> keysToMap) { final FillContext typedFillContext = (FillContext) fillContext;
     * typedFillContext.ensureUnorderedFillFieldsInitialized(); final WritableLongChunk<RowKeys> indexPositions =
     * typedFillContext.indexPositions; final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions>
     * sortKernelContext = typedFillContext.sortKernelContext; final WritableIntChunk<ChunkPositions>
     * outputChunkPositions = typedFillContext.unorderedFillChunkPositions; final WritableLongChunk<RowKeys>
     * orderedMappedKeys = typedFillContext.unorderedFillMappedKeys; final int chunkSize = keysToMap.size();
     * 
     * indexPositions.copyFromTypedChunk(keysToMap, 0, 0, chunkSize); indexPositions.setSize(chunkSize);
     * outputChunkPositions.setSize(chunkSize); ChunkUtils.fillInOrder(outputChunkPositions);
     * LongIntTimsortKernel.sort(sortKernelContext, outputChunkPositions, indexPositions);
     * 
     * wrappedRowSet.getKeysForPositions(new LongChunkIterator(indexPositions), new
     * LongChunkAppender(orderedMappedKeys)); orderedMappedKeys.setSize(chunkSize);
     * 
     * mappedKeysOut.setSize(chunkSize); LongPermuteKernel.permute(orderedMappedKeys, outputChunkPositions,
     * mappedKeysOut); }
     */

    @Override
    public void startTrackingPrevValues() {
        // Deliberately left blank. Nothing to do here.
    }

    @Override
    public synchronized long remove(long outerRowKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;

        for (final RowSet.RangeIterator rangeIterator = wrappedRowSet.rangeIterator(); rangeIterator.hasNext();) {
            if (positionStart > 0) {
                builder.append(", ");
            }
            final long rangeStart = rangeIterator.currentRangeStart();
            final long length = rangeIterator.currentRangeEnd() - rangeStart + 1;
            if (length > 1) {
                builder.append(rangeIterator.currentRangeStart()).append("-").append(positionStart + length - 1)
                        .append(" -> ").append(rangeStart).append("-").append(rangeIterator.currentRangeEnd());
            } else {
                builder.append(positionStart).append(" -> ").append(rangeStart);
            }
            positionStart += length;
        }

        builder.append("}");

        return builder.toString();
    }
}
