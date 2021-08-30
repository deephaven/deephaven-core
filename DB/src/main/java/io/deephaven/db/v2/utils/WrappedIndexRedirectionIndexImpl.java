/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.util.LongChunkAppender;
import io.deephaven.db.v2.sources.chunk.util.LongChunkIterator;
import org.jetbrains.annotations.NotNull;

public class WrappedIndexRedirectionIndexImpl implements RedirectionIndex {

    /**
     * {@link Index} used to map from outer key (position in the index) to inner key.
     */
    private final Index wrappedIndex;

    public WrappedIndexRedirectionIndexImpl(final Index wrappedIndex) {
        this.wrappedIndex = wrappedIndex;
    }

    @Override
    public synchronized long put(long key, long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized long get(long key) {
        return wrappedIndex.get(key);
    }

    @Override
    public synchronized long getPrev(long key) {
        return wrappedIndex.getPrev(key);
    }

    private static final class FillContext implements RedirectionIndex.FillContext {

        private final WritableLongChunk<KeyIndices> indexPositions;

        private FillContext(final int chunkCapacity) {
            indexPositions = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            indexPositions.close();
        }
    }

    /*
     * TODO: Switch to this version if we ever uncomment the override for fillChunkUnordered.
     * private static final class FillContext implements RedirectionIndex.FillContext {
     * 
     * private final int chunkCapacity; private final WritableLongChunk<KeyIndices> indexPositions;
     * 
     * private LongIntTimsortKernel.LongIntSortKernelContext<KeyIndices, ChunkPositions>
     * sortKernelContext; private WritableIntChunk<ChunkPositions> unorderedFillChunkPositions;
     * private WritableLongChunk<KeyIndices> unorderedFillMappedKeys;
     * 
     * private FillContext(final int chunkCapacity) { this.chunkCapacity = chunkCapacity;
     * indexPositions = WritableLongChunk.makeWritableChunk(chunkCapacity); }
     * 
     * private void ensureUnorderedFillFieldsInitialized() { if (sortKernelContext == null) {
     * sortKernelContext = LongIntTimsortKernel.createContext(chunkCapacity); } if
     * (unorderedFillChunkPositions == null) { unorderedFillChunkPositions =
     * WritableIntChunk.makeWritableChunk(chunkCapacity); } if (unorderedFillMappedKeys == null) {
     * unorderedFillMappedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity); } }
     * 
     * @Override public void close() { indexPositions.close(); if (sortKernelContext != null) {
     * sortKernelContext.close(); } if (unorderedFillChunkPositions != null) {
     * unorderedFillChunkPositions.close(); } if (unorderedFillMappedKeys != null) {
     * unorderedFillMappedKeys.close(); } } }
     */

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // NB: No need to implement sharing at this level. ReadOnlyRedirectedColumnSource uses a
        // SharedContext to share
        // RedirectionIndex lookup results.
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull final RedirectionIndex.FillContext fillContext,
        @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
        @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<KeyIndices> indexPositions =
            ((FillContext) fillContext).indexPositions;
        keysToMap.fillKeyIndicesChunk(indexPositions);
        wrappedIndex.getKeysForPositions(new LongChunkIterator(indexPositions),
            new LongChunkAppender(mappedKeysOut));
        mappedKeysOut.setSize(keysToMap.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull final RedirectionIndex.FillContext fillContext,
        @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
        @NotNull final OrderedKeys keysToMap) {
        final WritableLongChunk<KeyIndices> indexPositions =
            ((FillContext) fillContext).indexPositions;
        keysToMap.fillKeyIndicesChunk(indexPositions);
        try (final ReadOnlyIndex prevWrappedIndex = wrappedIndex.getPrevIndex()) {
            prevWrappedIndex.getKeysForPositions(new LongChunkIterator(indexPositions),
                new LongChunkAppender(mappedKeysOut));
        }
        mappedKeysOut.setSize(keysToMap.intSize());
    }

    /*
     * TODO: Uncomment and test this if we ever start using WrappedIndexRedirectionIndexImpl for
     * unordered reads.
     * 
     * @Override public void fillChunkUnordered(@NotNull final RedirectionIndex.FillContext
     * fillContext,
     * 
     * @NotNull final WritableLongChunk<KeyIndices> mappedKeysOut,
     * 
     * @NotNull final LongChunk<KeyIndices> keysToMap) { final FillContext typedFillContext =
     * (FillContext) fillContext; typedFillContext.ensureUnorderedFillFieldsInitialized(); final
     * WritableLongChunk<KeyIndices> indexPositions = typedFillContext.indexPositions; final
     * LongIntTimsortKernel.LongIntSortKernelContext<KeyIndices, ChunkPositions> sortKernelContext =
     * typedFillContext.sortKernelContext; final WritableIntChunk<ChunkPositions>
     * outputChunkPositions = typedFillContext.unorderedFillChunkPositions; final
     * WritableLongChunk<KeyIndices> orderedMappedKeys = typedFillContext.unorderedFillMappedKeys;
     * final int chunkSize = keysToMap.size();
     * 
     * indexPositions.copyFromTypedChunk(keysToMap, 0, 0, chunkSize);
     * indexPositions.setSize(chunkSize); outputChunkPositions.setSize(chunkSize);
     * ChunkUtils.fillInOrder(outputChunkPositions); LongIntTimsortKernel.sort(sortKernelContext,
     * outputChunkPositions, indexPositions);
     * 
     * wrappedIndex.getKeysForPositions(new LongChunkIterator(indexPositions), new
     * LongChunkAppender(orderedMappedKeys)); orderedMappedKeys.setSize(chunkSize);
     * 
     * mappedKeysOut.setSize(chunkSize); LongPermuteKernel.permute(orderedMappedKeys,
     * outputChunkPositions, mappedKeysOut); }
     */

    @Override
    public void startTrackingPrevValues() {
        // Deliberately left blank. Nothing to do here.
    }

    @Override
    public synchronized long remove(long leftIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;

        for (final Index.RangeIterator rangeIterator = wrappedIndex.rangeIterator(); rangeIterator
            .hasNext();) {
            if (positionStart > 0) {
                builder.append(", ");
            }
            final long rangeStart = rangeIterator.currentRangeStart();
            final long length = rangeIterator.currentRangeEnd() - rangeStart + 1;
            if (length > 1) {
                builder.append(rangeIterator.currentRangeStart()).append("-")
                    .append(positionStart + length - 1).append(" -> ").append(rangeStart)
                    .append("-").append(rangeIterator.currentRangeEnd());
            } else {
                builder.append(positionStart).append(" -> ").append(rangeStart);
            }
            positionStart += length;
        }

        builder.append("}");

        return builder.toString();
    }
}
