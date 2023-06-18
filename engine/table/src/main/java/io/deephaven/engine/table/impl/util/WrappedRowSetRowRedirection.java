/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
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

public class WrappedRowSetRowRedirection implements RowRedirection {

    /**
     * {@link TrackingRowSet} used to map from outer key (position in the RowSet) to inner key.
     */
    private final TrackingRowSet wrappedRowSet;

    public WrappedRowSetRowRedirection(final TrackingRowSet wrappedRowSet) {
        this.wrappedRowSet = wrappedRowSet;
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

        private final WritableLongChunk<OrderedRowKeys> rowPositions;

        private FillContext(final int chunkCapacity) {
            rowPositions = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            rowPositions.close();
        }
    }

    /* @formatter:off
     * TODO: Switch to this version if we ever uncomment the override for fillChunkUnordered.
     * private static final class FillContextForUnordered implements ChunkSource.FillContext {
     *
     *     private final int chunkCapacity;
     *     private final WritableLongChunk<RowKeys> rowPositions;
     *
     *     private LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext;
     *     private WritableIntChunk<ChunkPositions> unorderedFillChunkPositions;
     *     private WritableLongChunk<RowKeys> unorderedFillMappedKeys;
     *
     *     private FillContextForUnordered(final int chunkCapacity) {
     *         this.chunkCapacity = chunkCapacity;
     *         rowPositions = WritableLongChunk.makeWritableChunk(chunkCapacity);
     *     }
     *
     *     private void ensureUnorderedFillFieldsInitialized() {
     *         if (sortKernelContext == null) {
     *             sortKernelContext =
     *                     LongIntTimsortKernel.createContext(chunkCapacity);
     *         }
     *         if (unorderedFillChunkPositions == null) {
     *             unorderedFillChunkPositions = WritableIntChunk.makeWritableChunk(chunkCapacity);
     *         }
     *         if (unorderedFillMappedKeys == null) {
     *             unorderedFillMappedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
     *         }
     *     }
     *
     *     @Override
     *     public void close() {
     *         rowPositions.close();
     *         if (sortKernelContext != null) {
     *             sortKernelContext.close();
     *         }
     *         if (unorderedFillChunkPositions != null) {
     *             unorderedFillChunkPositions.close();
     *         }
     *         if (unorderedFillMappedKeys != null) {
     *             unorderedFillMappedKeys.close();
     *         }
     *     }
     * }
     * @formatter:on
     */

    @Override
    public ChunkSource.FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        // NB: No need to implement sharing at this level. RedirectedColumnSource uses a SharedContext to share
        // WritableRowRedirection lookup results.
        return new FillContext(chunkCapacity);
    }

    @Override
    public void fillChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<OrderedRowKeys> rowPositions = ((FillContext) fillContext).rowPositions;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        outerRowKeys.fillRowKeyChunk(rowPositions);
        wrappedRowSet.getKeysForPositions(
                new LongChunkIterator(rowPositions), new LongChunkAppender(innerRowKeysTyped));
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
    }

    @Override
    public void fillPrevChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<OrderedRowKeys> rowPositions = ((FillContext) fillContext).rowPositions;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        outerRowKeys.fillRowKeyChunk(rowPositions);
        try (final RowSet prevWrappedRowSet = wrappedRowSet.copyPrev()) {
            prevWrappedRowSet.getKeysForPositions(
                    new LongChunkIterator(rowPositions), new LongChunkAppender(innerRowKeysTyped));
        }
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
    }

    @Override
    public boolean ascendingMapping() {
        return true;
    }

    /* @formatter:off
     * TODO: Uncomment and test this if we ever start using WrappedRowSetRowRedirection for unordered reads.
     * @Override
     * public void fillChunkUnordered(
     *         @NotNull final ChunkSource.FillContext fillContext,
     *         @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
     *         @NotNull final LongChunk<? extends RowKeys> keysToMap) {
     *     final FillContextForUnordered typedFillContext = (FillContextForUnordered) fillContext;
     *     final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
     *     typedFillContext.ensureUnorderedFillFieldsInitialized();
     *     final WritableLongChunk<RowKeys> rowPositions = typedFillContext.rowPositions;
     *     final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext =
     *             typedFillContext.sortKernelContext;
     *     final WritableIntChunk<ChunkPositions> outputChunkPositions = typedFillContext.unorderedFillChunkPositions;
     *     final WritableLongChunk<RowKeys> orderedMappedKeys = typedFillContext.unorderedFillMappedKeys;
     *     final int chunkSize = keysToMap.size();
     *
     *     rowPositions.copyFromTypedChunk(keysToMap, 0, 0, chunkSize);
     *     rowPositions.setSize(chunkSize);
     *     outputChunkPositions.setSize(chunkSize);
     *     ChunkUtils.fillInOrder(outputChunkPositions);
     *     LongIntTimsortKernel.sort(sortKernelContext, outputChunkPositions, rowPositions);
     *
     *     wrappedRowSet.getKeysForPositions(
     *             new LongChunkIterator(rowPositions), new LongChunkAppender(orderedMappedKeys));
     *     orderedMappedKeys.setSize(chunkSize);
     *
     *     mappedKeysOutTyped.setSize(chunkSize);
     *     LongPermuteKernel.permute(orderedMappedKeys, outputChunkPositions, mappedKeysOutTyped);
     * }
     *
     * @Override
     * public void fillPrevChunkUnordered(
     *         @NotNull final ChunkSource.FillContext fillContext,
     *         @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
     *         @NotNull final LongChunk<? extends RowKeys> keysToMap) {
     *     final FillContextForUnordered typedFillContext = (FillContextForUnordered) fillContext;
     *     final WritableLongChunk<? super RowKeys> mappedKeysOutTyped = mappedKeysOut.asWritableLongChunk();
     *     typedFillContext.ensureUnorderedFillFieldsInitialized();
     *     final WritableLongChunk<RowKeys> rowPositions = typedFillContext.rowPositions;
     *     final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext =
     *             typedFillContext.sortKernelContext;
     *     final WritableIntChunk<ChunkPositions> outputChunkPositions = typedFillContext.unorderedFillChunkPositions;
     *     final WritableLongChunk<RowKeys> orderedMappedKeys = typedFillContext.unorderedFillMappedKeys;
     *     final int chunkSize = keysToMap.size();
     *
     *     rowPositions.copyFromTypedChunk(keysToMap, 0, 0, chunkSize);
     *     rowPositions.setSize(chunkSize);
     *     outputChunkPositions.setSize(chunkSize);
     *     ChunkUtils.fillInOrder(outputChunkPositions);
     *     LongIntTimsortKernel.sort(sortKernelContext, outputChunkPositions, rowPositions);
     *
     *     try (final RowSet prevWrappedRowSet = wrappedRowSet.copyPrev()) {
     *         prevWrappedRowSet.getKeysForPositions(
     *                 new LongChunkIterator(rowPositions), new LongChunkAppender(orderedMappedKeys));
     *     }
     *     orderedMappedKeys.setSize(chunkSize);
     *
     *     mappedKeysOutTyped.setSize(chunkSize);
     *     LongPermuteKernel.permute(orderedMappedKeys, outputChunkPositions, mappedKeysOutTyped);
     * }
     * @formatter:on
     */

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("{");

        long positionStart = 0;


        for (final RowSet.RangeIterator rangeIterator = wrappedRowSet.rangeIterator(); rangeIterator.hasNext();) {
            rangeIterator.next();

            if (positionStart > 0) {
                builder.append(", ");
            }
            final long rangeStart = rangeIterator.currentRangeStart();
            final long length = rangeIterator.currentRangeEnd() - rangeStart + 1;
            if (length > 1) {
                builder.append(positionStart).append("-").append(positionStart + length - 1)
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
