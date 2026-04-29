//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

public class WrappedRowSetRowRedirection extends StaticWrappedRowSetRowRedirection {
    final TrackingRowSet wrappedRowSet;

    public WrappedRowSetRowRedirection(final TrackingRowSet wrappedRowSet) {
        super(wrappedRowSet);
        this.wrappedRowSet = wrappedRowSet;
    }

    @Override
    public synchronized long getPrev(long outerRowKey) {
        return wrappedRowSet.getPrev(outerRowKey);
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
    public void fillPrevChunk(@NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> innerRowKeys,
            @NotNull final RowSequence outerRowKeys) {
        final WritableLongChunk<OrderedRowKeys> rowPositions = ((FillContext) fillContext).rowPositions;
        final WritableLongChunk<? super RowKeys> innerRowKeysTyped = innerRowKeys.asWritableLongChunk();
        outerRowKeys.fillRowKeyChunk(rowPositions);
        wrappedRowSet.prev().getKeysForPositions(new LongChunkIterator(rowPositions),
                new LongChunkAppender(innerRowKeysTyped));
        innerRowKeysTyped.setSize(outerRowKeys.intSize());
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
}
