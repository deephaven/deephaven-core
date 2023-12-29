/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.generic.page;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import org.jetbrains.annotations.NotNull;

/**
 * Append-only {@link Page} implementation that permanently wraps an array for data storage, atomically replacing "view"
 * {@link Chunk chunks} with larger ones as the page is extended.
 */
public class ChunkHolderPageChar<ATTR extends Any>
        implements Page.WithDefaults<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    private final long mask;
    private final long firstRow;
    private final char[] storage;

    private volatile CharChunk<ATTR> currentView;

    public ChunkHolderPageChar(final long mask, final long firstRow, @NotNull final char[] storage) {
        this.mask = mask;
        this.firstRow = Require.inRange(firstRow, "firstRow", mask, "mask");
        this.storage = storage;
        currentView = CharChunk.getEmptyChunk();
    }

    @Override
    public final ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    public final long firstRowOffset() {
        return firstRow;
    }

    @Override
    public final long maxRow(final long rowKey) {
        return (rowKey & ~mask()) | (firstRowOffset() + storage.length - 1);
    }

    /**
     * @return The offset into the chunk for this row key
     * @apiNote This function is for convenience over {@link #getRowOffset(long)}, so the caller doesn't have to cast to
     *          an int.
     * @implNote This page is known to be backed by a chunk, so {@code currentView.size()} is an int, and so is the
     *           offset.
     */
    private int getChunkOffset(final long rowKey) {
        return (int) getRowOffset(rowKey);
    }

    @Override
    public final long mask() {
        return mask;
    }

    /**
     * @return The current size of this page
     */
    public final int size() {
        return currentView.size();
    }

    /**
     * @param rowKey The row key to retrieve the value for
     * @return The value at {@code rowKey}
     */
    public final char get(final long rowKey) {
        return currentView.get(getChunkOffset(rowKey));
    }

    @Override
    public final Chunk<? extends ATTR> getChunk(
            @NotNull final GetContext context,
            final long firstKey,
            final long lastKey) {
        return currentView.slice(getChunkOffset(firstKey), Math.toIntExact(lastKey - firstKey + 1));
    }

    @Override
    public final void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        final WritableCharChunk<? super ATTR> to = destination.asWritableCharChunk();
        final CharChunk<ATTR> localView = currentView;

        if (rowSequence.getAverageRunLengthEstimate() >= Chunk.SYSTEM_ARRAYCOPY_THRESHOLD) {
            rowSequence.forAllRowKeyRanges((final long firstRowKey, final long lastRowKey) -> to.appendTypedChunk(
                    localView, getChunkOffset(firstRowKey), (int) (lastRowKey - firstRowKey + 1)));
        } else {
            rowSequence.forAllRowKeys((final long rowKey) -> to.add(localView.get(getChunkOffset(rowKey))));
        }
    }

    /**
     * Get a writable chunk slice of this page's data storage, starting at the end of the currently-visible range, to be
     * used for appending new data.
     *
     * @param expectedCurrentSize The expected current size of the visible data in this page, used to assert correctness
     * @return A chunk to fill with new data
     */
    public final WritableCharChunk<ATTR> getSliceForAppend(final int expectedCurrentSize) {
        Assert.eq(expectedCurrentSize, "expectedCurrentSize", size(), "current size");
        return WritableCharChunk.writableChunkWrap(storage, expectedCurrentSize, storage.length - expectedCurrentSize);
    }

    /**
     * Accept an appended slice of data to the currently-visible range for this page. Ownership of {@code slice}
     * transfers to the callee.
     *
     * @param slice The slice chunk of data, which must have been returned by {@link #getSliceForAppend(int)}; ownership
     *        transfers to the callee
     * @param expectedCurrentSize The expected current size of the visible data in this page, used to assert correctness
     */
    public final void acceptAppend(@NotNull final CharChunk<ATTR> slice, final int expectedCurrentSize) {
        Assert.eq(expectedCurrentSize, "expectedCurrentSize", size(), "current size");
        Assert.assertion(slice.isAlias(storage), "slice.isAlias(storage)");
        currentView = CharChunk.chunkWrap(storage, 0, expectedCurrentSize + slice.size());
    }
}
