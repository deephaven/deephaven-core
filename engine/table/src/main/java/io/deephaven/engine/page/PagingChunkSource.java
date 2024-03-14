//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.page;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In order to be able to cache and reuse {@link ChunkSource ChunkSources} across multiple tables (or other references),
 * {@code PagingChunkSource} adds a {@link #mask()} to {@code ChunkSource} and supports some additional
 * {@link #fillChunk} methods.
 * <p>
 * The mask is a bitmask of the lower order bits of the row keys in a {@link RowSequence}, which specifies the bits from
 * the {@link RowSequence} which will be used to uniquely specify the offsets into the ChunkSource elements on calls to
 * {@link #fillChunk} and {@link #getChunk}.
 * <p>
 * Also, a new method {@link #fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} is added, which
 * supports filling a chunk incrementally across a series of pages.
 * <p>
 * In order to support arbitrary nesting and re-use of {@link PagingChunkSource} implementations, it is required that
 * all implementations use or extend {@link io.deephaven.engine.table.impl.DefaultGetContext DefaultGetContext} and
 * {@link PagingContextHolder} as their {@link #makeGetContext(int, SharedContext) GetContext} and
 * {@link #makeFillContext(int, SharedContext) FillContext}, respectively. Nested implementations may thus store their
 * own state via the {@link PagingContextHolder#getInnerContext() inner context}, using sub-classes of
 * {@link PagingContextHolder} to support chaining of nested state.
 */
public interface PagingChunkSource<ATTR extends Any> extends DefaultChunkSource<ATTR> {

    @Override
    default FillContext makeFillContext(final int chunkCapacity, @Nullable final SharedContext sharedContext) {
        return new PagingContextHolder(chunkCapacity, sharedContext);
    }

    /**
     * This mask is applied to {@link RowSequence RowSequences} which are passed into {@link #getChunk},
     * {@link #fillChunk}, and {@link #fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} . This allows
     * {@code PagingChunkSources} to be cached and reused even if they are properly relocated in key space.
     *
     * @return The mask for this {@code PagingChunkSource}, which must be a bitmask representing some number of lower
     *         order bits of a long.
     */
    long mask();

    /**
     * <p>
     * The {@code maxRow} is the greatest possible row key which may be referenced in this ChunkSource. This method is
     * used by {@link #fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} to determine which of its row
     * keys are supplied by this {@code PagingChunkSource}.
     * </p>
     *
     * <p>
     * The default implementation assumes that only one {@code PagingChunkSource} exits for each page reference. That
     * is, there is only one {@code PagingChunkSource} for {@link RowSequence RowSequences} with the same bits outside
     * of {@link #mask()}.
     * </p>
     *
     * <p>
     * It is also possible to pack multiple, non-overlapping {@code PagingChunkSources} into the same page reference. In
     * this case, one typically will want to override {@code maxRow}. An example such implementation is
     * {@link ChunkPage}.
     *
     * @param rowKey Any row key contained by this {@link PagingChunkSource}
     * @return The maximum last row key of the page, located in the same way as {@code rowKey}
     */
    default long maxRow(final long rowKey) {
        return rowKey | mask();
    }

    /**
     * <p>
     * Similar to {@link #fillChunk(FillContext, WritableChunk, RowSequence)}, except that the values are appended to
     * {@code destination}, rather than placed at the beginning.
     * </p>
     *
     * <p>
     * The values to fill into {@code destination} are specified by {@code rowSequenceIterator}, whose
     * {@link RowSequence#firstRowKey()} must exist, and must be represented by this {@code PagingChunkSource} (modulo
     * {@link #mask()}), otherwise results are undefined.
     * </p>
     *
     * <p>
     * All values specified by {@code rowSequenceIterator} that are on the same page as its next row key will be
     * appended to {@code destination}. Row keys are on the same page when the bits outside of {@link #mask()} are
     * identical.
     *
     * @param context A context containing all mutable/state related data used in filling {@code destination}
     * @param destination The {@link WritableChunk} to append the results to
     * @param rowSequenceIterator An iterator over the remaining row keys specifying the values to retrieve, which
     *        contains at least the keys to extract from this {@code PagingChunkSource}
     */
    void fillChunkAppend(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super ATTR> destination,
            @NotNull RowSequence.Iterator rowSequenceIterator);
}
