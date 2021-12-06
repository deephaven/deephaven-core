package io.deephaven.engine.page;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

/**
 * In order to be able to cache and reuse ChunkSources across multiple Tables (or other references),
 * {@code PagingChunkSource} adds a mask to the ChunkSource, and supports some additional {@code fillChunk} methods.
 *
 * The mask is a bitmask of the lower order bits of the keys in an OrderKeys, which specifies the bits from the
 * {@link RowSequence} which will be used to uniquely specify the offsets into the ChunkSource elements on calls to
 * {@link ChunkSource#fillChunk(FillContext, WritableChunk, RowSequence)},
 * {@link ChunkSource#getChunk(GetContext, RowSequence)}, {@link ChunkSource#getChunk(GetContext, long, long)}.
 *
 * Also, a new method {@link PagingChunkSource#fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} is
 * added, which supports doing a fillChunk incrementally across a series of pages.
 */
public interface PagingChunkSource<ATTR extends Any> extends ChunkSource<ATTR> {

    /**
     * This mask is applied to {@link RowSequence} which are passed into
     * {@link #getChunk(ChunkSource.GetContext, RowSequence)} and
     * {@link #fillChunk(ChunkSource.FillContext, WritableChunk, RowSequence)}. This allows the {@link PagingChunkSource
     * PagingChunkSources} to be cached, and reused even if they are properly relocated in key space.
     *
     * @return the mask for this page, which must be a bitmask representing the some number of lower order bits of a
     *         long.
     */
    long mask();

    /**
     * <p>
     * The {@code maxRow} is the greatest possible row which may reference this ChunkSource. This method is used by
     * {@link #fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} to determine which of its
     * {@code RowSequence} are referencing this {@code PagingChunkSource}.
     * </p>
     *
     * <p>
     * The default implementation assumes that only one {@code PagingChunkSource} exits for each page reference. That
     * is, there is only one {@code PagingChunkSource} for {@code OrderedKey}s with the same bits outside of
     * {@link #mask()}.
     * </p>
     *
     * <p>
     * It is also possible to pack multiple, non-overlapping {@code PagingChunkSources} into the same page reference. In
     * this case, one typically will want to override {@code maxRow}. An example such implementation is
     * {@link ChunkPage}.
     *
     * @param row Any row contained on this page.
     * @return the maximum last row of this page, located in the same way as row.
     */
    default long maxRow(final long row) {
        return row | mask();
    }

    /**
     * <p>
     * Similar to {@link #fillChunk(FillContext, WritableChunk, RowSequence)}, except that the values from the
     * ChunkSource are appended to {@code destination}, rather than placed at the beginning.
     * </p>
     *
     * <p>
     * The values to fill into {@code destination} are specified by {@code RowSequenceIterator}, whose
     * {@link RowSequence.Iterator#firstRowKey()} must exist, and must be represented by this {@code PagingChunkSource}
     * (modulo {#link @mask}), otherwise results are undefined.
     * </p>
     *
     * <p>
     * No more than the elements in {@code RowSequenceIterator}, which are on the same page as
     * {@link RowSequence.Iterator#firstRowKey()}, have their values appended to {@code destination}, and consumed from
     * {@code RowSequenceIterator}. Indices are on the same page when the bits outside of {@link #mask()} are identical.
     *
     * @param context A context containing all mutable/state related data used in retrieving the Chunk. In particular,
     *        the Context may be used to provide a Chunk data pool
     * @param destination The chunk to append the results to.
     * @param RowSequenceIterator The iterator to the ordered keys, which contain at least the keys to extract from this
     *        {@code ChunkSource}. The keys to extract will be at the beginning of iteration order.
     */
    void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
            @NotNull RowSequence.Iterator RowSequenceIterator);
}
