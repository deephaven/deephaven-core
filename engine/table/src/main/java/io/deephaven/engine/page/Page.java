package io.deephaven.engine.page;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * This provides the {@link ChunkSource} interface to a contiguous block of data beginning at {@link #firstRowOffset()}
 * and continuing to some row less than or equal to {@link #firstRowOffset()} + {@link #maxRow(long)}.
 * <p>
 * Non overlapping pages can be collected together in a {@link PageStore}, which provides the {@link ChunkSource}
 * interface to the collection of all of its Pages.
 * <p>
 * There are two distinct use cases/types of pages. The first use case are {@code Page}s which always have a length() >
 * 0. These store length() values, which can be assessed via the {@link ChunkSource} methods. Valid {@link RowSequence}
 * passed to those methods will have their offset in the range [firstRowOffset(), firstRowOffset() + length()). Passing
 * OrderKeys with offsets outside of this range will have undefined results.
 * <p>
 * The second use case will always have length() == 0 and firstRowOffset() == 0. These represent "Null" regions which
 * return a fixed value, typically a null value, for every {@link RowSequence} passed into the {@link ChunkSource}
 * methods. In order to have this use case, override {@code length} and override {@code lastRow} as {@code maxRow}.
 * <p>
 * Though the {@link ChunkSource} methods ignore the non-offset portion of the rows in the {@link RowSequence}, they can
 * assume they are identical for all the passed in elements of the {@link RowSequence}. For instance, they can use the
 * simple difference between the complete row value to determine a length.
 */
public interface Page<ATTR extends Any> extends PagingChunkSource<ATTR> {

    /**
     * @return the first row of this page, after applying the {@link #mask()}, which refers to the first row of this
     *         page.
     */
    long firstRowOffset();

    /**
     * @param row Any row contained on this page.
     * @return the first row of this page, located in the same way as row.
     */
    @FinalDefault
    default long firstRow(final long row) {
        return (row & ~mask()) | firstRowOffset();
    }

    /**
     * @return the offset for the given row in this page, in [0, {@code maxRow(row)}].
     */
    @FinalDefault
    default long getRowOffset(long row) {
        return (row & mask()) - firstRowOffset();
    }

    /**
     * Helper defaults for general pages.
     */
    interface WithDefaults<ATTR extends Any> extends Page<ATTR>, DefaultChunkSource<ATTR> {

        @Override
        @FinalDefault
        default void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination,
                @NotNull final RowSequence.Iterator RowSequenceIterator) {
            fillChunkAppend(context, destination,
                    RowSequenceIterator.getNextRowSequenceThrough(maxRow(RowSequenceIterator.peekNextKey())));
        }

        @Override
        @FinalDefault
        default void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, @NotNull final RowSequence rowSequence) {
            destination.setSize(0);
            fillChunkAppend(context, destination, rowSequence);
        }

        /**
         * Appends the values referenced by {@code orderKeys} onto {@code destination}. {@code orderKeys} are assumed to
         * be entirely contained on this {@code Page}.
         */
        void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
                @NotNull RowSequence rowSequence);
    }

    /**
     * Helper defaults for pages that represent a repeating value, e.g. null or partitioning column regions.
     */
    interface WithDefaultsForRepeatingValues<ATTR extends Any> extends Page<ATTR>, DefaultChunkSource<ATTR> {

        @Override
        @FinalDefault
        default void fillChunkAppend(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination,
                @NotNull final RowSequence.Iterator RowSequenceIterator) {
            fillChunkAppend(context, destination, LongSizedDataStructure.intSize("fillChunkAppend",
                    RowSequenceIterator.advanceAndGetPositionDistance(maxRow(RowSequenceIterator.peekNextKey()) + 1)));
        }

        @Override
        @FinalDefault
        default void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination, @NotNull final RowSequence rowSequence) {
            destination.setSize(0);
            fillChunkAppend(context, destination, rowSequence.intSize());
        }

        /**
         * Appends the values repeating value {@code length} times to {@code destination}.
         */
        void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
                int length);
    }

    /**
     * Assuming {@code RowSequenceIterator} is position at its first row key on this page, consume all keys on this
     * page.
     *
     * @param RowSequenceIterator The iterator to advance
     */
    @FinalDefault
    default void advanceToNextPage(@NotNull final RowSequence.Iterator RowSequenceIterator) {
        RowSequenceIterator.advance(maxRow(RowSequenceIterator.peekNextKey()) + 1);
    }

    /**
     * Assuming {@code RowSequenceIterator} is position at its first row key on this page, consume all keys on this page
     * and return the number of keys consumed.
     *
     * @param RowSequenceIterator The iterator to advance
     */
    @FinalDefault
    default long advanceToNextPageAndGetPositionDistance(@NotNull final RowSequence.Iterator RowSequenceIterator) {
        return RowSequenceIterator.advanceAndGetPositionDistance(maxRow(RowSequenceIterator.peekNextKey()) + 1);
    }

    /**
     * Assuming {@code searchIterator} is position at its first row key on this page, consume all keys on this page.
     *
     * @param searchIterator The iterator to advance
     * @return The result of {@link RowSet.SearchIterator#advance(long)}
     */
    @FinalDefault
    default boolean advanceToNextPage(@NotNull final RowSet.SearchIterator searchIterator) {
        return searchIterator.advance(maxRow(searchIterator.currentValue()) + 1);
    }
}
