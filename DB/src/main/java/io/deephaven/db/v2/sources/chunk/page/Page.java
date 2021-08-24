package io.deephaven.db.v2.sources.chunk.page;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.DefaultChunkSource;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * This provides the {@link ChunkSource} interface to a contiguous block of data beginning at
 * {@link #firstRowOffset()} and continuing to some row less than or equal to
 * {@link #firstRowOffset()} + {@link #maxRow(long)}.
 * <p>
 * Non overlapping pages can be collected together in a {@link PageStore}, which provides the
 * {@link ChunkSource} interface to the collection of all of its Pages.
 * <p>
 * There are two distinct use cases/types of pages. The first use case are {@code Page}s which
 * always have a length() > 0. These store length() values, which can be assessed via the
 * {@link ChunkSource} methods. Valid {@link OrderedKeys} passed to those methods will have their
 * offset in the range [firstRowOffset(), firstRowOffset() + length()). Passing OrderKeys with
 * offsets outside of this range will have undefined results.
 * <p>
 * The second use case will always have length() == 0 and firstRowOffset() == 0. These represent
 * "Null" regions which return a fixed value, typically a null value, for every {@link OrderedKeys}
 * passed into the {@link ChunkSource} methods. In order to have this use case, override
 * {@code length} and override {@code lastRow} as {@code maxRow}.
 * <p>
 * Though the {@link ChunkSource} methods ignore the non-offset portion of the rows in the
 * {@link OrderedKeys}, they can assume they are identical for all the passed in elements of the
 * {@link OrderedKeys}. For instance, they can use the simple difference between the complete row
 * value to determine a length.
 */
public interface Page<ATTR extends Any> extends PagingChunkSource<ATTR> {

    /**
     * @return the first row of this page, after applying the {@link #mask()}, which refers to the
     *         first row of this page.
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
            @NotNull final OrderedKeys.Iterator orderedKeysIterator) {
            fillChunkAppend(context, destination, orderedKeysIterator
                .getNextOrderedKeysThrough(maxRow(orderedKeysIterator.peekNextKey())));
        }

        @Override
        @FinalDefault
        default void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final OrderedKeys orderedKeys) {
            destination.setSize(0);
            fillChunkAppend(context, destination, orderedKeys);
        }

        /**
         * Appends the values referenced by {@code orderKeys} onto {@code destination}.
         * {@code orderKeys} are assumed to be entirely contained on this {@code Page}.
         */
        void fillChunkAppend(@NotNull FillContext context,
            @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys);
    }

    /**
     * Helper defaults for pages that represent a repeating value, e.g. null or partitioning column
     * regions.
     */
    interface WithDefaultsForRepeatingValues<ATTR extends Any>
        extends Page<ATTR>, DefaultChunkSource<ATTR> {

        @Override
        @FinalDefault
        default void fillChunkAppend(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final OrderedKeys.Iterator orderedKeysIterator) {
            fillChunkAppend(context, destination, LongSizedDataStructure.intSize("fillChunkAppend",
                orderedKeysIterator
                    .advanceAndGetPositionDistance(maxRow(orderedKeysIterator.peekNextKey()) + 1)));
        }

        @Override
        @FinalDefault
        default void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final OrderedKeys orderedKeys) {
            destination.setSize(0);
            fillChunkAppend(context, destination, orderedKeys.intSize());
        }

        /**
         * Appends the values repeating value {@code length} times to {@code destination}.
         */
        void fillChunkAppend(@NotNull FillContext context,
            @NotNull WritableChunk<? super ATTR> destination, int length);
    }

    /**
     * Assuming {@code orderedKeysIterator} is position at its first index key on this page, consume
     * all keys on this page.
     *
     * @param orderedKeysIterator The iterator to advance
     */
    @FinalDefault
    default void advanceToNextPage(@NotNull final OrderedKeys.Iterator orderedKeysIterator) {
        orderedKeysIterator.advance(maxRow(orderedKeysIterator.peekNextKey()) + 1);
    }

    /**
     * Assuming {@code orderedKeysIterator} is position at its first index key on this page, consume
     * all keys on this page and return the number of keys consumed.
     *
     * @param orderedKeysIterator The iterator to advance
     */
    @FinalDefault
    default long advanceToNextPageAndGetPositionDistance(
        @NotNull final OrderedKeys.Iterator orderedKeysIterator) {
        return orderedKeysIterator
            .advanceAndGetPositionDistance(maxRow(orderedKeysIterator.peekNextKey()) + 1);
    }

    /**
     * Assuming {@code searchIterator} is position at its first index key on this page, consume all
     * keys on this page.
     *
     * @param searchIterator The iterator to advance
     * @return The result of
     *         {@link io.deephaven.db.v2.utils.ReadOnlyIndex.SearchIterator#advance(long)}
     */
    @FinalDefault
    default boolean advanceToNextPage(@NotNull final ReadOnlyIndex.SearchIterator searchIterator) {
        return searchIterator.advance(maxRow(searchIterator.currentValue()) + 1);
    }
}
