//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
 * Pages are {@link PagingChunkSource PagingChunkSources} that can supply values from a subset of a contiguous block of
 * row key space beginning at {@link #firstRowOffset()} and continuing to {@link #firstRowOffset()} +
 * {@link #maxRow(long)}. Not all row keys within the range may be valid; that is, pages may be sparse.
 * <p>
 * Pages may be held within one or more {@link PageStore} instances. The PageStore is responsible for determining which
 * row keys in absolute space are mapped to a particular Page. Pages need only concern themselves with lower order bits
 * of the row keys they are asked for, after applying their {@link #mask()}.
 */
public interface Page<ATTR extends Any> extends PagingChunkSource<ATTR> {

    /**
     * @return the first row of this page, after applying the {@link #mask()}, which refers to the first row of this
     *         page.
     */
    long firstRowOffset();

    /**
     * @param rowKey Any row key contained on this page
     * @return The first row key of this page, located in the same way as {@code rowKey}
     */
    @FinalDefault
    default long firstRow(final long rowKey) {
        return (rowKey & ~mask()) | firstRowOffset();
    }

    /**
     * @return The offset for the given row key in this page, in [0, {@code maxRow(rowKey)}].
     */
    @FinalDefault
    default long getRowOffset(final long rowKey) {
        return (rowKey & mask()) - firstRowOffset();
    }

    /**
     * Helper defaults for general pages.
     */
    interface WithDefaults<ATTR extends Any> extends Page<ATTR>, DefaultChunkSource<ATTR> {

        @Override
        @FinalDefault
        default void fillChunkAppend(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination,
                @NotNull final RowSequence.Iterator rowSequenceIterator) {
            fillChunkAppend(context, destination,
                    rowSequenceIterator.getNextRowSequenceThrough(maxRow(rowSequenceIterator.peekNextKey())));
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
        default void fillChunkAppend(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super ATTR> destination,
                @NotNull final RowSequence.Iterator rowSequenceIterator) {
            fillChunkAppend(context, destination, LongSizedDataStructure.intSize("fillChunkAppend",
                    rowSequenceIterator.advanceAndGetPositionDistance(maxRow(rowSequenceIterator.peekNextKey()) + 1)));
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
     * Assuming {@code rowSequenceIterator} is position at its first row key on this page, consume all keys on this
     * page.
     *
     * @param rowSequenceIterator The iterator to advance
     */
    @FinalDefault
    default void advanceToNextPage(@NotNull final RowSequence.Iterator rowSequenceIterator) {
        rowSequenceIterator.advance(maxRow(rowSequenceIterator.peekNextKey()) + 1);
    }

    /**
     * Assuming {@code rowSequenceIterator} is position at its first row key on this page, consume all keys on this page
     * and return the number of keys consumed.
     *
     * @param rowSequenceIterator The iterator to advance
     */
    @FinalDefault
    default long advanceToNextPageAndGetPositionDistance(@NotNull final RowSequence.Iterator rowSequenceIterator) {
        return rowSequenceIterator.advanceAndGetPositionDistance(maxRow(rowSequenceIterator.peekNextKey()) + 1);
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
