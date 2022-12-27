/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset;

import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.SafeCloseable;

/**
 * An ordered collection of {@code long} row keys.
 */
public interface RowSequence extends SafeCloseable, LongSizedDataStructure {

    /**
     * The null row key.
     */
    long NULL_ROW_KEY = -1L;

    /**
     * Get an {@link Iterator} over this {@code RowSequence}.
     *
     * @return A new iterator, positioned at the first row key
     */
    Iterator getRowSequenceIterator();

    /**
     * <p>
     * Get an ordered subset of the row keys in this {@code RowSequence} for a position range. The result will contain
     * the set of row keys in {@code this} that lie at positions in the half-open range [{@code startPositionInclusive},
     * {@code startPositionInclusive + length}).
     *
     * The returned reference is owned by the caller, who should call {@code close()} when it is done with it.
     *
     * @param startPositionInclusive The position of the first row key to include
     * @param length The number of row keys to include
     * @return The subset as an {@code RowSequence}, which may be {@code this}
     */
    RowSequence getRowSequenceByPosition(long startPositionInclusive, long length);

    /**
     * <p>
     * Get an ordered subset of the row keys in this {@code RowSequence} for a row key range. The returned set will be
     * the intersection of the row keys in {@code this} with the row keys in the closed interval
     * [{@code startRowKeyInclusive}, {@code endRowKeyInclusive}].
     *
     * The returned reference is owned by the caller, who should call {@code close()} when it is done with it.
     *
     * @param startRowKeyInclusive The minimum row key to include
     * @param endRowKeyInclusive The maximum row key to include
     * @return The subset as an {@code RowSequence}, which may be {@code this}
     */
    RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive);

    /**
     * Get a {@link RowSet} representation of this {@code RowSequence}.
     *
     * @return A {@link RowSet} representation for the same row keys in the same order
     * @apiNote If you use the result across clock ticks, you may observe inconsistencies.
     * @apiNote You must not mutate the result.
     * @apiNote You must {@link SafeCloseable#close() close} the result.
     */
    RowSet asRowSet();

    /**
     * Get a {@link LongChunk} representation of the individual row keys in this {@code RowSequence}.
     *
     * @return A {@link LongChunk} containing the row keys in this {@code RowSequence}
     * @apiNote This {@code RowSequence} owns the result, which is valid only as long as this {@code RowSequence}
     *          remains valid.
     * @apiNote You must not mutate the result.
     */
    LongChunk<OrderedRowKeys> asRowKeyChunk();

    /**
     * Get a {@link LongChunk} representation of row key ranges in this {@code RowSequence}.
     *
     * @return A {@link LongChunk} containing the row key ranges in this {@code RowSequence}
     * @apiNote This {@code RowSequence} owns the result, which is valid only as long as this {@code RowSequence}
     *          remains valid.
     * @apiNote You must not mutate the result.
     */
    LongChunk<OrderedRowKeyRanges> asRowKeyRangesChunk();

    /**
     * <p>
     * Fill the supplied {@link WritableLongChunk} with individual row keys from this {@code RowSequence}.
     * <p>
     * The chunk's capacity is assumed to be big enough.
     *
     * @param chunkToFill A chunk to fill with individual row keys
     */
    void fillRowKeyChunk(WritableLongChunk<? super OrderedRowKeys> chunkToFill);

    /**
     * <p>
     * Fill the supplied {@link WritableLongChunk} with row key ranges from this {@code RowSequence}.
     * <p>
     * The chunk's capacity is assumed to be big enough.
     *
     * @param chunkToFill A chunk to fill with row key ranges
     */
    void fillRowKeyRangesChunk(WritableLongChunk<OrderedRowKeyRanges> chunkToFill);

    /**
     * True if the size of this {@code RowSequence} is zero.
     *
     * @return True if there are no elements in this {@code RowSequence}.
     */
    boolean isEmpty();

    /**
     * Get the first row key in this {@code RowSequence}.
     *
     * @return The first row key, or {@link TrackingWritableRowSet#NULL_ROW_KEY} if there is none.
     */
    long firstRowKey();

    /**
     * Get the last row key in this {@code RowSequence}.
     *
     * @return The last row key, or {@link TrackingWritableRowSet#NULL_ROW_KEY} if there is none.
     */
    long lastRowKey();

    /**
     * Get the number of row keys in this {@code RowSequence}.
     *
     * @return The size, in [0, {@link Long#MAX_VALUE}]
     */
    long size();

    /**
     * Helper to tell you if this is one contiguous range.
     */
    default boolean isContiguous() {
        return isEmpty() || lastRowKey() - firstRowKey() == size() - 1;
    }

    /**
     * <p>
     * Get an estimate of the average (mean) length of runs of adjacent row keys in this {@code RowSequence}.
     * <p>
     * Implementations should strive to keep this method efficient (<i>O(1)</i> preferred) at the expense of accuracy.
     * <p>
     * Empty {@code RowSequence} should return an arbitrary valid value, usually 1.
     *
     * @return An estimate of the average run length in this {@code RowSequence}, in [1, {@code size()}]
     */
    long getAverageRunLengthEstimate();

    /**
     * For as long as the consumer wants more row keys, call accept on the consumer with the individual row key
     * instances in this RowSequence, in increasing order.
     *
     * @param lac a consumer to feed the individual row key values to.
     * @return false if the consumer provided ever returned false, true otherwise.
     */
    boolean forEachRowKey(LongAbortableConsumer lac);

    /**
     * For as long as the consumer wants more ranges, call accept on the consumer with the individual row key ranges in
     * this RowSequence, in increasing order.
     *
     * @param larc a consumer to feed the individual row key values to.
     * @return false if the consumer provided ever returned false, true otherwise.
     */
    boolean forEachRowKeyRange(LongRangeAbortableConsumer larc);

    default void forAllRowKeys(java.util.function.LongConsumer lc) {
        forEachRowKey((final long v) -> {
            lc.accept(v);
            return true;
        });
    }

    default void forAllRowKeyRanges(LongRangeConsumer lrc) {
        forEachRowKeyRange((final long first, final long last) -> {
            lrc.accept(first, last);
            return true;
        });
    }

    /**
     * <p>
     * Free any resources associated with this object.
     * <p>
     * Using any {@code RowSequence} methods after {@code close()} is an error and may produce exceptions or undefined
     * results.
     */
    default void close() {}

    /**
     * Iterator for consuming an {@code RowSequence} by ordered subsets.
     */
    interface Iterator extends SafeCloseable {

        /**
         * Poll whether there are more row keys available from this {@link Iterator}.
         *
         * @return True if there are more row keys available, else false
         */
        boolean hasMore();

        /**
         * Peek at the next row key that would be returned by {@link #getNextRowSequenceThrough(long)} or
         * {@link #getNextRowSequenceWithLength(long)}. Does not advance the position.
         *
         * @return The next row key that would be returned, or {@link TrackingWritableRowSet#NULL_ROW_KEY} if this
         *         iterator is exhausted
         */
        long peekNextKey();

        /**
         * Get a {@code RowSequence} from the row key at the position of this iterator up to the maximum row key
         * (inclusive). Advances the position of this iterator by the size of the result. If the maximum row key
         * provided is smaller than the next row key (as would be returned by {@link #peekNextKey()}), the empty
         * RowSequence is returned.
         *
         * The returned RowSequence object is only borrowed by the caller from the {@link Iterator}, who owns it. It is
         * guaranteed to be valid and not change only until a later call to another {@code getNext*} method. As the
         * returned reference is owned by the {@link Iterator}, the caller <i>should not</i> call {@code close()} on it.
         *
         * @param maxKeyInclusive The maximum row key to include.
         * @return An {@code RowSequence} from the row key at the initial position up to the maximum row key
         *         (inclusive).
         */
        RowSequence getNextRowSequenceThrough(long maxKeyInclusive);

        /**
         * Get a {@code RowSequence} from the row key at the position of this iterator up to the desired number of row
         * keys. Advances the position of this iterator by the size of the result.
         *
         * The returned RowSequence object is only borrowed by the caller from the {@link Iterator}, who owns it. It is
         * guaranteed to be valid and not change only until the next call to another {@code getNext*} method. As the
         * returned reference is owned by the {@link Iterator}, the caller <i>should not</i> call {@code close()} on it.
         *
         * @param numberOfKeys The desired number of row keys
         * @return An {@code RowSequence} from the row key at the initial position up to the desired number of row keys
         */
        RowSequence getNextRowSequenceWithLength(long numberOfKeys);

        /**
         * <p>
         * Advance this iterator's position to {@code nextKey}, or to the first present row key greater than
         * {@code nextKey} if {@code nextKey} is not found. If {@code nextKey} is less than or equal to the row key at
         * this iterator's current position, this method is a no-op.
         * <p>
         * Subsequent calls to {@link #peekNextKey()}, {@link #getNextRowSequenceThrough(long)}, or
         * {@link #getNextRowSequenceWithLength(long)} will begin with the row key advanced to.
         *
         * @param nextKey The row key to advance to
         * @return true If there are any row keys remaining to be iterated after the advance, false if this
         *         {@link Iterator} is exhausted
         */
        boolean advance(long nextKey);

        /**
         * Advance this iterator's position as in {@link #advance(long)}, returning the number of row keys thus
         * consumed.
         *
         * @param nextKey The row key to advance to
         * @return The number of row keys consumed from the iterator
         */
        default long advanceAndGetPositionDistance(final long nextKey) {
            final long initialRelativePosition = getRelativePosition();
            advance(nextKey);
            return getRelativePosition() - initialRelativePosition;
        }

        /**
         * <p>
         * Free any resources associated with this iterator.
         * <p>
         * Callers of {@link RowSequence#getRowSequenceIterator()} are responsible for ensuring that {@code close()} is
         * called when they are done with resulting {@link Iterator}.
         * <p>
         * Using any {@link Iterator} methods after {@code close()} is an error and may produce exceptions or undefined
         * results.
         */
        default void close() {}

        /**
         * Taking the difference between values returned by this method at different positions in the iterator gives you
         * the cardinality of the set of row keys between them, exclusive. Note a single value itself is not meaningful;
         * like measuring elapsed time, it only makes sense to take the difference from absolute points.
         *
         * @return A relative position offset from some arbitrary initial point in the underlying row sequence.
         */
        long getRelativePosition();
    }
}
