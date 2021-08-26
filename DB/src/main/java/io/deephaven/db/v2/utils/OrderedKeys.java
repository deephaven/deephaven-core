/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.util.SafeCloseable;

import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;

/**
 * An ordered collection of long keys.
 */
public interface OrderedKeys extends SafeCloseable, LongSizedDataStructure {

    /**
     * Wrap a LongChunk as an OrderedKeys.
     * 
     * @param longChunk A chunk to wrap as a new OrderedKeys object.
     * @return A new OrderedKeys object, who does not own the passed chunk.
     */
    static OrderedKeys wrapKeyIndicesChunkAsOrderedKeys(final LongChunk<OrderedKeyIndices> longChunk) {
        return OrderedKeysKeyIndicesChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Wrap a LongChunk as an OrderedKeys.
     * 
     * @param longChunk A chunk to wrap as a new OrderedKeys object.
     * @return A new OrderedKeys object, who does not own the passed chunk.
     */
    static OrderedKeys wrapKeyRangesChunkAsOrderedKeys(final LongChunk<OrderedKeyRanges> longChunk) {
        return OrderedKeysKeyRangesChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Create and return a new OrderedKeys object from the provided WritableLongChunk.
     * 
     * @param longChunk The input chunk. The returned object will take ownership of this chunk.
     * @return A new OrderedKeys object, who owns the passed chunk.
     */
    static OrderedKeys takeKeyIndicesChunkAndMakeOrderedKeys(final WritableLongChunk<OrderedKeyIndices> longChunk) {
        return OrderedKeysKeyIndicesChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Create and return a new OrderedKeys object from the provided WritableLongChunk.
     * 
     * @param longChunk The input chunk. The returned object will take ownership of this chunk.
     * @return A new OrderedKeys object, who owns the passed chunk.
     */
    static OrderedKeys takeKeyRangesChunkAndMakeOrderedKeys(final WritableLongChunk<OrderedKeyRanges> longChunk) {
        return OrderedKeysKeyRangesChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Get an {@link Iterator} over this {@code OrderedKeys}.
     *
     * @return A new iterator, positioned at the first key
     */
    Iterator getOrderedKeysIterator();

    /**
     * <p>
     * Get an ordered subset of the keys in this {@code OrderedKeys} for a position range. The result will contain the
     * set of keys in {@code this} that lie at positions in the half-open range [{@code startPositionInclusive},
     * {@code startPositionInclusive + length}).
     *
     * The returned reference is owned by the caller, who should call {@code close()} when it is done with it.
     *
     * @param startPositionInclusive The position of the first key to include
     * @param length The number of keys to include
     * @return The subset as an {@code OrderedKeys}, which may be {@code this}
     */
    OrderedKeys getOrderedKeysByPosition(long startPositionInclusive, long length);

    /**
     * <p>
     * Get an ordered subset of the keys in this {@code OrderedKeys} for a key range. The returned set will be the
     * intersection of the keys in {@code this} with the keys in the closed interval [{@code startKeyInclusive},
     * {@code endKeyInclusive}].
     *
     * The returned reference is owned by the caller, who should call {@code close()} when it is done with it.
     *
     * @param startKeyInclusive The minimum key to include
     * @param endKeyInclusive The maximum key to include
     * @return The subset as an {@code OrderedKeys}, which may be {@code this}
     */
    OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive);

    /**
     * Get an {@link Index} representation of this {@code OrderedKeys}.
     *
     * @return An {@link Index} representation for the same keys in the same order
     * @apiNote If you use the result across clock ticks, you may observe inconsistencies.
     * @apiNote You must not mutate the result.
     */
    Index asIndex();

    /**
     * Get a {@link LongChunk} representation of the individual keys in this {@code OrderedKeys}.
     *
     * @return A {@link LongChunk} containing the keys in this {@code OrderedKeys}
     * @apiNote This {@code OrderedKeys} owns the result, which is valid only as long as this {@code OrderedKeys}
     *          remains valid.
     * @apiNote You must not mutate the result.
     */
    LongChunk<OrderedKeyIndices> asKeyIndicesChunk();

    /**
     * Get a {@link LongChunk} representation of key ranges in this {@code OrderedKeys}.
     *
     * @return A {@link LongChunk} containing the key ranges in this {@code OrderedKeys}
     * @apiNote This {@code OrderedKeys} owns the result, which is valid only as long as this {@code OrderedKeys}
     *          remains valid.
     * @apiNote You must not mutate the result.
     */
    LongChunk<OrderedKeyRanges> asKeyRangesChunk();

    /**
     * <p>
     * Fill the supplied {@link WritableLongChunk} with individual keys from this {@code OrderedKeys}.
     * <p>
     * The chunk's capacity is assumed to be big enough.
     *
     * @param chunkToFill A chunk to fill with individual keys
     */
    void fillKeyIndicesChunk(WritableLongChunk<? extends KeyIndices> chunkToFill);

    /**
     * <p>
     * Fill the supplied {@link WritableLongChunk} with key ranges from this {@code OrderedKeys}.
     * <p>
     * The chunk's capacity is assumed to be big enough.
     *
     * @param chunkToFill A chunk to fill with key ranges
     */
    void fillKeyRangesChunk(WritableLongChunk<OrderedKeyRanges> chunkToFill);

    /**
     * True if the size of this {@code Orderedkeys} is zero.
     *
     * @return True if there are no elements in this {@code OrderedKeys}.
     */
    boolean isEmpty();

    /**
     * Get the first key in this {@code OrderedKeys}.
     *
     * @return The first key, or {@link Index#NULL_KEY} if there is none.
     */
    long firstKey();

    /**
     * Get the last key in this {@code OrderedKeys}.
     *
     * @return The last key, or {@link Index#NULL_KEY} if there is none.
     */
    long lastKey();

    /**
     * Get the number of keys in this {@code OrderedKeys}.
     *
     * @return The size, in [0, {@link Long#MAX_VALUE}]
     */
    long size();

    /**
     * Helper to tell you if this is one contiguous range.
     */
    default boolean isContiguous() {
        return size() == 0 || lastKey() - firstKey() == size() - 1;
    }

    /**
     * <p>
     * Get an estimate of the average (mean) length of runs of adjacent keys in this {@code OrderedKeys}.
     * <p>
     * Implementations should strive to keep this method efficient (<i>O(1)</i> preferred) at the expense of accuracy.
     * <p>
     * Empty {@code OrderedKeys} should return an arbitrary valid value, usually 1.
     *
     * @return An estimate of the average run length in this {@code OrderedKeys}, in [1, {@code size()}]
     */
    long getAverageRunLengthEstimate();

    /**
     * For as long as the consumer wants more keys, call accept on the consumer with the individual key instances in
     * this OrderedKeys, in increasing order.
     *
     * @param lac a consumer to feed the individual key values to.
     * @return false if the consumer provided ever returned false, true otherwise.
     */
    boolean forEachLong(LongAbortableConsumer lac);

    /**
     * For as long as the consumer wants more ranges, call accept on the consumer with the individual key ranges in this
     * OrderedKeys, in increasing order.
     *
     * @param larc a consumer to feed the individual key values to.
     * @return false if the consumer provided ever returned false, true otherwise.
     */
    boolean forEachLongRange(LongRangeAbortableConsumer larc);

    default void forAllLongs(java.util.function.LongConsumer lc) {
        forEachLong((final long v) -> {
            lc.accept(v);
            return true;
        });
    }

    default void forAllLongRanges(LongRangeConsumer lrc) {
        forEachLongRange((final long first, final long last) -> {
            lrc.accept(first, last);
            return true;
        });
    }

    /**
     * <p>
     * Free any resources associated with this object.
     * <p>
     * Using any {@code OrderedKeys} methods after {@code close()} is an error and may produce exceptions or undefined
     * results.
     */
    default void close() {}

    static OrderedKeys forRange(final long firstKey, final long lastKey) {
        // NB: We could use a pooled chunk, here, but the pool doesn't usually
        // hold chunks this small. Probably not worth the code complexity for release, either.
        return wrapKeyRangesChunkAsOrderedKeys(LongChunk.chunkWrap(new long[] {firstKey, lastKey}));
    }

    /**
     * Iterator for consuming an {@code OrderedKeys} by ordered subsets.
     */
    interface Iterator extends SafeCloseable {

        /**
         * Poll whether there are more keys available from this {@link Iterator}.
         *
         * @return True if there are more keys available, else false
         */
        boolean hasMore();

        /**
         * Peek at the next key that would be returned by {@link #getNextOrderedKeysThrough(long)} or
         * {@link #getNextOrderedKeysWithLength(long)}. Does not advance the position.
         *
         * @return The next key that would be returned, or {@link Index#NULL_KEY} if this iterator is exhausted
         */
        long peekNextKey();

        /**
         * Get an {@code OrderedKeys} from the key at the position of this iterator up to the maximum key (inclusive).
         * Advances the position of this iterator by the size of the result. If the maximum key provided is smaller than
         * the next key (as would be returned by {@link #peekNextKey()}), the empty OrderedKeys is returned.
         *
         * The returned OrderedKeys object is only borrowed by the caller from the {@link Iterator}, who owns it. It is
         * guaranteed to be valid and not change only until a later call to another {@code getNext*} method. As the
         * returned reference is owned by the {@link Iterator}, the caller <i>should not</i> call {@code close()} on it.
         *
         * @param maxKeyInclusive The maximum key to include.
         * @return An {@code OrderedKeys} from the key at the initial position up to the maximum key (inclusive).
         */
        OrderedKeys getNextOrderedKeysThrough(long maxKeyInclusive);

        /**
         * Get an {@code OrderedKeys} from the key at the position of this iterator up to the desired number of keys.
         * Advances the position of this iterator by the size of the result.
         *
         * The returned OrderedKeys object is only borrowed by the caller from the {@link Iterator}, who owns it. It is
         * guaranteed to be valid and not change only until the next call to another {@code getNext*} method. As the
         * returned reference is owned by the {@link Iterator}, the caller <i>should not</i> call {@code close()} on it.
         *
         * @param numberOfKeys The desired number of keys
         * @return An {@code OrderedKeys} from the key at the initial position up to the desired number of keys
         */
        OrderedKeys getNextOrderedKeysWithLength(long numberOfKeys);

        /**
         * <p>
         * Advance this iterator's position to {@code nextKey}, or to the first present key greater than {@code nextKey}
         * if {@code nextKey} is not found. If {@code nextKey} is less than or equal to the key at this iterator's
         * current position, this method is a no-op.
         * <p>
         * Subsequent calls to {@link #peekNextKey()}, {@link #getNextOrderedKeysThrough(long)}, or
         * {@link #getNextOrderedKeysWithLength(long)} will begin with the key advanced to.
         *
         * @param nextKey The key to advance to
         * @return true If there are any keys remaining to be iterated after the advance, false if this {@link Iterator}
         *         is exhausted
         */
        boolean advance(long nextKey);

        /**
         * Advance this iterator's position as in {@link #advance(long)}, returning the number of keys thus consumed.
         *
         * @param nextKey The key to advance to
         * @return The number of keys consumed from the iterator
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
         * Callers of {@link OrderedKeys#getOrderedKeysIterator()} are responsible for ensuring that {@code close()} is
         * called when they are done with resulting {@link Iterator}.
         * <p>
         * Using any {@link Iterator} methods after {@code close()} is an error and may produce exceptions or undefined
         * results.
         */
        default void close() {}

        /**
         * Taking the difference between values returned by this method at different positions in the iterator gives you
         * the cardinality of the set of keys between them, exclusive. Note a single value itself is not meaningful;
         * like measuring elapsed time, it only makes sense to take the difference from absolute points.
         *
         * @return A relative position offset from some arbitrary initial point in the underlying ordered keys.
         */
        long getRelativePosition();

        /**
         * Immutable, re-usable {@link Iterator} for an empty {@code OrderedKeys}.
         */
        Iterator EMPTY = new Iterator() {

            @Override
            public boolean hasMore() {
                return false;
            }

            @Override
            public long peekNextKey() {
                return Index.NULL_KEY;
            }

            @Override
            public OrderedKeys getNextOrderedKeysThrough(long maxKeyInclusive) {
                return OrderedKeys.EMPTY;
            }

            @Override
            public OrderedKeys getNextOrderedKeysWithLength(long numberOfKeys) {
                return OrderedKeys.EMPTY;
            }

            @Override
            public boolean advance(long nextKey) {
                return false;
            }

            @Override
            public long getRelativePosition() {
                return 0;
            }
        };
    }

    /**
     * Immutable, re-usable {@code OrderedKeys} instance.
     */
    OrderedKeys EMPTY = new OrderedKeys() {

        @Override
        public Iterator getOrderedKeysIterator() {
            return Iterator.EMPTY;
        }

        @Override
        public OrderedKeys getOrderedKeysByPosition(long startPositionInclusive, long length) {
            return this;
        }

        @Override
        public OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive) {
            return this;
        }

        @Override
        public Index asIndex() {
            return Index.FACTORY.getEmptyIndex();
        }

        @Override
        public LongChunk<OrderedKeyIndices> asKeyIndicesChunk() {
            return WritableLongChunk.getEmptyChunk();
        }

        @Override
        public LongChunk<OrderedKeyRanges> asKeyRangesChunk() {
            return WritableLongChunk.getEmptyChunk();
        }

        @Override
        public void fillKeyIndicesChunk(WritableLongChunk<? extends KeyIndices> chunkToFill) {
            chunkToFill.setSize(0);
        }

        @Override
        public void fillKeyRangesChunk(WritableLongChunk<OrderedKeyRanges> chunkToFill) {
            chunkToFill.setSize(0);
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long firstKey() {
            return Index.NULL_KEY;
        }

        @Override
        public long lastKey() {
            return Index.NULL_KEY;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean isContiguous() {
            return true;
        }

        @Override
        public long getAverageRunLengthEstimate() {
            return 1;
        }

        @Override
        public boolean forEachLong(final LongAbortableConsumer lac) {
            return true;
        }

        @Override
        public boolean forEachLongRange(final LongRangeAbortableConsumer larc) {
            return true;
        }

        @Override
        public String toString() {
            return "OrderedKeys.EMPTY";
        }
    };
}
