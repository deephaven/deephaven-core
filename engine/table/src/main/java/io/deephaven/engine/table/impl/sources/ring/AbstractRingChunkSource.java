//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequence.Iterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.util.datastructures.LongRangeConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.LongConsumer;

import static io.deephaven.engine.table.impl.AbstractColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;

/**
 * The base for a ring chunk source. Provides a single contiguous array for ring-buffer operations.
 *
 * @param <T> the item type
 * @param <ARRAY> the array type
 * @param <SELF> the self type
 */
abstract class AbstractRingChunkSource<T, ARRAY, SELF extends AbstractRingChunkSource<T, ARRAY, SELF>>
        implements DefaultChunkSource<Values> {

    /**
     * The return value of {@link #firstKey()} when {@link #isEmpty()}.
     */
    public static final long FIRST_KEY_EMPTY = 0;

    /**
     * The return value of {@link #lastKey()} when {@link #isEmpty()}.
     */
    public static final long LAST_KEY_EMPTY = -1;

    public static final String STRICT_KEYS_KEY = "AbstractRingChunkSource.strictKeys";

    /**
     * When strict key checks are enabled, read methods will throw {@link IllegalArgumentException} when the key is not
     * contained within the key range. Looks up the configuration key {@value STRICT_KEYS_KEY}. Defaults to
     * {@code true}.
     */
    public static final boolean STRICT_KEYS = Configuration.getInstance().getBooleanWithDefault(STRICT_KEYS_KEY, true);

    public static final String APPEND_CHUNK_SIZE_KEY = "AbstractRingChunkSource.appendChunkSize";

    /**
     * The chunk size used while {@link #appendBounded(ChunkSource, RowSet)}. Looks up the configuration key
     * {@value APPEND_CHUNK_SIZE_KEY}. Defaults to {@code 4096}.
     */
    public static final int APPEND_CHUNK_SIZE =
            Configuration.getInstance().getIntegerWithDefault(APPEND_CHUNK_SIZE_KEY, 4096);

    protected final ARRAY ring;
    protected final int capacity;
    long nextRingIx;

    public AbstractRingChunkSource(ARRAY ring) {
        this.ring = Objects.requireNonNull(ring);
        this.capacity = Array.getLength(ring);
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
    }

    /**
     * The maximum size {@code this} ring can hold. Constant.
     *
     * @return the capacity
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * The size, {@code 0 <= size <= capacity}. The size will never shrink.
     *
     * <p>
     * Logically equivalent to {@code lastKey - firstKey + 1}.
     *
     * @return the size
     */
    public final int size() {
        return capacity <= nextRingIx ? capacity : (int) nextRingIx;
    }

    /**
     * {@code true} if empty, else {@code false}. Once {@code false} is returned, will always return {@code false}.
     *
     * <p>
     * Logically equivalent to {@code size == 0}.
     *
     * @return {@code true} if empty
     */
    public final boolean isEmpty() {
        return nextRingIx == 0;
    }

    /**
     * {@code true} if {@code key} is in the index.
     *
     * <p>
     * Logically equivalent to the condition {@code firstKey <= key <= lastKey}.
     *
     * @param key the key
     * @return {@code true} if {@code key} is in the index.
     * @see #firstKey()
     * @see #lastKey()
     */
    public final boolean containsKey(long key) {
        // branchless check, probably better than needing to compute size() or firstKey()
        return key >= 0 && key >= (nextRingIx - capacity) && key < nextRingIx;
    }

    /**
     * {@code true} if {@code [firstKey, lastKey]} is in the index.
     *
     * <p>
     * Equivalent to {@code containsKey(firstKey) && containsKey(lastKey) && firstKey <= lastKey}.
     *
     * @param firstKey the first key (inclusive)
     * @param lastKey the last key (inclusive)
     * @return true if the [firstKey, lastKey] is in {@code this} range
     */
    public final boolean containsRange(long firstKey, long lastKey) {
        return firstKey <= lastKey && firstKey >= 0 && firstKey >= (nextRingIx - capacity) && lastKey < nextRingIx;
    }

    /**
     * The first key (inclusive). If {@link #isEmpty()}, returns {@value #FIRST_KEY_EMPTY}.
     *
     * @return the first key
     * @see #lastKey()
     */
    public final long firstKey() {
        return Math.max(nextRingIx - capacity, 0);
    }

    /**
     * The last key (inclusive). If {@link #isEmpty()}, returns {@value #LAST_KEY_EMPTY}.
     *
     * @return the last key
     * @see #firstKey()
     */
    public final long lastKey() {
        return nextRingIx - 1;
    }

    /**
     * Equivalent to {@code append(source, srcKeys, APPEND_CHUNK_SIZE)}.
     *
     * <p>
     * A generic append with a conservatively sized {@code appendChunkSize}. If the caller knows that the {@code source}
     * uses a {@link FillContext} with {@link FillContext#supportsUnboundedFill()}, they should instead call
     * {@link #appendUnbounded(ChunkSource, RowSet)}.
     * 
     * @param source the source
     * @param srcKeys the source keys
     * @see #append(ChunkSource, RowSet, int)
     */
    public final void appendBounded(ChunkSource<? extends Values> source, RowSet srcKeys) {
        append(source, srcKeys, APPEND_CHUNK_SIZE);
    }

    /**
     * Equivalent to {@code append(source, srcKeys, capacity)}.
     *
     * <p>
     * A specialized append, where the caller knows that {@code source} is using a {@link FillContext} with
     * {@link FillContext#supportsUnboundedFill()}.
     *
     * @param source the source
     * @param srcKeys the source keys
     * @see #append(ChunkSource, RowSet, int)
     */
    public final void appendUnbounded(ChunkSource<? extends Values> source, RowSet srcKeys) {
        append(source, srcKeys, capacity);
    }

    /**
     * Append the data represented by {@code source} and {@code srcKeys} into {@code this} ring. This method is meant to
     * be efficient, and will read at most {@link #capacity()} items from the end of {@code source} and {@code srcKeys}.
     * The {@link #lastKey() lastKey} will increase by {@code srcKeys.size()}.
     *
     * @param source the source
     * @param srcKeys the source keys
     * @param appendChunkSize the append chunk size
     */
    public final void append(ChunkSource<? extends Values> source, RowSet srcKeys, int appendChunkSize) {
        if (srcKeys.isEmpty()) {
            return;
        }
        final long logicalFillSize = srcKeys.size();
        final long skipRows = Math.max(logicalFillSize - capacity, 0);
        final int fillStartIx = keyToRingIndex(nextRingIx + skipRows);
        try (
                final ResettableWritableChunk<Any> chunk = getChunkType().makeResettableWritableChunk();
                final FillContext fillContext = source.makeFillContext(appendChunkSize);
                final Iterator it = srcKeys.getRowSequenceIterator()) {
            if (skipRows > 0) {
                skipRows(it, srcKeys.get(skipRows), skipRows);
            }
            fillRingFromMiddle(source, chunk, fillContext, it, fillStartIx, appendChunkSize);
            fillRingFromStart(source, chunk, fillContext, it, fillStartIx, appendChunkSize);
        }
        nextRingIx += logicalFillSize;
    }

    private static void skipRows(Iterator it, long skipKey, long expectedDistance) {
        final long actualDistance = it.advanceAndGetPositionDistance(skipKey);
        if (expectedDistance != actualDistance) {
            throw new IllegalStateException(
                    String.format("Inconsistent advanceAndGetPositionDistance: key=%d, expected=%d, actual=%d", skipKey,
                            expectedDistance, actualDistance));
        }
    }

    private void fillRingFromMiddle(
            final ChunkSource<? extends Values> src,
            final ResettableWritableChunk<Any> chunk,
            final FillContext fillContext,
            final Iterator it,
            final int ringStartIx,
            final int appendChunkSize) {
        int ringIx = ringStartIx;
        int nextSize = Math.min(appendChunkSize, capacity - ringIx);
        do {
            final RowSequence rows = it.getNextRowSequenceWithLength(nextSize);
            final int rowsSize = rows.intSize();
            src.fillChunk(fillContext, ring(chunk, ringIx, rowsSize), rows);
            ringIx += rowsSize;
        } while (it.hasMore() && (nextSize = Math.min(appendChunkSize, capacity - ringIx)) > 0);
    }

    private void fillRingFromStart(
            final ChunkSource<? extends Values> src,
            final ResettableWritableChunk<Any> chunk,
            final FillContext fillContext,
            final Iterator it,
            final int ringStartIx,
            final int appendChunkSize) {
        int ringIx = 0;
        while (it.hasMore()) {
            final RowSequence rows = it.getNextRowSequenceWithLength(appendChunkSize);
            final int rowsSize = rows.intSize();
            if (ringIx + rowsSize > ringStartIx) {
                throw new IllegalStateException("Overrunning into the start of our fillRingFromMiddle");
            }
            src.fillChunk(fillContext, ring(chunk, ringIx, rowsSize), rows);
            ringIx += rowsSize;
        }
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return getChunkType().getEmptyChunk();
        }
        if (rowSequence.isContiguous()) {
            return getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey());
        }
        final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
        fillChunk(DefaultGetContext.getFillContext(context), chunk, rowSequence);
        return chunk;
    }

    @Override
    public final Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        if (STRICT_KEYS && !containsRange(firstKey, lastKey)) {
            throw new IllegalStateException(
                    String.format("getChunk precondition broken, invalid range. requested=[%d, %d], available=[%d, %d]",
                            firstKey, lastKey, firstKey(), lastKey()));
        }
        final int firstRingIx = keyToRingIndex(firstKey);
        final int lastRingIx = keyToRingIndex(lastKey);
        if (firstRingIx <= lastRingIx) {
            // Optimization when we can return a contiguous view
            return ring(DefaultGetContext.getResettableChunk(context), firstRingIx, lastRingIx - firstRingIx + 1);
        }
        final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
        try (final Filler filler = filler(chunk)) {
            filler.acceptCopy2(firstRingIx, lastRingIx);
        }
        return chunk;
    }

    @Override
    public final void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            destination.setSize(0);
            return;
        }
        if (STRICT_KEYS) {
            final long firstKey = rowSequence.firstRowKey();
            final long lastKey = rowSequence.lastRowKey();
            if (!containsRange(firstKey, lastKey)) {
                throw new IllegalStateException(String.format(
                        "fillChunk precondition broken, invalid range. requested=[%d, %d] (or subset), available=[%d, %d]",
                        firstKey, lastKey, firstKey(), lastKey()));
            }
        }
        try (final Filler filler = filler(destination)) {
            if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
                rowSequence.forAllRowKeys(filler);
            } else {
                rowSequence.forAllRowKeyRanges(filler);
            }
        }
    }

    protected abstract class Filler implements LongConsumer, LongRangeConsumer, Closeable {

        private int destOffset = 0;

        @Override
        public final void accept(long key) {
            copyFromRing(keyToRingIndex(key), destOffset);
            ++destOffset;
        }

        @Override
        public final void accept(long firstKey, long lastKey) {
            final int firstRingIx = keyToRingIndex(firstKey);
            final int lastRingIx = keyToRingIndex(lastKey);
            final int size = fillByCopy(firstRingIx, lastRingIx, destOffset);
            if (size != lastKey - firstKey + 1) {
                throw new IllegalStateException();
            }
            destOffset += size;
        }

        public final void acceptCopy2(int firstRingIx, int lastRingIx) {
            final int size = fillByCopy2(firstRingIx, lastRingIx, destOffset);
            destOffset += size;
        }

        @Override
        public final void close() {
            setSize(destOffset);
        }

        private int fillByCopy(int firstRingIx, int lastRingIx, int destOffset) {
            // Precondition: valid firstRingIx, lastRingIx
            if (firstRingIx <= lastRingIx) {
                // Optimization when we can accomplish with single copy
                final int size = lastRingIx - firstRingIx + 1;
                copyFromRing(firstRingIx, destOffset, size);
                return size;
            }
            return fillByCopy2(firstRingIx, lastRingIx, destOffset);
        }

        private int fillByCopy2(int firstRingIx, int lastRingIx, int destOffset) {
            // Precondition: valid firstRingIx, lastRingIx
            // Precondition: firstRingIx > lastRingIx
            final int fillSize1 = capacity - firstRingIx;
            final int fillSize2 = lastRingIx + 1;
            copyFromRing(firstRingIx, destOffset, fillSize1);
            copyFromRing(0, destOffset + fillSize1, fillSize2);
            return fillSize1 + fillSize2;
        }

        protected abstract void copyFromRing(int srcRingIx, int destOffset);

        protected abstract void copyFromRing(int srcRingIx, int destOffset, int size);

        protected abstract void setSize(int size);
    }

    private WritableChunk<Values> ring(ResettableWritableChunk<? super Values> chunk, int ringIx, int length) {
        return chunk.resetFromArray(ring, ringIx, length);
    }

    /**
     * Compute the ring index. Assumes that {@code key} is valid and in the range of {@code this}. When
     * {@link #STRICT_KEYS} is {@code true}, callers using the return value for reading purposes should verify
     * {@code key} with {@link #containsKey(long)} or {@link #containsRange(long, long)}.
     *
     * <p>
     * Equivalent to {@code (int) (key % capacity)}.
     *
     * @param key the key
     * @return the ring index
     */
    final int keyToRingIndex(long key) {
        return (int) (key % capacity);
    }

    final void bringUpToDate(SELF current) {
        // When we are bringing ourselves up-to-date, we know that current is a ring and uses an unbounded FillContext.
        // Logically copying the full range [lastKey() + 1, current.lastKey()] from current to previous even though the
        // physical data in current may be smaller.
        appendUnbounded(current, RowSetFactory.fromRange(nextRingIx, current.lastKey()));
        if (nextRingIx != current.nextRingIx) {
            throw new IllegalStateException();
        }
    }

    abstract Filler filler(@NotNull WritableChunk<? super Values> destination);

    abstract T get(long key);

    byte getByte(long key) {
        throw new UnsupportedOperationException();
    }

    char getChar(long key) {
        throw new UnsupportedOperationException();
    }

    double getDouble(long key) {
        throw new UnsupportedOperationException();
    }

    float getFloat(long key) {
        throw new UnsupportedOperationException();
    }

    int getInt(long key) {
        throw new UnsupportedOperationException();
    }

    long getLong(long key) {
        throw new UnsupportedOperationException();
    }

    short getShort(long key) {
        throw new UnsupportedOperationException();
    }
}
