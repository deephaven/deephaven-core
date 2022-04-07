package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
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

    protected final ARRAY ring;
    protected final int capacity;
    long nextRingIx;

    private final ResettableWritableChunk<Values> ringView;

    public AbstractRingChunkSource(@NotNull Class<T> componentType, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        // noinspection unchecked
        ring = (ARRAY) Array.newInstance(componentType, capacity);
        ringView = getChunkType().makeResettableWritableChunk();
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
     * The first key (inclusive). If {@link #isEmpty()}, returns {@code 0}.
     *
     * @return the first key
     * @see #lastKey()
     */
    public final long firstKey() {
        return Math.max(nextRingIx - capacity, 0);
    }

    /**
     * The last key (inclusive). If {@link #isEmpty()}, returns {@code -1}.
     *
     * @return the last key
     * @see #firstKey()
     */
    public final long lastKey() {
        return nextRingIx - 1;
    }

    /**
     * Equivalent to {@code append(fillContext, (ChunkSource<? extends Values>) src, srcKeys)}.
     *
     * @param fillContext the fill context
     * @param src the source
     * @param srcKeys the source keys
     * @see #append(FillContext, ChunkSource, RowSet)
     */
    public final void append(FillContext fillContext, ColumnSource<T> src, RowSet srcKeys) {
        append(fillContext, (ChunkSource<? extends Values>) src, srcKeys);
    }

    /**
     * Append the data represented by {@code src} and {@code srcKeys} into {@code this} ring. This method is meant to be
     * efficient, and will read at most {@link #capacity()} items from the end of {@code src} and {@code srcKeys}. The
     * {@link #lastKey() lastKey} will increase by {@code srcKeys.size()}.
     *
     * @param fillContext the fill context
     * @param src the source
     * @param srcKeys the source keys
     */
    public final void append(FillContext fillContext, ChunkSource<? extends Values> src, RowSet srcKeys) {
        if (srcKeys.isEmpty()) {
            return;
        }
        final long logicalFillSize = srcKeys.size();
        final RowSet physicalRows;
        final long physicalStartRingIx;
        final int physicalFillSize;
        final boolean hasSkippedRows = logicalFillSize > capacity;
        if (!hasSkippedRows) {
            physicalRows = srcKeys;
            physicalStartRingIx = nextRingIx;
            physicalFillSize = (int) logicalFillSize;
        } else {
            final long skipRows = logicalFillSize - capacity;
            physicalRows = srcKeys.subSetByPositionRange(skipRows, logicalFillSize);
            physicalStartRingIx = nextRingIx + skipRows;
            physicalFillSize = capacity;
        }
        try {
            // [0, capacity)
            final int fillIndex1 = keyToRingIndex(physicalStartRingIx);
            // (0, capacity]
            final int fillMax1 = capacity - fillIndex1;
            // fillSize1 + fillSize2 = physicalFillSize
            final int fillSize1 = Math.min(fillMax1, physicalFillSize);
            final int fillSize2 = physicalFillSize - fillSize1;
            if (fillSize2 == 0) {
                src.fillChunk(fillContext, ring(fillIndex1, fillSize1), physicalRows);
            } else {
                // might be nice if there was a "split"
                // (could be more efficient than calling subSetByPositionRange twice)
                try (final RowSet rows1 = physicalRows.subSetByPositionRange(0, fillSize1)) {
                    src.fillChunk(fillContext, ring(fillIndex1, fillSize1), rows1);
                }
                try (final RowSet rows2 = physicalRows.subSetByPositionRange(fillSize1, fillSize1 + fillSize2)) {
                    src.fillChunk(fillContext, ring(0, fillSize2), rows2);
                }
            }
        } finally {
            if (hasSkippedRows) {
                physicalRows.close();
            }
        }
        nextRingIx += logicalFillSize;
    }

    @Override
    public final Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
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
    public final Chunk<Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        // This check should not be necessary given precondition on getChunk
        if (!containsRange(firstKey, lastKey)) {
            throw new IllegalStateException(
                    String.format("getChunk precondition broken, invalid range. requested=[%d, %d], available=[%d, %d]",
                            firstKey, lastKey, firstKey(), lastKey()));
        }
        final int firstRingIx = keyToRingIndex(firstKey);
        final int secondRingIx = keyToRingIndex(lastKey);
        if (firstRingIx <= secondRingIx) {
            // Optimization when we can return a contiguous view
            return ring(firstRingIx, secondRingIx - firstRingIx + 1); // todo: is it bad that this is writable?
        }
        final WritableChunk<Values> chunk = DefaultGetContext.getWritableChunk(context);
        final int size = fillByCopy2(chunk, 0, firstRingIx, secondRingIx);
        if (size != lastKey - firstKey + 1) {
            throw new IllegalStateException();
        }
        chunk.setSize(size);
        return chunk;
    }

    @Override
    public final void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            try (final KeyFiller filler = new KeyFiller(destination)) {
                rowSequence.forAllRowKeys(filler);
            }
        } else {
            try (final RangeFiller filler = new RangeFiller(destination)) {
                rowSequence.forAllRowKeyRanges(filler);
            }
        }
    }

    private final class KeyFiller implements LongConsumer, Closeable {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public KeyFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long key) {
            // this check should not be necessary given precondition on fillChunk
            if (!containsKey(key)) {
                throw new IllegalStateException(
                        String.format("fillChunk precondition broken, invalid key. requested=%d, available=[%d, %d]",
                                key, firstKey(), lastKey()));
            }
            final int ringIx = keyToRingIndex(key);
            fillKey(destination, destOffset, ringIx);
            ++destOffset;
        }

        @Override
        public void close() {
            destination.setSize(destOffset);
        }
    }

    private final class RangeFiller implements LongRangeConsumer, Closeable {
        private final WritableChunk<? super Values> destination;
        private int destOffset;

        public RangeFiller(WritableChunk<? super Values> destination) {
            this.destination = Objects.requireNonNull(destination);
        }

        @Override
        public void accept(long firstKey, long lastKey) {
            // this check should not be necessary given precondition on fillChunk
            if (!containsRange(firstKey, lastKey)) {
                throw new IllegalStateException(String.format(
                        "fillChunk precondition broken, invalid range. requested=[%d, %d], available=[%d, %d]",
                        firstKey, lastKey, firstKey(), lastKey()));
            }
            final int firstRingIx = keyToRingIndex(firstKey);
            final int lastRingIx = keyToRingIndex(lastKey);
            final int size = fillByCopy(destination, destOffset, firstRingIx, lastRingIx);
            if (size != lastKey - firstKey + 1) {
                throw new IllegalStateException();
            }
            destOffset += size;
        }

        @Override
        public void close() {
            destination.setSize(destOffset);
        }
    }

    private WritableChunk<Values> ring(int offset, int length) {
        return ringView.resetFromArray(ring, offset, length);
    }

    private int fillByCopy(@NotNull WritableChunk<? super Values> destination, int destOffset, int firstRingIx,
            int lastRingIx) {
        // Precondition: valid firstRingIx, lastRingIx
        if (firstRingIx <= lastRingIx) {
            // Optimization when we can accomplish with single copy
            final int size = lastRingIx - firstRingIx + 1;
            destination.copyFromArray(ring, firstRingIx, destOffset, size);
            return size;
        }
        return fillByCopy2(destination, destOffset, firstRingIx, lastRingIx);
    }

    private int fillByCopy2(@NotNull WritableChunk<? super Values> destination, int destOffset, int firstRingIx,
            int lastRingIx) {
        // Precondition: valid firstRingIx, lastRingIx
        // Precondition: firstRingIx > lastRingIx
        final int fillSize1 = capacity - firstRingIx;
        final int fillSize2 = lastRingIx + 1;
        destination.copyFromArray(ring, firstRingIx, destOffset, fillSize1);
        destination.copyFromArray(ring, 0, destOffset + fillSize1, fillSize2);
        return fillSize1 + fillSize2;
    }

    final int keyToRingIndex(long key) {
        return (int) (key % capacity);
    }

    final void bringUpToDate(FillContext fillContext, SELF current) {
        // We *could* try to be smart and get away with a single copy.
        // append should be relatively efficient though, and at worst will be two copies...
        // final long logicalFillSize = current.nextRingIx - nextRingIx;
        // if (logicalFillSize >= capacity / 2) {
        // // noinspection SuspiciousSystemArraycopy
        // System.arraycopy(current.ring, 0, ring, 0, capacity);
        // nextRingIx = current.nextRingIx;
        // return;
        // }
        append(fillContext, current, RowSetFactory.fromRange(nextRingIx, current.nextRingIx - 1));
        if (nextRingIx != current.nextRingIx) {
            throw new IllegalStateException();
        }
    }

    abstract void clear();

    abstract void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx);

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
