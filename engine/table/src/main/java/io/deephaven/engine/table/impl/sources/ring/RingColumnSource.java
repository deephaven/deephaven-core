/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * A ring column source.
 *
 * @param <T> the item type
 */
final class RingColumnSource<T>
        extends AbstractColumnSource<T>
        implements InMemoryColumnSource {

    public static RingColumnSource<Byte> ofByte(int capacity) {
        return ByteRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Character> ofCharacter(int capacity) {
        return CharacterRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Double> ofDouble(int capacity) {
        return DoubleRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Float> ofFloat(int capacity) {
        return FloatRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Integer> ofInteger(int capacity) {
        return IntegerRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Long> ofLong(int capacity) {
        return LongRingChunkSource.columnSource(capacity);
    }

    public static RingColumnSource<Short> ofShort(int capacity) {
        return ShortRingChunkSource.columnSource(capacity);
    }

    public static <T> RingColumnSource<T> ofObject(Class<T> type, int capacity) {
        return ObjectRingChunkSource.columnSource(type, capacity);
    }

    public static <T> RingColumnSource<T> ofObject(Class<T> type, Class<?> componentType, int capacity) {
        return ObjectRingChunkSource.columnSource(type, componentType, capacity);
    }

    @SuppressWarnings("unchecked")
    public static <T> RingColumnSource<T> of(int capacity, Class<T> dataType, Class<?> componentType) {
        if (dataType == byte.class || dataType == Byte.class) {
            return (RingColumnSource<T>) ofByte(capacity);
        } else if (dataType == char.class || dataType == Character.class) {
            return (RingColumnSource<T>) ofCharacter(capacity);
        } else if (dataType == double.class || dataType == Double.class) {
            return (RingColumnSource<T>) ofDouble(capacity);
        } else if (dataType == float.class || dataType == Float.class) {
            return (RingColumnSource<T>) ofFloat(capacity);
        } else if (dataType == int.class || dataType == Integer.class) {
            return (RingColumnSource<T>) ofInteger(capacity);
        } else if (dataType == long.class || dataType == Long.class) {
            return (RingColumnSource<T>) ofLong(capacity);
        } else if (dataType == short.class || dataType == Short.class) {
            return (RingColumnSource<T>) ofShort(capacity);
        } else if (dataType == boolean.class || dataType == Boolean.class) {
            throw new UnsupportedOperationException(
                    "No Boolean chunk source for RingColumnSource - use byte and reinterpret");
        } else if (dataType == DateTime.class) {
            throw new UnsupportedOperationException(
                    "No DateTime chunk source for RingColumnSource - use long and reinterpret");
        } else {
            if (componentType != null) {
                return ofObject(dataType, componentType, capacity);
            } else {
                return ofObject(dataType, capacity);
            }
        }
    }

    private final AbstractRingChunkSource<T, ?, ?> ring;
    private final AbstractRingChunkSource<T, ?, ?> prev;

    <ARRAY, RING extends AbstractRingChunkSource<T, ARRAY, RING>> RingColumnSource(
            @NotNull Class<T> type,
            RING ring,
            RING prev) {
        super(type);
        this.ring = Objects.requireNonNull(ring);
        this.prev = Objects.requireNonNull(prev);
    }

    <ARRAY, RING extends AbstractRingChunkSource<T, ARRAY, RING>> RingColumnSource(
            @NotNull Class<T> type,
            Class<?> elementType,
            RING ring,
            RING prev) {
        super(type, elementType);
        this.ring = Objects.requireNonNull(ring);
        this.prev = Objects.requireNonNull(prev);
    }

    public int capacity() {
        return ring.capacity();
    }

    public void appendBounded(ChunkSource<? extends Values> source, RowSet srcKeys) {
        ring.appendBounded(source, srcKeys);
    }

    public void appendUnbounded(ChunkSource<? extends Values> source, RowSet srcKeys) {
        ring.appendUnbounded(source, srcKeys);
    }

    public void bringPreviousUpToDate() {
        // noinspection unchecked,rawtypes
        ((AbstractRingChunkSource) prev).bringUpToDate(ring);
    }

    public WritableRowSet rowSet() {
        return ring.isEmpty() ? RowSetFactory.empty() : RowSetFactory.fromRange(ring.firstKey(), ring.lastKey());
    }

    public TableUpdate tableUpdate() {
        // Precondition: some rows have been added (k4 > k2)
        final long k1 = prev.firstKey();
        final long k2 = prev.lastKey();
        final long k3 = ring.firstKey();
        final long k4 = ring.lastKey();
        final RowSet removed;
        final RowSet added;
        if (k2 < k3) {
            // No intersection. Prev may be empty.
            // Removed: empty or [k1, k2]
            // Added: [k3, k4]
            removed = k2 == AbstractRingChunkSource.LAST_KEY_EMPTY ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(k1, k2);
            added = RowSetFactory.fromRange(k3, k4);
        } else {
            // Intersection from [k3, k2]. Neither prev nor ring are empty.
            // k1 <= k3 <= k2 < k4
            // Removed: empty or [k1, k3)
            // Added: (k2, k4]
            removed = k1 == k3 ? RowSetFactory.empty() : RowSetFactory.fromRange(k1, k3 - 1);
            added = RowSetFactory.fromRange(k2 + 1, k4);
        }
        return new TableUpdateImpl(added, removed, RowSetFactory.empty(), RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return ring.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return ring.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        ring.fillChunk(context, destination, rowSequence);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        prev.fillChunk(context, destination, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return prev.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getPrevChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return prev.getChunk(context, firstKey, lastKey);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public T get(long index) {
        return ring.get(index);
    }

    @Override
    public Boolean getBoolean(long index) {
        throw new UnsupportedOperationException(
                "No Boolean chunk source for RingColumnSource - use byte and reinterpret");
    }

    @Override
    public byte getByte(long index) {
        return ring.getByte(index);
    }

    @Override
    public char getChar(long index) {
        return ring.getChar(index);
    }

    @Override
    public double getDouble(long index) {
        return ring.getDouble(index);
    }

    @Override
    public float getFloat(long index) {
        return ring.getFloat(index);
    }

    @Override
    public int getInt(long index) {
        return ring.getInt(index);
    }

    @Override
    public long getLong(long index) {
        return ring.getLong(index);
    }

    @Override
    public short getShort(long index) {
        return ring.getShort(index);
    }

    @Override
    public T getPrev(long index) {
        return prev.get(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        throw new UnsupportedOperationException(
                "No Boolean chunk source for RingColumnSource - use byte and reinterpret");
    }

    @Override
    public byte getPrevByte(long index) {
        return prev.getByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        return prev.getChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        return prev.getDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        return prev.getFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        return prev.getInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        return prev.getLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        return prev.getShort(index);
    }
}
