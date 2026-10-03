//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

public final class HashMapLockFreeK1V1 extends HashMapK1V1 implements NullableLongLongMapTestAccessors {
    private volatile long[] keysAndValues;

    public static HashMapLockFreeK1V1 ofExpectedSize(int expectedSize, double loadFactor, long noEntryValue) {
        final int desiredInitialCapacity = capacityForExpectedEntries(expectedSize, loadFactor);
        return new HashMapLockFreeK1V1(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    public HashMapLockFreeK1V1() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    public HashMapLockFreeK1V1(int desiredInitialCapacity) {
        this(desiredInitialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK1V1(int desiredInitialCapacity, double loadFactor) {
        this(desiredInitialCapacity, loadFactor, DEFAULT_NO_ENTRY_VALUE);
    }

    public HashMapLockFreeK1V1(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
        this.keysAndValues = null;
    }

    @Override
    void setKeysAndValues(long[] keysAndValues) {
        this.keysAndValues = keysAndValues;
    }

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        final int size = keys.size();
        for (int ii = 0; ii < size; ++ii) {
            // Unlike get, the volatile read is NOT hoisted: any put may rehash, so each element must see the array
            // that the previous element may have replaced.
            oldValues.set(ii, putImpl(keysAndValues, keys.get(ii), values.get(ii), false));
        }
        oldValues.setSize(size);
    }

    @Override
    public void putIfAbsent(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        final int size = keys.size();
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, putImpl(keysAndValues, keys.get(ii), values.get(ii), true));
        }
        oldValues.setSize(size);
    }

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values) {
        final int size = keys.size();
        for (int ii = 0; ii < size; ++ii) {
            // As above: the volatile read is not hoisted, because any put may rehash.
            putImpl(keysAndValues, keys.get(ii), values.get(ii), false);
        }
    }

    @Override
    public void put(LongChunk<? extends Any> keys, long value) {
        final int size = keys.size();
        for (int ii = 0; ii < size; ++ii) {
            putImpl(keysAndValues, keys.get(ii), value, false);
        }
    }

    @Override
    public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        // Take the volatile read once: like every read operation, a chunked get sees one consistent snapshot of the
        // array.
        final long[] localKvs = keysAndValues;
        final int size = keys.size();
        if (localKvs == null) {
            // Never populated, or reset: every key is a miss, and we need not probe to know it.
            result.fillWithValue(0, size, defaultReturnValue());
            result.setSize(size);
            return;
        }
        for (int ii = 0; ii < size; ++ii) {
            result.set(ii, getImpl(localKvs, keys.get(ii)));
        }
        result.setSize(size);
    }

    @Override
    public long remove(long key) {
        return removeImpl(keysAndValues, key);
    }

    public int capacity() {
        return capacityImpl(keysAndValues);
    }

    @Override
    public void clear() {
        clearImpl(keysAndValues);
    }

    @Override
    public void resetToNull() {
        resetToNullImpl();
        keysAndValues = null;
    }

    @Override
    public void resetToNullRetainingCapacity() {
        resetToNullRetainingCapacityImpl(keysAndValues);
        keysAndValues = null;
    }

    @Override
    public long[] keyArray() {
        return keysOrValuesImpl(keysAndValues, null, false);
    }

    @Override
    public long[] keyArray(long[] space) {
        return keysOrValuesImpl(keysAndValues, space, false);
    }

    @Override
    public long[] valueArray() {
        return keysOrValuesImpl(keysAndValues, null, true);
    }

    @Override
    public long[] valueArray(long[] space) {
        return keysOrValuesImpl(keysAndValues, space, true);
    }

    @Override
    public void forEach(LongLongBiConsumer consumer) {
        forEachImpl(keysAndValues, consumer);
    }
}
