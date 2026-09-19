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
    public long put(long key, long value) {
        return putImpl(keysAndValues, key, value, false);
    }

    @Override
    public long putIfAbsent(long key, long value) {
        return putImpl(keysAndValues, key, value, true);
    }

    @Override
    public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        // Take the volatile read once: like every read operation, a chunked get sees one consistent snapshot of the
        // array. (getImpl tolerates a null snapshot.)
        final long[] localKvs = keysAndValues;
        final int size = keys.size();
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
