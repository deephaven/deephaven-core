//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;

public final class HashMapLockFreeK2V2 extends HashMapK2V2 {
    private volatile long[] keysAndValues;

    public HashMapLockFreeK2V2() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    public HashMapLockFreeK2V2(int desiredInitialCapacity) {
        this(desiredInitialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK2V2(int desiredInitialCapacity, float loadFactor) {
        this(desiredInitialCapacity, loadFactor, DEFAULT_NO_ENTRY_VALUE);
    }

    public HashMapLockFreeK2V2(int desiredInitialCapacity, float loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
        this.keysAndValues = null;
    }

    @Override
    protected void setKeysAndValues(long[] keysAndValues) {
        this.keysAndValues = keysAndValues;
    }

    @Override
    public final long put(long key, long value) {
        return putImpl(keysAndValues, key, value, false);
    }

    @Override
    public final long putIfAbsent(long key, long value) {
        return putImpl(keysAndValues, key, value, true);
    }

    @Override
    public final long get(long key) {
        return getImpl(keysAndValues, key);
    }

    @Override
    public final long remove(long key) {
        return removeImpl(keysAndValues, key);
    }

    public final int capacity() {
        return capacityImpl(keysAndValues);
    }

    @Override
    public final void clear() {
        clearImpl(keysAndValues);
    }

    public final void resetToNull() {
        resetToNullImpl();
        keysAndValues = null;
    }

    @Override
    public final ObjectSet<Long2LongMap.Entry> long2LongEntrySet() {
        return entrySetImpl(keysAndValues);
    }

    @Override
    public final LongSet keySet() {
        return keySetImpl(keysAndValues);
    }

    @Override
    public final LongCollection values() {
        return valuesImpl(keysAndValues);
    }
}
