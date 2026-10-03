//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The K2V2 implementation of {@link NullableLongLongMap}: each hash bucket holds two keys followed by their two values.
 * The concrete type is an implementation detail — callers construct maps through {@link NullableLongLongMaps} (naming
 * {@link NullableLongLongMaps.Shape#K2V2}) and hold the interface.
 */
final class HashMapLockFreeK2V2 extends HashMapK2V2 implements NullableLongLongMapTestAccessors {
    private volatile long[] keysAndValues;

    HashMapLockFreeK2V2(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
        this.keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    @Override
    void setKeysAndValues(long[] keysAndValues) {
        this.keysAndValues = keysAndValues;
    }

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        final int size = keys.size();
        // Unlike get, the volatile read is NOT hoisted: any put may rehash, so each element must see the array
        // that the previous element may have replaced. The reciprocal rides in a register-local memo, refreshed
        // from the new array's own header whenever the array changes (a load, not a divide: every array carries
        // its reciprocal).
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), false));
            // Hot reads: cheap, and free of a stale-check branch (the array is never null).
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
        oldValues.setSize(size);
    }

    @Override
    public void putIfAbsent(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        final int size = keys.size();
        // Same volatile-read and reciprocal-memo discipline as put.
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), true));
            // Hot reads: cheap, and free of a stale-check branch (the array is never null).
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
        oldValues.setSize(size);
    }

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values) {
        final int size = keys.size();
        // As above: the array is re-read per element, because any put may rehash; the reciprocal rides along.
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), false);
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    @Override
    public void put(LongChunk<? extends Any> keys, long value) {
        final int size = keys.size();
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            putImpl(kvs, numBucketsReciprocal, keys.get(ii), value, false);
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    @Override
    public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        // Take the volatile read once: like every read operation, a chunked get sees one consistent snapshot of the
        // array.
        final long[] localKvs = keysAndValues;
        final int size = keys.size();
        if (isEmptyArray(localKvs)) {
            // The empty sentinel: never populated, or reset. Every key is a miss, and we need not probe to know it.
            result.fillWithValue(0, size, defaultReturnValue());
            result.setSize(size);
            return;
        }
        // The reciprocal comes from the snapshot's own header — published with the array and immutable
        // thereafter, so it cannot tear against it.
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        for (int ii = 0; ii < size; ++ii) {
            result.set(ii, getImpl(localKvs, numBucketsReciprocal, keys.get(ii)));
        }
        result.setSize(size);
    }

    @Override
    public void remove(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> oldValues) {
        // Like get (and unlike put), the volatile read is hoisted: removeImpl tombstones slots in place and never
        // rehashes, so no element can replace the array a later element must see.
        final long[] localKvs = keysAndValues;
        // Same header-borne reciprocal as get.
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        final int size = keys.size();
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, removeImpl(localKvs, numBucketsReciprocal, keys.get(ii)));
        }
        oldValues.setSize(size);
    }

    public int capacity() {
        return capacityImpl(keysAndValues);
    }

    @Override
    public void clear() {
        clearImpl(keysAndValues);
    }

    public void resetToNull() {
        resetToNullImpl();
        keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    @Override
    public void resetToNullRetainingCapacity() {
        resetToNullRetainingCapacityImpl(keysAndValues);
        keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    @Override
    public long[] keysAndValuesSnapshot() {
        return keysAndValues;
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
