//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The K4V4 implementation of {@link NullableLongLongMap}: each hash bucket holds four keys followed by their four
 * values. The concrete type is an implementation detail — callers construct maps through the static factories and hold
 * the interface. The factory is the seam where implementation choice lives (and where, in a future change, a map may
 * choose or change its own shape).
 */
public final class HashMapLockFreeK4V4 extends HashMapK4V4 implements NullableLongLongMapTestAccessors {
    private volatile long[] keysAndValues;

    /**
     * Creates a map presized so that {@code expectedSize} entries at {@code loadFactor} fit without a rehash.
     */
    public static NullableLongLongMap ofExpectedSize(int expectedSize, double loadFactor, long noEntryValue) {
        final int desiredInitialCapacity = capacityForExpectedEntries(expectedSize, loadFactor);
        return of(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    /**
     * Creates a map with the given initial capacity, load factor, and noEntryValue (the value returned by reads that
     * find no mapping).
     */
    public static NullableLongLongMap of(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
        return new HashMapLockFreeK4V4(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    HashMapLockFreeK4V4() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4(int desiredInitialCapacity) {
        this(desiredInitialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4(int desiredInitialCapacity, double loadFactor) {
        this(desiredInitialCapacity, loadFactor, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
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
        // Unlike get, the volatile read is NOT hoisted: any put may rehash, so each element must see the array
        // that the previous element may have replaced. The reciprocal rides in a register-local memo, refreshed
        // from the new array's own header whenever the array changes (a load, not a divide: every array carries
        // its reciprocal).
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), false));
            // Hot reads: cheap, and free of a stale-check branch; kvs is non-null once putImpl has run.
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
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            oldValues.set(ii, putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), true));
            // Hot reads: cheap, and free of a stale-check branch; kvs is non-null once putImpl has run.
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
        // array, whose header carries its reciprocal.
        final long[] localKvs = keysAndValues;
        final int n = keys.size();
        if (localKvs == null) {
            result.fillWithValue(0, n, defaultReturnValue());
            result.setSize(n);
            return;
        }
        // Adaptive read strategy: when the map's footprint is beyond the last-level cache — its whole job is
        // overlapping the misses that a cache-resident table simply does not have — service the chunk through the
        // AMAC window; otherwise use the serial loop, which ties or wins when the table is cache-resident. Footprint
        // is a function of the snapshot's own length, so the choice is stable between rehashes and flips exactly when
        // the array grows past the cache. (Occupancy is deliberately not consulted; see wantWindowedReads.) Reads are
        // pure, so the windowed path may resolve lookups out of index order, invisibly to the caller.
        if (NullableLongLongMaps.wantWindowedReads((localKvs.length - HEADER_LONGS) / 2)) {
            getBatchImpl(localKvs, reciprocalOf(localKvs), keys, result);
        } else {
            final long numBucketsReciprocal = reciprocalOf(localKvs);
            for (int ii = 0; ii < n; ++ii) {
                result.set(ii, getImpl(localKvs, numBucketsReciprocal, keys.get(ii)));
            }
        }
        result.setSize(n);
    }

    @Override
    public void remove(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> oldValues) {
        // Like get (and unlike put), the volatile read is hoisted: removeImpl tombstones slots in place and never
        // rehashes, so no element can replace the array a later element must see.
        final long[] localKvs = keysAndValues;
        // Same header-borne reciprocal as get.
        final long numBucketsReciprocal = localKvs == null ? 0 : reciprocalOf(localKvs);
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
