//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.ReadMode;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

import java.util.Objects;

/**
 * The K4V4 implementation of {@link NullableLongLongMap}: each hash bucket holds four keys followed by their four
 * values. The concrete type is an implementation detail — callers construct maps through {@link NullableLongLongMaps}
 * (naming {@link NullableLongLongMaps.Shape#K4V4}) and hold the interface.
 */
final class HashMapLockFreeK4V4 extends HashMapK4V4 implements NullableLongLongMapTestAccessors {
    private volatile long[] keysAndValues;
    private final ReadMode readMode;

    HashMapLockFreeK4V4(int desiredInitialCapacity, double loadFactor, long noEntryValue, ReadMode readMode) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
        this.readMode = Objects.requireNonNull(readMode, "readMode");
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
        // array, whose header carries its reciprocal.
        final long[] localKvs = keysAndValues;
        final int n = keys.size();
        // Adaptive read strategy: when the map's footprint is past the measured crossover (near L2; see
        // NullableLongLongMaps.wantWindowedReads) — the window's whole job is overlapping the misses that a table
        // resident in the near caches simply does not have — service the chunk through the AMAC window; otherwise use
        // the serial loop, which ties or wins when the table fits those caches. Footprint is a function of the
        // snapshot's own length, so the choice is stable between rehashes and flips exactly when the array grows past
        // the crossover. (Occupancy is deliberately not consulted; see wantWindowedReads.) The chunk must also be
        // wide enough to fill the window: its fixed cost is paid per call, and a single-key chunk — the scalar
        // cursor's case — has nothing to overlap, measured at 1.6-2.3x slower under the window. A pinned ReadMode
        // overrides the gate, for pricing and tests only. Reads are pure, so the windowed path may resolve lookups
        // out of index order, invisibly to the caller.
        final boolean windowed = readMode == ReadMode.ADAPTIVE
                ? NullableLongLongMaps.wantWindowedReads((localKvs.length - HEADER_LONGS) / 2, n)
                : readMode == ReadMode.WINDOW;
        if (windowed) {
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
