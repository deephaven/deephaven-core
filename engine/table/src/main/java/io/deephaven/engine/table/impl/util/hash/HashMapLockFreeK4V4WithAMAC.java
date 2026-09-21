//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The K4V4WithAMAC implementation of {@link NullableLongLongMap}: the K4V4 map (each hash bucket holds four keys
 * followed by their four values) whose chunked gets are serviced through a rolling window of in-flight lookups ("AMAC":
 * Asynchronous Memory Access Chaining), so the cache misses of up to {@link #GET_WINDOW} independent probes overlap
 * instead of serializing. Writes are identical to K4V4: the interface processes elements in index order, and
 * duplicate-key semantics depend on it; reads are pure, so the window may resolve lookups out of index order, invisibly
 * to the caller.
 *
 * <p>
 * Profile: the window pays when misses dominate — large, dense (high-load-factor) tables — and is pure bookkeeping
 * overhead when the table is cache-resident, where the serial implementations are measurably faster. Choose it for maps
 * that live at high occupancy; a future change will cut dense maps over to this shape at rehash time.
 *
 * <p>
 * The concrete type is an implementation detail — callers construct maps through the static factories and hold the
 * interface. The factory is the seam where implementation choice lives (and where, in a future change, a map may choose
 * or change its own shape).
 */
public final class HashMapLockFreeK4V4WithAMAC extends HashMapK4V4 implements NullableLongLongMapTestAccessors {
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
        return new HashMapLockFreeK4V4WithAMAC(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    HashMapLockFreeK4V4WithAMAC() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4WithAMAC(int desiredInitialCapacity) {
        this(desiredInitialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4WithAMAC(int desiredInitialCapacity, double loadFactor) {
        this(desiredInitialCapacity, loadFactor, DEFAULT_NO_ENTRY_VALUE);
    }

    HashMapLockFreeK4V4WithAMAC(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
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
        // Unlike get, the volatile read is NOT hoisted: any put may rehash, so each element must see the array
        // that the previous element may have replaced. The reciprocal rides in a register-local memo, refreshed
        // from the new array's own header whenever the array changes (a load, not a divide: every array carries
        // its reciprocal).
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            putImpl(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), false);
            // Hot reads: cheap, and free of a stale-check branch; kvs is non-null once putImpl has run.
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    @Override
    public void put(LongChunk<? extends Any> keys, long value) {
        final int size = keys.size();
        // Unlike get, the volatile read is NOT hoisted: any put may rehash, so each element must see the array
        // that the previous element may have replaced. The reciprocal rides in a register-local memo, refreshed
        // from the new array's own header whenever the array changes (a load, not a divide: every array carries
        // its reciprocal).
        long[] kvs = keysAndValues;
        long numBucketsReciprocal = kvs == null ? 0 : reciprocalOf(kvs);
        for (int ii = 0; ii < size; ++ii) {
            putImpl(kvs, numBucketsReciprocal, keys.get(ii), value, false);
            // Hot reads: cheap, and free of a stale-check branch; kvs is non-null once putImpl has run.
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    @Override
    public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        // Take the volatile read once: like every read operation, a chunked get sees one consistent snapshot of the
        // array, whose header carries its reciprocal. Reads are pure, so the windowed implementation may resolve
        // lookups out of index order — invisible to the caller. (Mutators stay in index order: duplicate-key
        // semantics depend on it.)
        final long[] localKvs = keysAndValues;
        final int size = keys.size();
        if (localKvs == null) {
            final long noEntry = defaultReturnValue();
            for (int ii = 0; ii < size; ++ii) {
                result.set(ii, noEntry);
            }
            result.setSize(size);
            return;
        }
        getBatchImpl(localKvs, reciprocalOf(localKvs), keys, result);
        result.setSize(size);
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

    /**
     * Number of in-flight lookups in the batch-get window. Sized to the memory-level parallelism a single core can
     * sustain (typically 10-16 outstanding L1 misses); raising it past that buys nothing and costs bookkeeping.
     */
    static final int GET_WINDOW = 16;

    /**
     * Reusable per-thread scratch for {@link #getBatchImpl}: the rolling window's per-job state. One static instance
     * per thread for the whole class (not per map): the state is fixed-size and map-independent, so this is O(threads)
     * storage that any map of this shape may share.
     */
    private static final class GetWindow {
        final int[] jobSlot = new int[GET_WINDOW];
        final long[] jobKey = new long[GET_WINDOW];
        final int[] jobProbe = new int[GET_WINDOW];
        final int[] jobProbeStart = new int[GET_WINDOW];
        final int[] jobOffset = new int[GET_WINDOW];
        final long[] stashedKey0 = new long[GET_WINDOW];
        final long[] stashedKeyLast = new long[GET_WINDOW];
    }

    private static final ThreadLocal<GetWindow> GET_WINDOW_STATE = ThreadLocal.withInitial(GetWindow::new);

    /**
     * Batch get via a rolling window of {@link #GET_WINDOW} in-flight lookups (AMAC style). Each job's turn ends by
     * loading the first and last keys of its next bucket into the stash; the values are consumed one full
     * window-rotation later, by which time the cache lines have typically arrived — so up to a window's worth of misses
     * are serviced concurrently instead of serially. Tombstones need no bookkeeping here: a lookup just probes past
     * them, and only insertion cares where they are.
     */
    final void getBatchImpl(long[] kvs, long numBucketsReciprocal, LongChunk<? extends Any> keys,
            WritableLongChunk<? extends Any> result) {
        final int n = keys.size();
        final int dataLength = kvs.length - HEADER_LONGS;
        final int numBuckets = dataLength / (4 * 2);
        final long noEntry = defaultReturnValue();
        final int window = Math.min(GET_WINDOW, n);
        final GetWindow state = GET_WINDOW_STATE.get();
        final int[] jobSlot = state.jobSlot;
        final long[] jobKey = state.jobKey;
        final int[] jobProbe = state.jobProbe;
        final int[] jobProbeStart = state.jobProbeStart;
        final int[] jobOffset = state.jobOffset;
        final long[] stashedKey0 = state.stashedKey0;
        final long[] stashedKeyLast = state.stashedKeyLast;
        int next = 0;
        int active = 0;
        for (int w = 0; w < window; ++w) {
            final long target = fixKey(keys.get(next));
            final int probe = probe1(target, numBuckets, numBucketsReciprocal) * (4 * 2);
            jobSlot[w] = next;
            jobKey[w] = target;
            jobProbe[w] = probe;
            jobProbeStart[w] = probe;
            jobOffset[w] = 0;
            stashedKey0[w] = kvs[probe];
            stashedKeyLast[w] = kvs[probe + 6];
            ++next;
            ++active;
        }
        int w = -1;
        while (active > 0) {
            w = w + 1 == window ? 0 : w + 1;
            final int slot = jobSlot[w];
            if (slot < 0) {
                continue;
            }
            final long target = jobKey[w];
            final int probe = jobProbe[w];
            final long k0 = stashedKey0[w];
            final long k3 = stashedKeyLast[w];
            // Entries 1 and 2 sit between entry 0 and entry 3, so their cache lines were warmed by the stashes.
            final long k1 = kvs[probe + 2];
            final long k2 = kvs[probe + 4];
            final long value;
            if (k0 == target) {
                value = kvs[probe + 1];
            } else if (k1 == target) {
                value = kvs[probe + 3];
            } else if (k2 == target) {
                value = kvs[probe + 5];
            } else if (k3 == target) {
                value = kvs[probe + 7];
            } else if (k0 == SPECIAL_KEY_FOR_EMPTY_SLOT || k1 == SPECIAL_KEY_FOR_EMPTY_SLOT
                    || k2 == SPECIAL_KEY_FOR_EMPTY_SLOT || k3 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
                value = noEntry;
            } else {
                // No match, no empty slot: advance to the next bucket and yield this job's turn.
                int offset = jobOffset[w];
                if (offset == 0) {
                    offset = (1 + probe2(target, numBuckets - 2)) * (4 * 2);
                    jobOffset[w] = offset;
                }
                final long advanced = (long) probe + offset;
                final int nextProbe = (int) (advanced >= dataLength ? advanced - dataLength : advanced);
                if (nextProbe == jobProbeStart[w]) {
                    throw new IllegalStateException("Wrapped around? Impossible.");
                }
                jobProbe[w] = nextProbe;
                stashedKey0[w] = kvs[nextProbe];
                stashedKeyLast[w] = kvs[nextProbe + 6];
                continue;
            }
            result.set(slot, value);
            if (next < n) {
                final long newTarget = fixKey(keys.get(next));
                final int newProbe = probe1(newTarget, numBuckets, numBucketsReciprocal) * (4 * 2);
                jobSlot[w] = next;
                jobKey[w] = newTarget;
                jobProbe[w] = newProbe;
                jobProbeStart[w] = newProbe;
                jobOffset[w] = 0;
                stashedKey0[w] = kvs[newProbe];
                stashedKeyLast[w] = kvs[newProbe + 6];
                ++next;
            } else {
                jobSlot[w] = -1;
                --active;
            }
        }
    }
}
