//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;

public abstract class HashMapK4V4 extends HashMapBase {
    HashMapK4V4(int desiredInitialCapacity, double loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    final long putImpl(long[] kvs, long numBucketsReciprocal, long key, long value, boolean insertOnly) {
        if (kvs == null) {
            kvs = allocateKeysAndValuesArray(4);
            numBucketsReciprocal = reciprocalOf(kvs);
        }
        final long fixedKey = fixKey(key);
        return putImplNoTranslate(kvs, numBucketsReciprocal, fixedKey, value, insertOnly);
    }

    final long putImplNoTranslate(long[] kvs, long numBucketsReciprocal, long key, long value, boolean insertOnly) {
        int location = getLocationFor(kvs, key, numBucketsReciprocal);
        if (location >= 0) {
            // Item found, so replace it (unless 'insertOnly' is set).
            final long oldValue = kvs[location + 1];
            if (!insertOnly) {
                kvs[location + 1] = value;
            }
            return oldValue;
        }

        // Item not found, so insert it.
        location = -location - 1;
        ++size;
        checkSize(SIZE_LIMIT4);
        // The slot is either empty or removed. If we're about to consume an empty slot, then update our counter.
        if (kvs[location] == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            ++nonEmptySlots;
        }
        kvs[location] = key;
        kvs[location + 1] = value;

        // Did we run out of empty slots?
        if (nonEmptySlots >= rehashThreshold) {
            // This means we're low on empty slots. We might be low on empty slots because we've done a lot of
            // deletions of previous items (in this case 'size' could be small), or because we've done a lot of
            // insertions (in this case 'size' would be close to 'nonEmptySlots'). In the former case we would rather
            // rehash to the same size. In the latter case we would like to grow the hash table. The heuristic we use to
            // make this decision is if size exceeds 2/3 of the nonEmptySlots.
            boolean wantResize = size >= nonEmptySlots * 2 / 3;
            rehash(kvs, wantResize, 4);
        }

        return defaultReturnValue();
    }

    final long getImpl(long[] kvs, long numBucketsReciprocal, long key) {
        if (kvs == null) {
            return defaultReturnValue();
        }
        key = fixKey(key);
        final int location = getLocationFor(kvs, key, numBucketsReciprocal);
        if (location < 0) {
            return defaultReturnValue();
        }
        return kvs[location + 1];
    }

    final long removeImpl(long[] kvs, long numBucketsReciprocal, long key) {
        if (kvs == null) {
            return defaultReturnValue();
        }
        key = fixKey(key);
        final int location = getLocationFor(kvs, key, numBucketsReciprocal);
        if (location < 0) {
            return defaultReturnValue();
        }
        --size;
        kvs[location] = SPECIAL_KEY_FOR_DELETED_SLOT;
        return kvs[location + 1];
    }

    private static int getLocationFor(long[] kvs, long target, long numBucketsReciprocal) {
        // In units of longs, excluding the header
        final int dataLength = kvs.length - HEADER_LONGS;
        // In units of buckets
        final int numBuckets = dataLength / (4 * 2);

        final int bucketProbe = probe1(target, numBuckets, numBucketsReciprocal);
        // In units of longs again
        int probe = bucketProbe * (4 * 2);

        // Unroll this loop for probe + 0, 2, 4, 6.
        // If the key matches, return the probe (indicating an exact match).
        // If we hit an empty slot, return (-slot - 1) for the slot an insert should take: the earliest deleted slot
        // passed so far if there is one, else the empty slot itself — the same rule the loop below applies to every
        // later bucket. Remembering that slot costs one predictable compare per slot passed, on a hit in a later slot
        // as on a miss; measured against a form that compared only on reaching the empty slot, lookups came out
        // neutral to a few percent faster.
        int priorDeletedSlot;
        long cKey0 = kvs[probe];
        if (cKey0 == target) {
            return probe;
        }
        if (cKey0 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -probe - 1;
        }
        if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe;
        } else {
            priorDeletedSlot = -1;
        }

        long cKey1 = kvs[probe + 2];
        if (cKey1 == target) {
            return probe + 2;
        }
        if (cKey1 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            if (priorDeletedSlot != -1) {
                return -priorDeletedSlot - 1;
            }
            return -(probe + 2) - 1;
        }
        if (cKey1 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
            priorDeletedSlot = probe + 2;
        }

        long cKey2 = kvs[probe + 4];
        if (cKey2 == target) {
            return probe + 4;
        }
        if (cKey2 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            if (priorDeletedSlot != -1) {
                return -priorDeletedSlot - 1;
            }
            return -(probe + 4) - 1;
        }
        if (cKey2 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
            priorDeletedSlot = probe + 4;
        }

        long cKey3 = kvs[probe + 6];
        if (cKey3 == target) {
            return probe + 6;
        }
        if (cKey3 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            if (priorDeletedSlot != -1) {
                return -priorDeletedSlot - 1;
            }
            return -(probe + 6) - 1;
        }
        if (cKey3 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
            priorDeletedSlot = probe + 6;
        }

        // Offset is also in units of longs
        final int offset = (1 + probe2(target, numBuckets - 2)) * (4 * 2);
        final int probeStart = probe;
        while (true) {
            // offset < dataLength and probe < dataLength, so one conditional subtraction replaces the modulo.
            final long advanced = (long) probe + offset;
            probe = (int) (advanced >= dataLength ? advanced - dataLength : advanced);
            if (probe == probeStart) {
                throw new IllegalStateException("Wrapped around? Impossible.");
            }

            // Same logic as the above. Looking for the specific key and aborting if the empty slot is found. (But if
            // the empty slot is found, the insert takes the earliest deleted slot passed instead: one from an earlier
            // bucket, remembered in priorDeletedSlot, or else one earlier in this bucket, still in registers.)
            cKey0 = kvs[probe];
            if (cKey0 == target) {
                return probe;
            }
            if (cKey0 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
                if (priorDeletedSlot != -1) {
                    return -priorDeletedSlot - 1;
                }
                return -probe - 1;
            }
            if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
                priorDeletedSlot = probe;
            }

            cKey1 = kvs[probe + 2];
            if (cKey1 == target) {
                return probe + 2;
            }
            if (cKey1 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
                if (priorDeletedSlot != -1) {
                    return -priorDeletedSlot - 1;
                }
                return -(probe + 2) - 1;
            }
            if (cKey1 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
                priorDeletedSlot = probe + 2;
            }

            cKey2 = kvs[probe + 4];
            if (cKey2 == target) {
                return probe + 4;
            }
            if (cKey2 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
                if (priorDeletedSlot != -1) {
                    return -priorDeletedSlot - 1;
                }
                return -(probe + 4) - 1;
            }
            if (cKey2 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
                priorDeletedSlot = probe + 4;
            }

            cKey3 = kvs[probe + 6];
            if (cKey3 == target) {
                return probe + 6;
            }
            if (cKey3 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
                if (priorDeletedSlot != -1) {
                    return -priorDeletedSlot - 1;
                }
                return -(probe + 6) - 1;
            }
            if (cKey3 == SPECIAL_KEY_FOR_DELETED_SLOT && priorDeletedSlot == -1) {
                priorDeletedSlot = probe + 6;
            }
        }
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
     * Batch get via a rolling window of {@link HashMapK4V4#GET_WINDOW} in-flight lookups (AMAC style). Each job's turn
     * ends by loading the first and last keys of its next bucket into the stash; the values are consumed one full
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
