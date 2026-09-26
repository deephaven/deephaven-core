//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.HEADER_LONGS;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SIZE_LIMIT2;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SPECIAL_KEY_FOR_DELETED_SLOT;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SPECIAL_KEY_FOR_EMPTY_SLOT;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.fixKey;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.probe1;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.probe2;

/**
 * The probe loops for arrays whose buckets hold two keys followed by two values
 * ({@link NullableLongLongMaps.Shape#K2V2}). Static, and pure in the array plus the owning map's counters:
 * {@link HashMapLockFreeKnVn} dispatches here on a snapshot's shape tag, so nothing in this class knows or cares which
 * shape a map was born with.
 */
final class K2V2Kernel {
    private K2V2Kernel() {}

    static long put(HashMapLockFreeKnVn map, long[] kvs, long numBucketsReciprocal, long key, long value,
            boolean insertOnly) {
        final long fixedKey = fixKey(key);
        return putNoTranslate(map, kvs, numBucketsReciprocal, fixedKey, value, insertOnly);
    }

    static long putNoTranslate(HashMapLockFreeKnVn map, long[] kvs, long numBucketsReciprocal, long key, long value,
            boolean insertOnly) {
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
        ++map.size;
        map.checkSize(SIZE_LIMIT2);
        // The slot is either empty or removed. If we're about to consume an empty slot, then update our counter.
        if (kvs[location] == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            ++map.nonEmptySlots;
        }
        kvs[location] = key;
        kvs[location + 1] = value;

        // Did we run out of empty slots?
        if (map.nonEmptySlots >= map.rehashThreshold) {
            // This means we're low on empty slots. We might be low on empty slots because we've done a lot of
            // deletions of previous items (in this case 'size' could be small), or because we've done a lot of
            // insertions (in this case 'size' would be close to 'nonEmptySlots'). In the former case we would rather
            // rehash to the same size. In the latter case we would like to grow the hash table. The heuristic we use to
            // make this decision is if size exceeds 2/3 of the nonEmptySlots.
            boolean wantResize = map.size >= map.nonEmptySlots * 2 / 3;
            map.rehash(kvs, wantResize);
        }

        return map.defaultReturnValue();
    }

    static long get(long[] kvs, long numBucketsReciprocal, long key, long noEntry) {
        key = fixKey(key);
        final int location = getLocationFor(kvs, key, numBucketsReciprocal);
        if (location < 0) {
            return noEntry;
        }
        return kvs[location + 1];
    }

    static long remove(HashMapLockFreeKnVn map, long[] kvs, long numBucketsReciprocal, long key) {
        key = fixKey(key);
        final int location = getLocationFor(kvs, key, numBucketsReciprocal);
        if (location < 0) {
            return map.defaultReturnValue();
        }
        --map.size;
        kvs[location] = SPECIAL_KEY_FOR_DELETED_SLOT;
        return kvs[location + 1];
    }

    private static int getLocationFor(long[] kvs, long target, long numBucketsReciprocal) {
        // In units of longs, excluding the header
        final int dataLength = kvs.length - HEADER_LONGS;
        // In units of buckets
        final int numBuckets = dataLength / (2 * 2);

        final int bucketProbe = probe1(target, numBuckets, numBucketsReciprocal);
        // In units of longs again
        int probe = bucketProbe * (2 * 2);

        // Unroll this loop for probe + 0, 2
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

        // Offset is also in units of longs
        final int offset = (1 + probe2(target, numBuckets - 2)) * (2 * 2);
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
        }
    }
}
