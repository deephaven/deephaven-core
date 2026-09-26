//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.HEADER_LONGS;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SIZE_LIMIT1;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SPECIAL_KEY_FOR_DELETED_SLOT;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.SPECIAL_KEY_FOR_EMPTY_SLOT;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.fixKey;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.probe1;
import static io.deephaven.engine.table.impl.util.hash.HashMapLockFreeKnVn.probe2;

/**
 * The probe loops for arrays whose buckets hold one key and one value ({@link NullableLongLongMaps.Shape#K1V1}).
 * Static, and pure in the array plus the owning map's counters: {@link HashMapLockFreeKnVn} dispatches here on a
 * snapshot's shape tag, so nothing in this class knows or cares which shape a map was born with.
 */
final class K1V1Kernel {
    private K1V1Kernel() {}

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
        map.checkSize(SIZE_LIMIT1);
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
        final int numBuckets = dataLength / (1 * 2);

        final int bucketProbe = probe1(target, numBuckets, numBucketsReciprocal);
        // In units of longs again
        int probe = bucketProbe * (1 * 2);

        // Unroll this loop for probe + 0, 2
        // If the key matches, return the probe (indicating an exact match).
        // If we hit an empty slot, return (-probe - 1), indicating empty slot reached at probe.
        long cKey0 = kvs[probe];
        if (cKey0 == target) {
            return probe;
        }
        if (cKey0 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -probe - 1;
        }

        // These slots might also have been deleted slots. If so, we need to keep searching (until key found or the
        // first empty slot), but we remember the first deleted slot.
        int priorDeletedSlot;
        if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe;
        } else {
            priorDeletedSlot = -1;
        }

        // Offset is also in units of longs
        final int offset = (1 + probe2(target, numBuckets - 2)) * (1 * 2);
        final int probeStart = probe;
        while (true) {
            // offset < dataLength and probe < dataLength, so one conditional subtraction replaces the modulo.
            final long advanced = (long) probe + offset;
            probe = (int) (advanced >= dataLength ? advanced - dataLength : advanced);
            if (probe == probeStart) {
                throw new IllegalStateException("Wrapped around? Impossible.");
            }

            // Same logic as the above. Looking for the specific key and aborting if the empty slot is found.
            // (But, if the empty slot is found, and if there was an earlier deleted slot, we need to return the
            // earlier deleted slot)
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

            if (priorDeletedSlot == -1) {
                if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT) {
                    priorDeletedSlot = probe;
                }
            }
        }
    }
}
