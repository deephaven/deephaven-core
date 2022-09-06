/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.datastructures.hash;

public abstract class HashMapK4V4 extends HashMapBase {
    HashMapK4V4(int desiredInitialCapacity, float loadFactor, long noEntryValue) {
        super(desiredInitialCapacity, loadFactor, noEntryValue);
    }

    final long putImpl(long[] kvs, long key, long value, boolean insertOnly) {
        if (kvs == null) {
            kvs = allocateKeysAndValuesArray(4);
        }
        final long fixedKey = fixKey(key);
        return putImplNoTranslate(kvs, fixedKey, value, insertOnly);
    }

    protected final long putImplNoTranslate(long[] kvs, long key, long value, boolean insertOnly) {
        // To minimize possible painful effects of nonsynchronized access to our array, we get the reference once.
        int location = getLocationFor(kvs, key);
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

        return noEntryValue;
    }

    final long getImpl(long[] kvs, long key) {
        if (kvs == null) {
            return noEntryValue;
        }
        key = fixKey(key);
        // To minimize possible painful effects of nonsynchronized access to our array, we get the reference once.
        final int location = getLocationFor(kvs, key);
        if (location < 0) {
            return noEntryValue;
        }
        return kvs[location + 1];
    }

    final long removeImpl(long[] kvs, long key) {
        if (kvs == null) {
            return noEntryValue;
        }
        key = fixKey(key);
        // To minimize possible painful effects of nonsynchronized access to our array, we get the reference once.
        final int location = getLocationFor(kvs, key);
        if (location < 0) {
            return noEntryValue;
        }
        --size;
        kvs[location] = SPECIAL_KEY_FOR_DELETED_SLOT;
        return kvs[location + 1];
    }

    private static int getLocationFor(long[] kvs, long target) {
        // In units of longs
        final int length = kvs.length;
        // In units of buckets
        final int numBuckets = length / (4 * 2);

        final int bucketProbe = probe1(target, numBuckets);
        // In units of longs again
        int probe = bucketProbe * (4 * 2);

        // Unroll this loop for probe + 0, 2, 4, 6.
        // If the key matches, return the probe (indicating an exact match).
        // If we hit an empty slot, return (-probe - 1), indicating empty slot reached at probe.
        long cKey0 = kvs[probe];
        if (cKey0 == target) {
            return probe;
        }
        if (cKey0 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -probe - 1;
        }
        long cKey1 = kvs[probe + 2];
        if (cKey1 == target) {
            return probe + 2;
        }
        if (cKey1 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -(probe + 2) - 1;
        }
        long cKey2 = kvs[probe + 4];
        if (cKey2 == target) {
            return probe + 4;
        }
        if (cKey2 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -(probe + 4) - 1;
        }
        long cKey3 = kvs[probe + 6];
        if (cKey3 == target) {
            return probe + 6;
        }
        if (cKey3 == SPECIAL_KEY_FOR_EMPTY_SLOT) {
            return -(probe + 6) - 1;
        }

        // These slots might also have been deleted slots. If so, we need to keep searching (until key found or the
        // first empty slot), but we remember the first deleted slot.
        int priorDeletedSlot;
        if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe;
        } else if (cKey1 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe + 2;
        } else if (cKey2 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe + 4;
        } else if (cKey3 == SPECIAL_KEY_FOR_DELETED_SLOT) {
            priorDeletedSlot = probe + 6;
        } else {
            priorDeletedSlot = -1;
        }

        // Offset is also in units of longs
        final int offset = (1 + probe2(target, numBuckets - 2)) * (4 * 2);
        final int probeStart = probe;
        while (true) {
            probe = (int) (((long) probe + offset) % length);
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

            if (priorDeletedSlot == -1) {
                if (cKey0 == SPECIAL_KEY_FOR_DELETED_SLOT) {
                    priorDeletedSlot = probe;
                } else if (cKey1 == SPECIAL_KEY_FOR_DELETED_SLOT) {
                    priorDeletedSlot = probe + 2;
                } else if (cKey2 == SPECIAL_KEY_FOR_DELETED_SLOT) {
                    priorDeletedSlot = probe + 4;
                } else if (cKey3 == SPECIAL_KEY_FOR_DELETED_SLOT) {
                    priorDeletedSlot = probe + 6;
                }
            }
        }
    }
}
