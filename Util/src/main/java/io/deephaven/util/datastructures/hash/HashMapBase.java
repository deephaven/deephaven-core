//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.hash;

import io.deephaven.base.verify.Assert;
import io.deephaven.hash.PrimeFinder;
import it.unimi.dsi.fastutil.longs.AbstractLong2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class HashMapBase implements NullableLong2LongMap {
    static final int DEFAULT_INITIAL_CAPACITY = 10;
    static final long DEFAULT_NO_ENTRY_VALUE = -1;
    static final float DEFAULT_LOAD_FACTOR = 0.5f;

    // There are three "special keys" removed from the range of valid keys that are used to represent various slot
    // states:
    // 1. SPECIAL_KEY_FOR_EMPTY_SLOT is used to represent a slot that has never been used.
    // 2. SPECIAL_KEY_FOR_DELETED_SLOT is used to represent a slot that was once in use, but the key that was formerly
    // present there has been deleted.
    // 3. NULL_LONG is used to represent the null key.
    //
    // These values must all be distinct.
    //
    // For the sake of efficiency, we define SPECIAL_KEY_FOR_EMPTY_SLOT to be 0. This means that our arrays are ready to
    // go after being allocated, and we don't need to fill them with a special value. However, we are aware that our
    // callers may wish to use 0 as a key. To support this, we remap key=0 on get, put, remove, and iteration operations
    // to our special value called REDIRECTED_KEY_FOR_EMPTY_SLOT.
    static final long SPECIAL_KEY_FOR_EMPTY_SLOT = 0;
    static final long REDIRECTED_KEY_FOR_EMPTY_SLOT = NULL_LONG + 1;
    static final long SPECIAL_KEY_FOR_DELETED_SLOT = NULL_LONG + 2;

    /**
     * This is the load factor we use as the hashtable nears its maximum size, in order to try to keep functioning
     * (albeit with reduced performance) rather than failing.
     */
    private static final float NEARLY_FULL_LOAD_FACTOR = 0.9f;

    /**
     * This is the fraction of the maximum possible size at which we just give up and throw an exception. It is kept
     * slightly smaller than the NEARLY_FULL_LOAD_FACTOR (otherwise, we might end up in a situation where every put
     * caused a rehash). Additionally, for some reason K2V2 is much less tolerant of getting full than the other two (it
     * gets very slow as it approaches the max). For this reason, until we figure it out, we maintain individual size
     * factors for each KnVn.
     */
    private static final float SIZE_LIMIT_FACTOR1 = 0.85f;
    private static final float SIZE_LIMIT_FACTOR2 = 0.75f;
    private static final float SIZE_LIMIT_FACTOR4 = 0.85f;
    /**
     * This is the size at which we just give up and throw an exception rather than do a new put. It is number of
     * entries (aka number of longs / 2) * SIZE_LIMIT_FACTORn.
     */
    static final int SIZE_LIMIT1 = (int) (Integer.MAX_VALUE / 2 * SIZE_LIMIT_FACTOR1);
    static final int SIZE_LIMIT2 = (int) (Integer.MAX_VALUE / 2 * SIZE_LIMIT_FACTOR2);
    static final int SIZE_LIMIT4 = (int) (Integer.MAX_VALUE / 2 * SIZE_LIMIT_FACTOR4);

    static {
        // All the "SPECIAL_" values need to be unique. This is one way to check this easily.
        HashSet<Long> hs = new HashSet<>();
        hs.add(NULL_LONG);
        hs.add(SPECIAL_KEY_FOR_EMPTY_SLOT);
        hs.add(REDIRECTED_KEY_FOR_EMPTY_SLOT);
        hs.add(SPECIAL_KEY_FOR_DELETED_SLOT);
        Assert.eq(hs.size(), "hs.size()", 4, "4");
    }

    private final int desiredInitialCapacity;
    private final float loadFactor;
    private final long noEntryValue;
    // There are three kinds of slots: empty, holding a value, and deleted (formerly holding a value).
    // 'size' is the number of slots holding a value.
    int size;
    // 'nonEmptySlots' is the number of slots either holding a value or deleted. It is an invariant that
    // nonEmptySlots >= size.
    int nonEmptySlots;
    // The threshold (generally loadFactor * capacity) at which a rehash is triggered. This happens when nonEmptySlots
    // meets or exceeds rehashThreshold. There is a decision to make about whether to rehash at the same capacity or a
    // larger capacity. The heuristic we use is that if size >= (2/3) * nonEmptySlots we rehash to a larger capacity.
    int rehashThreshold;
    // In various places in the code, we will be dealing with three kinds of units:
    // - How many buckets in the array (this is always a prime number)
    // - How many entries in the array (at 4 entries per bucket, this is numBuckets * 4)
    // - How many longs in the array (at 2 longs per entry (key and value), this is numEntries * 2)
    // The actual array of longs (with length (numBuckets * 4 * 2)) is stored in our child.

    HashMapBase(int desiredInitialCapacity, float loadFactor, long noEntryValue) {
        this.desiredInitialCapacity = desiredInitialCapacity;
        this.loadFactor = loadFactor;
        this.noEntryValue = noEntryValue;
        this.size = 0;
        this.nonEmptySlots = 0;
        this.rehashThreshold = 0;
    }

    static long fixKey(long key) {
        Assert.neq(key, "key", REDIRECTED_KEY_FOR_EMPTY_SLOT, "REDIRECTED_KEY_FOR_EMPTY_SLOT");
        Assert.neq(key, "key", SPECIAL_KEY_FOR_DELETED_SLOT, "SPECIAL_KEY_FOR_DELETED_SLOT");
        return key == SPECIAL_KEY_FOR_EMPTY_SLOT ? REDIRECTED_KEY_FOR_EMPTY_SLOT : key;
    }

    long[] allocateKeysAndValuesArray(int entriesPerBucket) {
        // DesiredInitialCapacity is in units of 'entries'.
        // Ceiling(desiredInitialCapacity / entriesPerBucket)
        final int desiredNumBuckets = (desiredInitialCapacity + entriesPerBucket - 1) / entriesPerBucket;
        // Because we want the number of buckets to be prime
        final int proposedBucketCapacity = PrimeFinder.nextPrime(desiredNumBuckets);
        final int maxBucketCapacity = getMaxBucketCapacity(entriesPerBucket);
        final int newBucketCapacity = Math.min(proposedBucketCapacity, maxBucketCapacity);
        Assert.leq((long) newBucketCapacity * entriesPerBucket * 2, "(long)newBucketCapacity * entriesPerBucket * 2",
                Integer.MAX_VALUE, "Integer.MAX_VALUE");
        final int entryCapacity = newBucketCapacity * entriesPerBucket;
        final int longCapacity = entryCapacity * 2;
        rehashThreshold = (int) (entryCapacity * loadFactor);
        final long[] keysAndValues = new long[longCapacity];
        setKeysAndValues(keysAndValues);
        return keysAndValues;
    }

    void rehash(long[] oldKeysAndValues, boolean wantResize, int entriesPerBucket) {
        final int oldNumLongs = oldKeysAndValues.length;

        final int newNumLongs;
        if (wantResize) {
            final int oldBucketCapacity = oldNumLongs / (entriesPerBucket * 2);
            final int proposedBucketCapacity = PrimeFinder.nextPrime(oldBucketCapacity * 2);
            final int maxBucketCapacity = getMaxBucketCapacity(entriesPerBucket);
            final int newBucketCapacity = Math.min(proposedBucketCapacity, maxBucketCapacity);
            Assert.leq((long) newBucketCapacity * entriesPerBucket * 2,
                    "(long)newBucketCapacity * entriesPerBucket * 2",
                    Integer.MAX_VALUE, "Integer.MAX_VALUE");
            final int newEntryCapacity = newBucketCapacity * entriesPerBucket;
            newNumLongs = newEntryCapacity * 2;

            // If we reach the max bucket capacity, then force the rehash threshold to a high number like 90%.
            final float loadFactorToUse = newBucketCapacity < maxBucketCapacity ? loadFactor : NEARLY_FULL_LOAD_FACTOR;
            rehashThreshold = (int) (newEntryCapacity * loadFactorToUse);
        } else {
            newNumLongs = oldNumLongs;
        }
        size = 0;
        nonEmptySlots = 0;
        long[] newKvs = new long[newNumLongs];

        // Copy the keys and values over.
        for (int ii = 0; ii < oldKeysAndValues.length; ii += 2) {
            final long oldKey = oldKeysAndValues[ii];
            if (oldKey == SPECIAL_KEY_FOR_EMPTY_SLOT || oldKey == SPECIAL_KEY_FOR_DELETED_SLOT) {
                continue;
            }
            final long oldValue = oldKeysAndValues[ii + 1];
            putImplNoTranslate(newKvs, oldKey, oldValue, true);
        }
        setKeysAndValues(newKvs);
    }

    void checkSize(int sizeLimit) {
        // If the size reaches the max allowed value, then throw an exception.
        if (size >= sizeLimit) {
            throw new UnsupportedOperationException(
                    String.format("The Hashtable has exceeded its maximum capacity of %d elements", sizeLimit));
        }
    }

    protected abstract long putImplNoTranslate(long[] kvs, long key, long value, boolean insertOnly);

    protected abstract void setKeysAndValues(long[] keysAndValues);

    @Override
    public final int size() {
        return size;
    }

    @Override
    public final boolean isEmpty() {
        return size == 0;
    }

    final int capacityImpl(long[] keysAndValues) {
        return keysAndValues == null ? 0 : keysAndValues.length / 2;
    }

    final void clearImpl(long[] keysAndValues) {
        size = 0;
        nonEmptySlots = 0;
        // We leave rehashThreshold alone because the array size (and therefore the hashtable capacity) isn't changing.
        Arrays.fill(keysAndValues, SPECIAL_KEY_FOR_EMPTY_SLOT);
    }

    final void resetToNullImpl() {
        size = 0;
        nonEmptySlots = 0;
        rehashThreshold = 0;
    }

    @Override
    public final void defaultReturnValue(final long rv) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long defaultReturnValue() {
        return noEntryValue;
    }

    /**
     * @param kv Our keys and values array
     * @param space The array to populate (if {@code array} is not null and {@code array.length} >=
     *        {@link HashMapBase#size()}, otherwise an array of length {@link HashMapBase#size()} will be allocated.
     * @param wantValues false to return keys; true to return values
     * @return The passed-in or newly-allocated array of (keys or values).
     */
    final long[] keysOrValuesImpl(final long[] kv, final long[] space, final boolean wantValues) {
        final int sz = size;
        final long[] result = space != null && space.length >= sz ? space : new long[sz];
        int nextIndex = 0;
        // In a single-threaded case, we would not need the 'nextIndex < sz' part of the conjunction. But in the
        // unsynchronized concurrent case, we might encounter more keys than would fit in the array. To avoid an index
        // range exception, we do the 'nextIndex < sz' test both here and in the loop below.
        for (int ii = 0; ii < kv.length && nextIndex < sz; ii += 2) {
            final long key = kv[ii];
            if (key == SPECIAL_KEY_FOR_EMPTY_SLOT || key == SPECIAL_KEY_FOR_DELETED_SLOT) {
                continue;
            }
            final long resultEntry;
            if (wantValues) {
                resultEntry = kv[ii + 1];
            } else {
                resultEntry = key == REDIRECTED_KEY_FOR_EMPTY_SLOT ? SPECIAL_KEY_FOR_EMPTY_SLOT : key;
            }
            result[nextIndex++] = resultEntry;
        }
        return result;
    }

    final ObjectSet<Long2LongMap.Entry> entrySetImpl(final long[] kv) {
        return kv == null ? EMPTY_ENTRY_SET : new EntrySet(kv);
    }

    private static final EntrySet EMPTY_ENTRY_SET = new EntrySet(new long[0]);

    /**
     * Read-only entry set view backed by the supplied keys-and-values array. Supports iteration only.
     */
    private static final class EntrySet extends AbstractObjectSet<Long2LongMap.Entry>
            implements Long2LongMap.FastEntrySet {
        // Keep a local reference so a concurrent rehash that replaces the owning map's array does not crash us.
        private final long[] keysAndValues;

        EntrySet(final long[] kv) {
            this.keysAndValues = kv;
        }

        @Override
        public int size() {
            int count = 0;
            for (int ii = 0; ii < keysAndValues.length; ii += 2) {
                final long key = keysAndValues[ii];
                if (key != SPECIAL_KEY_FOR_EMPTY_SLOT && key != SPECIAL_KEY_FOR_DELETED_SLOT) {
                    ++count;
                }
            }
            return count;
        }

        @Override
        public ObjectIterator<Long2LongMap.Entry> iterator() {
            return new EntryIterator(keysAndValues);
        }

        @Override
        public ObjectIterator<Long2LongMap.Entry> fastIterator() {
            return new EntryIterator(keysAndValues);
        }
    }

    /*
     * Iterator over the keys-and-values array. We keep our own reference to the array so we can avoid crashing if there
     * is an unprotected concurrent write (e.g. a rehash that reallocates the owning map's array). We also un-redirect
     * REDIRECTED_KEY_FOR_EMPTY_SLOT back to 0 so callers see the key they originally inserted.
     */
    private static final class EntryIterator implements ObjectIterator<Long2LongMap.Entry> {
        private final long[] keysAndValues;
        /**
         * nextIndex points to the next occupied slot (or the first occupied slot if we have just been constructed), or
         * keysAndValues.length if there is no next occupied slot.
         */
        private int nextIndex;

        EntryIterator(final long[] kv) {
            this.keysAndValues = kv;
            this.nextIndex = findOccupiedSlot(0);
        }

        @Override
        public boolean hasNext() {
            return nextIndex < keysAndValues.length;
        }

        @Override
        public Long2LongMap.Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final long rawKey = keysAndValues[nextIndex];
            final long key = rawKey == REDIRECTED_KEY_FOR_EMPTY_SLOT ? SPECIAL_KEY_FOR_EMPTY_SLOT : rawKey;
            final long value = keysAndValues[nextIndex + 1];
            nextIndex = findOccupiedSlot(nextIndex + 2);
            return new AbstractLong2LongMap.BasicEntry(key, value);
        }

        /**
         * Find next occupied slot starting at {@code beginSlot}.
         *
         * @param beginSlot The inclusive position from where to start looking.
         * @return The slot containing the next occupied key, or keysAndValues.length if none.
         */
        private int findOccupiedSlot(int beginSlot) {
            while (beginSlot < keysAndValues.length) {
                final long key = keysAndValues[beginSlot];
                if (key != SPECIAL_KEY_FOR_EMPTY_SLOT && key != SPECIAL_KEY_FOR_DELETED_SLOT) {
                    break;
                }
                beginSlot += 2;
            }
            return beginSlot;
        }
    }

    // Following the original upstream spirit, callers should use keyArray() / valueArray() (declared on
    // NullableLong2LongMap) rather than the Long2LongMap-inherited keySet() / values() collection views, which would
    // require us to maintain custom collection-view objects. The collection views, containsKey, containsValue, and
    // putAll all throw UnsupportedOperationException; they can be implemented manually later if needed.

    @Override
    public boolean containsKey(long key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongSet keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LongCollection values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@org.jetbrains.annotations.NotNull final Map<? extends Long, ? extends Long> m) {
        throw new UnsupportedOperationException();
    }

    // Run this at class load time to confirm that the values returned by getMaxBucketCapacity aren't too large.
    // (It would be nice to also confirm that they are prime, but there's no easy way to do that)
    static {
        final int longsPerEntry = 2;
        for (int entriesPerBucket : new int[] {1, 2, 4}) {
            final long mbc = getMaxBucketCapacity(entriesPerBucket);
            // Assert.isPrime(mbc);
            Assert.leq(mbc * entriesPerBucket * longsPerEntry, "mbc * entriesPerBucket * longsPerEntry",
                    Integer.MAX_VALUE, "Integer.MAX_VALUE");
        }
    }

    /**
     * @param entriesPerBucket Number of entries per bucket
     * @return The largest prime p such that p * entriesPerBucket * 2 <= Integer.MAX_VALUE
     */
    private static int getMaxBucketCapacity(int entriesPerBucket) {
        switch (entriesPerBucket) {
            case 1:
                return 1073741789;
            case 2:
                return 536870909;
            case 4:
                return 268435399;
            default:
                throw new UnsupportedOperationException("Unexpected entriesPerBucket " + entriesPerBucket);
        }
    }

    /**
     * Computes Stafford variant 13 of 64bit mix function.
     *
     * <p>
     * See David Stafford's <a href="http://zimbry.blogspot.com/2011/09/better-bit-mixing-improving-on.html">Mix13
     * variant</a> / java.util.SplittableRandom#mix64(long).
     */
    static long mix64(long key) {
        key ^= (key >>> 30);
        key *= 0xbf58476d1ce4e5b9L;
        key ^= (key >>> 27);
        key *= 0x94d049bb133111ebL;
        key ^= (key >>> 31);
        return key;
    }

    /**
     * This poorly distributed hash function has been intentionally left with the acknowledgement that some sequentially
     * indexed key cases may benefit from the cacheability of the poor distribution. If we find common use cases in the
     * future where this poor first hash causes more problems than it solves, we can update it to a better distributed
     * hash function.
     */
    static int probe1(long key, int range) {
        final long badHash = (key ^ (key >>> 32));
        return (int) ((badHash & 0x7fffffffffffffffL) % range);
    }

    static int probe2(long key, int range) {
        final long mixHash = mix64(key);
        return (int) ((mixHash & 0x7fffffffffffffffL) % range);
    }
}
