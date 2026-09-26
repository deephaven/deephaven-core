//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.ReadMode;
import io.deephaven.engine.table.impl.util.hash.NullableLongLongMaps.Shape;
import io.deephaven.hash.PrimeFinder;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * The one {@link NullableLongLongMap} implementation: a lock-free open-addressing hash map whose buckets live in a
 * single {@code long[]} that describes itself — its header carries the shape tag (bucket width) and the fastmod
 * reciprocal (see {@link #HEADER_LONGS}). The probe loops live in the width kernels ({@link K1V1Kernel},
 * {@link K2V2Kernel}, {@link K4V4Kernel}), static and pure in the array plus this map's counters; every operation takes
 * one volatile read of the array and dispatches on that snapshot's own tag, so no code path needs to know which shape
 * the map was born with. Callers construct maps through {@link NullableLongLongMaps} and hold the interface.
 *
 * <p>
 * One writer, any number of unsynchronized readers: readers work on a snapshot of the array, the writer publishes a
 * rebuilt array with one volatile store, and every piece of state a reader needs to probe travels inside the array.
 */
final class HashMapLockFreeKnVn implements NullableLongLongMapTestAccessors {
    static final int DEFAULT_INITIAL_CAPACITY = 10;
    static final long DEFAULT_NO_ENTRY_VALUE = -1;
    static final double DEFAULT_LOAD_FACTOR = 0.5;

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
    private static final double NEARLY_FULL_LOAD_FACTOR = 0.9;

    /**
     * This is the fraction of the maximum possible size at which we just give up and throw an exception. It is kept
     * slightly smaller than the NEARLY_FULL_LOAD_FACTOR (otherwise, we might end up in a situation where every put
     * caused a rehash). Additionally, for some reason K2V2 is much less tolerant of getting full than the other two (it
     * gets very slow as it approaches the max). For this reason, until we figure it out, we maintain individual size
     * factors for each KnVn.
     */
    private static final double SIZE_LIMIT_FACTOR1 = 0.85;
    private static final double SIZE_LIMIT_FACTOR2 = 0.75;
    private static final double SIZE_LIMIT_FACTOR4 = 0.85;
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

    // The entry capacity for the next backing array allocation. Starts at the construction-time request, and is
    // raised by resetToNullRetainingCapacityImpl() to the capacity the map had reached.
    private int desiredInitialCapacity;
    private final double loadFactor;
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

    // The buckets, self-described by their header (shape tag and reciprocal); never null — the empty sentinel stands
    // in for "no buckets". Every operation takes one volatile read of this field and works on that snapshot; put
    // re-reads it per element, because a put may rehash.
    private volatile long[] keysAndValues;
    // The shape the first allocation takes. Until then the sentinel's tag says EMPTY, so this is the only place the
    // requested width lives; afterwards the array's own tag is authoritative.
    private final Shape initialShape;
    private final ReadMode readMode;

    HashMapLockFreeKnVn(Shape initialShape, int desiredInitialCapacity, double loadFactor, long noEntryValue,
            ReadMode readMode) {
        this.initialShape = initialShape;
        this.desiredInitialCapacity = desiredInitialCapacity;
        this.loadFactor = loadFactor;
        this.noEntryValue = noEntryValue;
        this.readMode = Objects.requireNonNull(readMode, "readMode");
        this.size = 0;
        this.nonEmptySlots = 0;
        this.rehashThreshold = 0;
        this.keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    static long fixKey(long key) {
        Assert.neq(key, "key", REDIRECTED_KEY_FOR_EMPTY_SLOT, "REDIRECTED_KEY_FOR_EMPTY_SLOT");
        Assert.neq(key, "key", SPECIAL_KEY_FOR_DELETED_SLOT, "SPECIAL_KEY_FOR_DELETED_SLOT");
        return key == SPECIAL_KEY_FOR_EMPTY_SLOT ? REDIRECTED_KEY_FOR_EMPTY_SLOT : key;
    }

    /**
     * Round an entry capacity up to a whole number of buckets, in long arithmetic so that a saturated request (near
     * {@link Integer#MAX_VALUE}) cannot wrap negative.
     */
    static int desiredBucketCount(final int desiredEntryCapacity, final int entriesPerBucket) {
        return (int) (((long) desiredEntryCapacity + entriesPerBucket - 1) / entriesPerBucket);
    }

    // After the bucket data, the keys-and-values array carries a two-long header, [ data | shapeTag | reciprocal ]:
    // the array's shape tag (its bucket width — 1, 2 or 4 — or SHAPE_TAG_EMPTY for the empty sentinel) and the
    // fastmod reciprocal of its bucket count. Both are written while the array is being built and are published
    // by the same volatile store that publishes the buckets; neither is ever mutated afterwards. Whoever holds an
    // array therefore holds its width and its reciprocal — this state cannot tear against a snapshot, which is
    // the invariant all reader-visible probing state must satisfy: be a pure function of the array snapshot, or
    // travel inside it. The reciprocal sits at the very end (length - 1) and the tag just before it (length - 2):
    // an array describes itself, and a snapshot's own header says how to probe it.
    //
    // The header slots may share a cache line with the last buckets, or with the neighboring heap object.
    // Accepted: an invalidation requires someone else to write the sharing partner while a reader holds the
    // line — rare in both cases — and costs one line re-fetch. The rule is to pad against certainties, not
    // possibilities: if this header ever gains writer-hot state (e.g. size, written on every put), that is
    // guaranteed sharing, and a no-man's-land margin between the read-mostly and writer-hot regions becomes
    // mandatory at that point.
    static final int HEADER_LONGS = 2;

    /**
     * The shape tag of an array that holds no buckets — the empty sentinel. Every real array's tag is its bucket width.
     */
    static final int SHAPE_TAG_EMPTY = 0;

    // The array is never null. An empty map — fresh, or after resetToNull* — holds this shared, immutable sentinel:
    // one bucket of the widest shape (every slot SPECIAL_KEY_FOR_EMPTY_SLOT, which is 0) behind a header whose
    // reciprocal is 0, so probe1 sends every key to bucket 0 (fastRange(0, n) == 0 for any n) and finds it empty —
    // whatever a map's width makes of the data length (four K1V1 buckets, two K2V2, one K4V4: all empty). Probing
    // the sentinel is therefore an ordinary miss, so no read path needs an empty-map branch to be correct; the
    // chunked gets keep one anyway, as a fast path (every key is a miss, so they fill the result without probing).
    // Writes must never touch it: put swaps in a real array before probing (where the null check used to live);
    // clear and resetToNullRetainingCapacity skip it. Both tell it apart by its shape tag, SHAPE_TAG_EMPTY (see
    // isEmptyArray).
    static final long[] EMPTY_KEYS_AND_VALUES = newEmptyKeysAndValues();

    private static long[] newEmptyKeysAndValues() {
        final long[] kvs = new long[4 * 2 + HEADER_LONGS];
        writeShapeTag(kvs, SHAPE_TAG_EMPTY);
        writeReciprocal(kvs, 0);
        return kvs;
    }

    /**
     * The fastmod reciprocal of {@code kvs}'s bucket count, read from the array's own header — published with the array
     * and immutable thereafter, so it cannot tear against the snapshot in hand.
     */
    static long reciprocalOf(long[] kvs) {
        return kvs[kvs.length - 1];
    }

    private static void writeReciprocal(long[] kvs, long reciprocal) {
        kvs[kvs.length - 1] = reciprocal;
    }

    /**
     * The shape tag of {@code kvs}, read from the array's own header: its bucket width (1, 2 or 4), or
     * {@link #SHAPE_TAG_EMPTY} for the empty sentinel. Published with the array and immutable thereafter, like the
     * reciprocal.
     */
    static int shapeTagOf(long[] kvs) {
        return (int) kvs[kvs.length - 2];
    }

    private static void writeShapeTag(long[] kvs, int shapeTag) {
        kvs[kvs.length - 2] = shapeTag;
    }

    /**
     * Is {@code kvs} the empty sentinel — no buckets at all, the state of a map that has never been written or has been
     * reset? The array answers for itself, through its tag; writes ask before touching an array.
     */
    static boolean isEmptyArray(long[] kvs) {
        return shapeTagOf(kvs) == SHAPE_TAG_EMPTY;
    }

    /**
     * The first allocation, at the requested initial shape; replaces the empty sentinel.
     */
    private long[] allocateKeysAndValuesArray() {
        final int entriesPerBucket = initialShape.bucketWidth();
        final int desiredNumBuckets = desiredBucketCount(desiredInitialCapacity, entriesPerBucket);
        final int dataLongs = setRehashThresholdAndCalcLongCapacity(desiredNumBuckets, entriesPerBucket);
        final long[] keysAndValues = new long[dataLongs + HEADER_LONGS];
        writeShapeTag(keysAndValues, entriesPerBucket);
        writeReciprocal(keysAndValues, reciprocalFor(dataLongs / (entriesPerBucket * 2)));
        this.keysAndValues = keysAndValues;
        return keysAndValues;
    }

    /**
     * Rebuilds the array — at double the bucket count if {@code wantResize}, else the same — and publishes it. The
     * array says what shape it is, and the rebuilt array keeps that shape.
     */
    void rehash(long[] oldKeysAndValues, boolean wantResize) {
        final int entriesPerBucket = shapeTagOf(oldKeysAndValues);
        final int oldDataLongs = oldKeysAndValues.length - HEADER_LONGS;

        final int newDataLongs;
        if (wantResize) {
            final int oldBucketCapacity = oldDataLongs / (entriesPerBucket * 2);
            final int desiredNumBuckets = grownBucketCount(oldBucketCapacity, entriesPerBucket);
            newDataLongs = setRehashThresholdAndCalcLongCapacity(desiredNumBuckets, entriesPerBucket);
        } else {
            newDataLongs = oldDataLongs;
        }
        size = 0;
        nonEmptySlots = 0;
        long[] newKvs = new long[newDataLongs + HEADER_LONGS];
        writeShapeTag(newKvs, entriesPerBucket);
        final long newReciprocal = reciprocalFor(newDataLongs / (entriesPerBucket * 2));
        writeReciprocal(newKvs, newReciprocal);

        // Copy the keys and values over. (The bound excludes the source array's header.)
        for (int ii = 0; ii < oldDataLongs; ii += 2) {
            final long oldKey = oldKeysAndValues[ii];
            if (oldKey == SPECIAL_KEY_FOR_EMPTY_SLOT || oldKey == SPECIAL_KEY_FOR_DELETED_SLOT) {
                continue;
            }
            final long oldValue = oldKeysAndValues[ii + 1];
            putNoTranslate(entriesPerBucket, newKvs, newReciprocal, oldKey, oldValue);
        }
        keysAndValues = newKvs;
    }

    // Rehash's insert: the key is already in its stored form, and the width is the one the new array was built at.
    private void putNoTranslate(int entriesPerBucket, long[] kvs, long numBucketsReciprocal, long key, long value) {
        switch (entriesPerBucket) {
            case 1:
                K1V1Kernel.putNoTranslate(this, kvs, numBucketsReciprocal, key, value, true);
                return;
            case 2:
                K2V2Kernel.putNoTranslate(this, kvs, numBucketsReciprocal, key, value, true);
                return;
            case 4:
                K4V4Kernel.putNoTranslate(this, kvs, numBucketsReciprocal, key, value, true);
                return;
            default:
                throw new IllegalStateException("Unexpected shape tag " + entriesPerBucket);
        }
    }

    /**
     * The bucket count a growing rehash asks for: double the current one, saturating at the width's maximum bucket
     * capacity. Doubling in int arithmetic overflowed once the map sat at that maximum — and a growing rehash can be
     * asked for there, because deleted slots count toward the rehash threshold while only live entries count toward the
     * size limit — so the prime finder was handed a negative count and answered with a handful of buckets for a billion
     * entries.
     */
    static int grownBucketCount(int oldBucketCapacity, int entriesPerBucket) {
        return (int) Math.min(getMaxBucketCapacity(entriesPerBucket), 2L * oldBucketCapacity);
    }

    private int setRehashThresholdAndCalcLongCapacity(int desiredNumBuckets, int entriesPerBucket) {
        // Because we want the number of buckets to be prime
        final int proposedBucketCapacity = PrimeFinder.nextPrime(desiredNumBuckets);
        final int maxBucketCapacity = getMaxBucketCapacity(entriesPerBucket);
        final int newBucketCapacity = Math.min(proposedBucketCapacity, maxBucketCapacity);
        Assert.leq((long) newBucketCapacity * entriesPerBucket * 2 + HEADER_LONGS,
                "(long)newBucketCapacity * entriesPerBucket * 2 + HEADER_LONGS",
                Integer.MAX_VALUE, "Integer.MAX_VALUE");
        final int entryCapacity = newBucketCapacity * entriesPerBucket;
        final int longCapacity = entryCapacity * 2;
        // Once clamped to the maximum bucket capacity there is no larger size to grow into, so run at the
        // nearly-full load factor rather than rehashing (at the same capacity) partway through a large fill.
        final double loadFactorToUse = newBucketCapacity < maxBucketCapacity ? loadFactor : NEARLY_FULL_LOAD_FACTOR;
        rehashThreshold = (int) (entryCapacity * loadFactorToUse);
        return longCapacity;
    }

    void checkSize(int sizeLimit) {
        // If the size reaches the max allowed value, then throw an exception.
        if (size >= sizeLimit) {
            throw new UnsupportedOperationException(
                    String.format("The Hashtable has exceeded its maximum capacity of %d elements", sizeLimit));
        }
    }

    @Override
    public final int size() {
        return size;
    }

    @Override
    public final boolean isEmpty() {
        return size == 0;
    }

    final int capacityImpl(long[] keysAndValues) {
        return isEmptyArray(keysAndValues) ? 0 : (keysAndValues.length - HEADER_LONGS) / 2;
    }

    final void clearImpl(long[] keysAndValues) {
        size = 0;
        nonEmptySlots = 0;
        if (isEmptyArray(keysAndValues)) {
            // Already empty; the shared sentinel is never written.
            return;
        }
        // We leave rehashThreshold alone because the array size (and therefore the hashtable capacity) isn't changing.
        Arrays.fill(keysAndValues, 0, keysAndValues.length - HEADER_LONGS, SPECIAL_KEY_FOR_EMPTY_SLOT);
    }

    final void resetToNullImpl() {
        size = 0;
        nonEmptySlots = 0;
        rehashThreshold = 0;
    }

    final void resetToNullRetainingCapacityImpl(long[] keysAndValues) {
        if (!isEmptyArray(keysAndValues)) {
            // Remember the capacity, in entries, so that the next allocation lands back at this size directly rather
            // than regrowing from the construction-time capacity through successive rehashes. We remember the size
            // rather than holding the array itself so that the storage is reclaimable while the map sits empty.
            desiredInitialCapacity = Math.max(desiredInitialCapacity, (keysAndValues.length - HEADER_LONGS) / 2);
        }
        resetToNullImpl();
    }

    /**
     * Compute an entry capacity to request at construction so that the map can absorb {@code expectedEntries} entries
     * (including deleted slots) without rehashing.
     *
     * <p>
     * A put rehashes when nonEmptySlots reaches rehashThreshold, which is {@code (int) (entryCapacity * loadFactor)}.
     * So we need the smallest capacity whose threshold is strictly greater than {@code expectedEntries}. We compute it
     * directly, then check it against the same expression the map uses and bump by one if rounding left us short.
     * Bucket-count rounding and prime selection in {@link #allocateKeysAndValuesArray} only ever increase the capacity,
     * and the threshold is non-decreasing in the capacity, so the allocated map's threshold clears the expected count
     * too.
     *
     * @param expectedEntries the number of slots the map must absorb without rehashing
     * @param loadFactor the map's load factor
     * @return an entry capacity to request, saturating at {@link Integer#MAX_VALUE} (at which point the map clamps to
     *         its maximum capacity and runs at the nearly-full load factor)
     */
    static int capacityForExpectedEntries(final int expectedEntries, final double loadFactor) {
        final long neededThreshold = (long) expectedEntries + 1;
        long candidate = (long) Math.ceil(neededThreshold / loadFactor);
        if (candidate < Integer.MAX_VALUE && (long) (candidate * loadFactor) < neededThreshold) {
            // Because the arithmetic is in double and candidate fits in an int, one bump is always enough.
            ++candidate;
        }
        return (int) Math.min(candidate, Integer.MAX_VALUE);
    }

    @Override
    public final long defaultReturnValue() {
        return noEntryValue;
    }

    /**
     * @param kv Our keys and values array
     * @param space The array to populate (if {@code array} is not null and {@code array.length} >= {@link #size()},
     *        otherwise an array of length {@link #size()} will be allocated.
     * @param wantValues false to return keys; true to return values
     * @return The passed-in or newly-allocated array of (keys or values).
     */
    final long[] keysOrValuesImpl(final long[] kv, final long[] space, final boolean wantValues) {
        final int sz = size;
        final long[] result = space != null && space.length >= sz ? space : new long[sz];
        int nextIndex = 0;
        // In a single-threaded case, we would not need the 'nextIndex < sz' part of the conjunction. But in the
        // unsynchronized concurrent case, we might encounter more keys than would fit in the array. To avoid an index
        // range exception, we do the 'nextIndex < sz' test here.
        final int dataLongs = kv.length - HEADER_LONGS;
        for (int ii = 0; ii < dataLongs && nextIndex < sz; ii += 2) {
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

    final void forEachImpl(final long[] kv, LongLongBiConsumer consumer) {
        final int dataLongs = kv.length - HEADER_LONGS;
        for (int nextIndex = findOccupiedSlot(kv, 0); nextIndex < dataLongs; nextIndex =
                findOccupiedSlot(kv, nextIndex + 2)) {
            final long rawKey = kv[nextIndex];
            final long key = rawKey == REDIRECTED_KEY_FOR_EMPTY_SLOT ? SPECIAL_KEY_FOR_EMPTY_SLOT : rawKey;
            final long value = kv[nextIndex + 1];
            consumer.accept(key, value);
        }
    }

    /**
     * Find next occupied slot starting at {@code beginSlot}.
     *
     * @param beginSlot The inclusive position from where to start looking.
     * @return The slot containing the next occupied key, or keysAndValues.length if none.
     */
    private int findOccupiedSlot(long[] keysAndValues, int beginSlot) {
        final int dataLongs = keysAndValues.length - HEADER_LONGS;
        while (beginSlot < dataLongs) {
            final long key = keysAndValues[beginSlot];
            if (key != SPECIAL_KEY_FOR_EMPTY_SLOT && key != SPECIAL_KEY_FOR_DELETED_SLOT) {
                break;
            }
            beginSlot += 2;
        }
        return beginSlot;
    }

    // Run this at class load time to confirm that the values returned by getMaxBucketCapacity aren't too large.
    // (It would be nice to also confirm that they are prime, but there's no easy way to do that)
    static {
        final int longsPerEntry = 2;
        for (int entriesPerBucket : new int[] {1, 2, 4}) {
            final long mbc = getMaxBucketCapacity(entriesPerBucket);
            // Assert.isPrime(mbc);
            Assert.leq(mbc * entriesPerBucket * longsPerEntry + HEADER_LONGS,
                    "mbc * entriesPerBucket * longsPerEntry + HEADER_LONGS",
                    Integer.MAX_VALUE, "Integer.MAX_VALUE");
        }
    }

    /**
     * @param entriesPerBucket Number of entries per bucket
     * @return The largest prime p such that p * entriesPerBucket * 2 + HEADER_LONGS <= Integer.MAX_VALUE
     */
    static int getMaxBucketCapacity(int entriesPerBucket) {
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
     * Computes the murmur3 fmix64 finalizer — a full-strength mixer, independent of probe1's weak fold, so the
     * double-hash step behaves as an independent hash function.
     */
    static long mix64b(long key) {
        key ^= (key >>> 33);
        key *= 0xff51afd7ed558ccdL;
        key ^= (key >>> 33);
        key *= 0xc4ceb9fe1a85ec53L;
        key ^= (key >>> 33);
        return key;
    }

    /**
     * Computes the scaled reciprocal used by Lemire's exact "fastmod" remainder: ceil(2^64 / numBuckets), i.e. the
     * reciprocal of the bucket count in unsigned 0.64 fixed-point, rounded up. For 32-bit unsigned x, x % numBuckets ==
     * fastRange(reciprocal * x, numBuckets). The result is only meaningful together with the numBuckets it was computed
     * from; the array's header carries it (see {@link #reciprocalOf}), so whoever holds an array holds its reciprocal.
     */
    static long reciprocalFor(int numBuckets) {
        return Long.divideUnsigned(-1L, numBuckets) + 1;
    }

    /**
     * The high 64 bits of the unsigned 128-bit product of x and range, i.e. floor(x / 2^64 * range) — the multiply-high
     * reduction of unsigned x into [0, range). {@link Math#multiplyHigh} is signed and Math.unsignedMultiplyHigh needs
     * JDK 18 (we compile to an earlier release); for a nonnegative range the unsigned correction reduces to a single
     * and-add.
     */
    static int fastRange(long x, int range) {
        return (int) (Math.multiplyHigh(x, range) + ((x >> 63) & range));
    }

    /**
     * First probe. This poorly distributed hash function — a 32-bit fold that sends sequentially indexed keys to
     * adjacent, distinct buckets — has been intentionally kept: sequentially indexed key cases benefit from the
     * cacheability of the poor distribution. The prime modulo is computed exactly, via fastmod with the caller-supplied
     * precomputed reciprocal, keeping the 64-bit division off the per-element path.
     */
    static int probe1(long key, int range, long numBucketsReciprocal) {
        // The 64->32 fold uses + rather than ^: the xor fold is sign-flip symmetric (for 0 < k < 2^32, the fold of
        // -k equals k - 1), so mirror-image key families alias onto each other's buckets — measured as an ~85%
        // getMiss regression against negated-key probes. Carry propagation breaks the symmetry, and for keys whose
        // high word is zero (all small nonnegative keys) + and ^ produce identical buckets, so the hit path of
        // typical row-key populations is unchanged. Sequential keys still land in adjacent buckets either way.
        // The high word is spread by an odd multiplier before the fold. Without it, key families a power of two apart
        // (a partitioned table's regions sit at r << 43) landed 2,048 buckets apart and overlapped bucket for bucket,
        // so a lookup keyed by regioned row keys paid a wasted first probe almost every time; with it they land at
        // pseudo-random offsets. The low word stays linear, so consecutive keys still take consecutive buckets, and
        // keys whose high word is zero hash exactly as before.
        final long fold32 = (key + (key >>> 32) * 0x9E3779B9L) & 0xffffffffL;
        return fastRange(numBucketsReciprocal * fold32, range);
    }

    /**
     * Second probe (double-hash step): full mix then multiply-high "fastrange" reduction — well-mixed input makes plain
     * scaling into [0, range) as good as a remainder, and it needs no per-divisor constant at all.
     */
    static int probe2(long key, int range) {
        return fastRange(mix64b(key), range);
    }

    // ------------------------------------------------------------------------------------------------------------
    // The interface: one volatile read of the array per operation, then dispatch on the snapshot's own shape tag.

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        putChunk(keys, values, oldValues, false);
    }

    @Override
    public void putIfAbsent(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues) {
        putChunk(keys, values, oldValues, true);
    }

    private void putChunk(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues, boolean insertOnly) {
        final int n = keys.size();
        long[] kvs = firstArrayForPuts(n);
        long numBucketsReciprocal = reciprocalOf(kvs);
        for (int ii = 0; ii < n; ++ii) {
            oldValues.set(ii, putOne(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), insertOnly));
            // Hot reads: cheap, and free of a stale-check branch (the array is never null).
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
        oldValues.setSize(n);
    }

    @Override
    public void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values) {
        final int n = keys.size();
        long[] kvs = firstArrayForPuts(n);
        long numBucketsReciprocal = reciprocalOf(kvs);
        for (int ii = 0; ii < n; ++ii) {
            putOne(kvs, numBucketsReciprocal, keys.get(ii), values.get(ii), false);
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    @Override
    public void put(LongChunk<? extends Any> keys, long value) {
        final int n = keys.size();
        long[] kvs = firstArrayForPuts(n);
        long numBucketsReciprocal = reciprocalOf(kvs);
        for (int ii = 0; ii < n; ++ii) {
            putOne(kvs, numBucketsReciprocal, keys.get(ii), value, false);
            kvs = keysAndValues;
            numBucketsReciprocal = reciprocalOf(kvs);
        }
    }

    /**
     * The array the first element of a batch of {@code n} puts probes. Unlike get, the volatile read is NOT hoisted
     * across the batch: any put may rehash, so each element must see the array that the previous element may have
     * replaced; the reciprocal rides in a register-local memo, refreshed from the new array's own header whenever the
     * array changes (a load, not a divide: every array carries its reciprocal). The first write allocates, at the
     * requested initial shape. From there on the array is never the sentinel (rehash only ever builds real arrays), so
     * the loops dispatch on real widths only.
     */
    private long[] firstArrayForPuts(final int n) {
        final long[] kvs = keysAndValues;
        return n > 0 && isEmptyArray(kvs) ? allocateKeysAndValuesArray() : kvs;
    }

    /**
     * One put, dispatched per element rather than per chunk: a put may replace the array between elements, and the
     * array's own tag says which kernel probes it (today a rehash keeps the width; it need not always). One predicted
     * branch per element.
     */
    private long putOne(final long[] kvs, final long numBucketsReciprocal, final long key, final long value,
            final boolean insertOnly) {
        switch (shapeTagOf(kvs)) {
            case 1:
                return K1V1Kernel.put(this, kvs, numBucketsReciprocal, key, value, insertOnly);
            case 2:
                return K2V2Kernel.put(this, kvs, numBucketsReciprocal, key, value, insertOnly);
            case 4:
                return K4V4Kernel.put(this, kvs, numBucketsReciprocal, key, value, insertOnly);
            default:
                throw new IllegalStateException("Unexpected shape tag " + shapeTagOf(kvs));
        }
    }

    @Override
    public void get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        // Take the volatile read once: like every read operation, a chunked get sees one consistent snapshot of the
        // array, whose header carries its shape and its reciprocal. Each shape's loop lives in a method of its own,
        // so that this dispatcher stays small enough to be inlined at the scalar cursor's call site with its kernel
        // calls inlined in turn: one method holding every loop measured as a real call per key on single-key chunks.
        final long[] localKvs = keysAndValues;
        switch (shapeTagOf(localKvs)) {
            case 1:
                getK1V1(localKvs, keys, result);
                break;
            case 2:
                getK2V2(localKvs, keys, result);
                break;
            case 4:
                getK4V4(localKvs, keys, result);
                break;
            case SHAPE_TAG_EMPTY:
                getEmpty(keys, result);
                break;
            default:
                throw new IllegalStateException("Unexpected shape tag " + shapeTagOf(localKvs));
        }
    }

    private void getK1V1(long[] localKvs, LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        final long noEntry = noEntryValue;
        for (int ii = 0; ii < n; ++ii) {
            result.set(ii, K1V1Kernel.get(localKvs, numBucketsReciprocal, keys.get(ii), noEntry));
        }
        result.setSize(n);
    }

    private void getK2V2(long[] localKvs, LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        final long noEntry = noEntryValue;
        for (int ii = 0; ii < n; ++ii) {
            result.set(ii, K2V2Kernel.get(localKvs, numBucketsReciprocal, keys.get(ii), noEntry));
        }
        result.setSize(n);
    }

    private void getK4V4(long[] localKvs, LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        final long noEntry = noEntryValue;
        // Adaptive read strategy: when the map's footprint is beyond the last-level cache — its whole job is
        // overlapping the misses that a cache-resident table simply does not have — service the chunk through the
        // AMAC window; otherwise use the serial loop, which ties or wins when the table is cache-resident. Footprint
        // is a function of the snapshot's own length, so the choice is stable between rehashes and flips exactly when
        // the array grows past the cache. (Occupancy is deliberately not consulted; see wantWindowedReads.) The
        // chunk must also be wide enough to fill the window: its fixed cost is paid per call, and a single-key chunk —
        // the scalar cursor's case — has nothing to overlap, measured at 1.6-2.3x slower under the window. A pinned
        // ReadMode overrides the gate, for pricing and tests only. Reads are pure, so the windowed path may resolve
        // lookups out of index order, invisibly to the caller.
        final boolean windowed = readMode == ReadMode.ADAPTIVE
                ? NullableLongLongMaps.wantWindowedReads((localKvs.length - HEADER_LONGS) / 2, n)
                : readMode == ReadMode.WINDOW;
        if (windowed) {
            K4V4Kernel.getBatch(localKvs, numBucketsReciprocal, keys, result, noEntry);
        } else {
            for (int ii = 0; ii < n; ++ii) {
                result.set(ii, K4V4Kernel.get(localKvs, numBucketsReciprocal, keys.get(ii), noEntry));
            }
        }
        result.setSize(n);
    }

    // No buckets: every key is a miss.
    private void getEmpty(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result) {
        final int n = keys.size();
        final long noEntry = noEntryValue;
        for (int ii = 0; ii < n; ++ii) {
            result.set(ii, noEntry);
        }
        result.setSize(n);
    }

    @Override
    public void remove(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> oldValues) {
        // Like get (and unlike put), the volatile read is hoisted: a remove tombstones slots in place and never
        // rehashes, so no element can replace the array a later element must see. Per-shape loops in their own
        // methods, for the same inlining reason as get.
        final long[] localKvs = keysAndValues;
        switch (shapeTagOf(localKvs)) {
            case 1:
                removeK1V1(localKvs, keys, oldValues);
                break;
            case 2:
                removeK2V2(localKvs, keys, oldValues);
                break;
            case 4:
                removeK4V4(localKvs, keys, oldValues);
                break;
            case SHAPE_TAG_EMPTY:
                // No buckets: nothing to remove.
                getEmpty(keys, oldValues);
                break;
            default:
                throw new IllegalStateException("Unexpected shape tag " + shapeTagOf(localKvs));
        }
    }

    private void removeK1V1(long[] localKvs, LongChunk<? extends Any> keys,
            WritableLongChunk<? extends Any> oldValues) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        for (int ii = 0; ii < n; ++ii) {
            oldValues.set(ii, K1V1Kernel.remove(this, localKvs, numBucketsReciprocal, keys.get(ii)));
        }
        oldValues.setSize(n);
    }

    private void removeK2V2(long[] localKvs, LongChunk<? extends Any> keys,
            WritableLongChunk<? extends Any> oldValues) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        for (int ii = 0; ii < n; ++ii) {
            oldValues.set(ii, K2V2Kernel.remove(this, localKvs, numBucketsReciprocal, keys.get(ii)));
        }
        oldValues.setSize(n);
    }

    private void removeK4V4(long[] localKvs, LongChunk<? extends Any> keys,
            WritableLongChunk<? extends Any> oldValues) {
        final int n = keys.size();
        final long numBucketsReciprocal = reciprocalOf(localKvs);
        for (int ii = 0; ii < n; ++ii) {
            oldValues.set(ii, K4V4Kernel.remove(this, localKvs, numBucketsReciprocal, keys.get(ii)));
        }
        oldValues.setSize(n);
    }

    @Override
    public int capacity() {
        return capacityImpl(keysAndValues);
    }

    @Override
    public void clear() {
        clearImpl(keysAndValues);
    }

    @Override
    public void resetToNull() {
        resetToNullImpl();
        keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    @Override
    public void resetToNullRetainingCapacity() {
        resetToNullRetainingCapacityImpl(keysAndValues);
        keysAndValues = EMPTY_KEYS_AND_VALUES;
    }

    /**
     * The shape this map probes with right now: its array's own tag or, while it is empty, the shape its first
     * allocation will take.
     */
    Shape shape() {
        final long[] localKvs = keysAndValues;
        return isEmptyArray(localKvs) ? initialShape : Shape.forBucketWidth(shapeTagOf(localKvs));
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
