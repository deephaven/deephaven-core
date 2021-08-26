/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;

import io.deephaven.base.MathUtil;
import io.deephaven.base.Procedure;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Require;
import gnu.trove.impl.PrimeFinder;
import org.jetbrains.annotations.Nullable;

import java.util.Random;

/**
 * The central idea is that we can use an open-addressed map as a bounded cache with concurrent get and synchronized put
 * access.
 *
 * Rather than rely on expensive and/or concurrency-destroying bookkeeping schemes to allow "smart" cache replacement,
 * we rely on the assumption that our hash function and probe sequence computation does a fairly good job of
 * distributing keyed objects that have a high likelihood of being useful to cache during overlapping timeframes.
 *
 * We never remove anything from the cache without replacing it with a new item. A callback is accepted to allow for
 * item resource cleanup upon eviction from the cache.
 *
 * The impact of collisions (for the bucket an item hashes to, or any other bucket in its associated probe sequence) is
 * mitigated by randomized eviction of a victim item in a probe sequence of bounded length.
 *
 * Note that, unlike common open-addressed hashing schemes, we're unconcerned with load factor - we have an explicitly
 * bounded capacity, and explicitly bounded probe sequence length, which must be tuned for the workload in question.
 */
public class KeyedObjectCache<KEY_TYPE, VALUE_TYPE> {

    /**
     * The key definition for this instance.
     */
    private final KeyedObjectKey<KEY_TYPE, VALUE_TYPE> keyDefinition;

    /**
     * The procedure to invoke on values after they've been evicted.
     */
    private final Procedure.Unary<VALUE_TYPE> postEvictionProcedure;

    /**
     * A source of pseudo-random numbers for choosing which slot in a bounded probe sequence to evict if no empty slots
     * are found.
     */
    private final Random random;

    /**
     * Storage for values in the cache.
     */
    private final VALUE_TYPE storage[];

    /**
     * Temporary storage for indexes visited in the probe-sequence of a put.
     */
    private final int probedSlots[];

    /**
     * @param capacity Lower bound on maximum capacity. Rounded up to to a prime number.
     * @param probeSequenceLength Lower bound on number of slots to probe (inclusive of the one a key hashes directly
     *        to). Rounded up to a power of 2.
     * @param keyDefinition The key definition
     * @param postEvictionProcedure Optional. Invoked without any extra synchronization.
     * @param random Pseudo-random number generator
     */
    public KeyedObjectCache(final int capacity,
            final int probeSequenceLength,
            final KeyedObjectKey<KEY_TYPE, VALUE_TYPE> keyDefinition,
            @Nullable final Procedure.Unary<VALUE_TYPE> postEvictionProcedure,
            final Random random) {
        Require.gtZero(capacity, "capacity");
        Require.inRange(probeSequenceLength, "probeSequenceLength", capacity / 2, "capacity / 2");

        final int primeCapacity = PrimeFinder.nextPrime(capacity);
        // noinspection unchecked
        this.storage = (VALUE_TYPE[]) new Object[primeCapacity];

        final int powerOf2ProbeSequenceLength = 1 << MathUtil.ceilLog2(probeSequenceLength);
        this.probedSlots = new int[powerOf2ProbeSequenceLength];

        this.keyDefinition = Require.neqNull(keyDefinition, "keyDefinition");
        this.postEvictionProcedure = postEvictionProcedure;
        this.random = Require.neqNull(random, "random");
    }

    public final int getCapacity() {
        return storage.length;
    }

    final int getProbeSequenceLength() {
        return probedSlots.length;
    }

    /**
     * Lookup an item by key.
     * 
     * @param key The key
     * @return The associated value, or null if none is present.
     */
    public final VALUE_TYPE get(KEY_TYPE key) {
        final int hashCode = keyDefinition.hashKey(key) & 0x7FFFFFFF;

        int slot = hashCode % storage.length;
        VALUE_TYPE candidate = storage[slot];

        if (candidate == null || keyDefinition.equalKey(key, candidate)) {
            return candidate;
        }

        final int probeInterval = computeProbeInterval(hashCode);

        for (int psi = 1; psi < probedSlots.length; ++psi) {
            if ((slot -= probeInterval) < 0) {
                slot += storage.length;
            }
            candidate = storage[slot];
            if (candidate == null || keyDefinition.equalKey(key, candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /**
     * Add an item to the cache if it's not already present.
     * 
     * @param value The value, from which the key will be derived using the key definition.
     * @return The equal (by key) item already present, or value if no such item was found.
     */
    public final VALUE_TYPE putIfAbsent(VALUE_TYPE value) {
        final KEY_TYPE key = keyDefinition.getKey(value);
        final int hashCode = keyDefinition.hashKey(key) & 0x7FFFFFFF;

        int slot = hashCode % storage.length;
        final VALUE_TYPE evictedValue;

        // NB: We might be better off getting our random to-evict probed-slot-index pessimistically before grabbing the
        // lock. We could then eliminate the probedSlots array and its maintenance, and do the pseudo-random number
        // generation without a lock. On the other hand, we'd create contention for atomic updates to the PRNG's
        // internal state, and potentially waste computation time if the pessimism was unwarranted.
        synchronized (storage) {
            VALUE_TYPE candidate = storage[slot];

            if (candidate == null) {
                storage[slot] = value;
                return value;
            } else if (keyDefinition.equalKey(key, candidate)) {
                return candidate;
            } else {
                probedSlots[0] = slot;
                final int probeInterval = computeProbeInterval(hashCode);

                for (int psi = 1; psi < probedSlots.length; ++psi) {
                    if ((slot -= probeInterval) < 0) {
                        slot += storage.length;
                    }

                    candidate = storage[slot];
                    if (candidate == null) {
                        storage[slot] = value;
                        return value;
                    } else if (keyDefinition.equalKey(key, candidate)) {
                        return candidate;
                    }

                    probedSlots[psi] = slot;
                }
            }

            final int slotToEvict = probedSlots[random.nextInt(probedSlots.length)];
            evictedValue = storage[slotToEvict];
            storage[slotToEvict] = value;
        }

        if (postEvictionProcedure != null) {
            postEvictionProcedure.call(evictedValue);
        }
        return value;
    }

    private int computeProbeInterval(int hashCode) {
        // see Knuth, p. 529
        return 1 + (hashCode % (storage.length - 2));
    }
}
