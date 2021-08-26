/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;


import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.strategy.IdentityHashingStrategy;
import org.jetbrains.annotations.NotNull;

/**
 * Utility for holding strong references to otherwise unreachable classes (e.g. listeners that will
 * be weakly held by the object they subscribe to).
 */
public class RetentionCache<TYPE> {

    private final TObjectIntMap<TYPE> retainedObjectToReferenceCount =
        new TObjectIntCustomHashMap<>(IdentityHashingStrategy.INSTANCE, Constants.DEFAULT_CAPACITY,
            Constants.DEFAULT_LOAD_FACTOR, 0);

    /**
     * Ask this RetentionCache to hold on to a reference in order to ensure that {@code referent}
     * remains strongly-reachable for the garbage collector.
     *
     * @param referent The object to hold a reference to
     * @return {@code referent}, for convenience when retaining anonymous class instances
     */
    public synchronized TYPE retain(@NotNull final TYPE referent) {
        retainedObjectToReferenceCount.put(referent,
            retainedObjectToReferenceCount.get(referent) + 1);
        return referent;
    }

    /**
     * Ask this RetentionCache to forget about a reference.
     *
     * @param referent The referent to forget the reference to
     */
    public synchronized void forget(@NotNull final TYPE referent) {
        int referenceCount = retainedObjectToReferenceCount.get(referent);
        if (referenceCount == 1) {
            retainedObjectToReferenceCount.remove(referent);
        } else {
            retainedObjectToReferenceCount.put(referent, referenceCount - 1);
        }
    }

    /**
     * Ask this RetentionCache to forget all the references it's remembering.
     */
    @SuppressWarnings("unused")
    public synchronized void forgetAll() {
        retainedObjectToReferenceCount.clear();
    }
}
