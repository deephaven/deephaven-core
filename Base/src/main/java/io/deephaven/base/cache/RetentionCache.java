/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;


import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.custom_hash.TObjectIntCustomHashMap;
import gnu.trove.strategy.IdentityHashingStrategy;

/**
 * Utility for holding strong references to otherwise unreachable classes (e.g. listeners that will be weakly held
 * by the object they subscribe to).
 */
public class RetentionCache<TYPE> {

    private final TObjectIntMap<TYPE> retainedObjectToReferenceCount =
            new TObjectIntCustomHashMap<>(IdentityHashingStrategy.INSTANCE, Constants.DEFAULT_CAPACITY, Constants.DEFAULT_LOAD_FACTOR, 0);

    /**
     * Ask this RetentionCache to hold on to a reference.
     * @param object The object to hold a reference to.
     * @return object, for convenience when retaining anonymous class instances.
     */
    public synchronized TYPE retain(TYPE object) {
        retainedObjectToReferenceCount.put(object, retainedObjectToReferenceCount.get(object) + 1);
        return object;
    }

    /**
     * Ask this RetentionCache to forget about a reference.
     * @param object The object to forget the reference to.
     */
    public synchronized void forget(TYPE object) {
        int referenceCount = retainedObjectToReferenceCount.get(object);
        if(referenceCount == 1) {
            retainedObjectToReferenceCount.remove(object);
        } else {
            retainedObjectToReferenceCount.put(object, referenceCount - 1);
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
