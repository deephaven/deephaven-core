/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.reference;

/**
 * Simple reference interface used by CachedReference.
 */
public interface SimpleReference<T> {

    /**
     * Retrieve the current referent.
     * 
     * @return The current referent, which may be null.
     */
    T get();

    /**
     * Clear the referent.
     */
    void clear();
}
