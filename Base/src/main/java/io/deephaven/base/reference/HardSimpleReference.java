//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.reference;

/**
 * Strongly-held SimpleReference.
 */
public class HardSimpleReference<T> implements SimpleReference<T> {

    private T referent;

    public HardSimpleReference(T referent) {
        this.referent = referent;
    }

    @Override
    public final T get() {
        return referent;
    }

    @Override
    public final void clear() {
        referent = null;
    }
}
