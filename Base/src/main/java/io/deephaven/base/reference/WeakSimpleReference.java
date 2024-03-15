//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.reference;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Weakly-held SimpleReference.
 */
public class WeakSimpleReference<T> extends WeakReference<T> implements SimpleReference<T> {

    public WeakSimpleReference(final T referent) {
        super(referent);
    }

    public WeakSimpleReference(final T referent, final ReferenceQueue<? super T> referenceQueue) {
        super(referent, referenceQueue);
    }
}
