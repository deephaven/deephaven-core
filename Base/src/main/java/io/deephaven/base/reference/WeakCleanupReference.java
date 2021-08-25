/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.reference;

import java.lang.ref.ReferenceQueue;

/**
 * A weakly-held CleanupReference.
 */
public abstract class WeakCleanupReference<T> extends WeakSimpleReference<T> implements CleanupReference<T> {

    public WeakCleanupReference(final T referent, final ReferenceQueue<? super T> referenceQueue) {
        super(referent, referenceQueue);
    }
}
