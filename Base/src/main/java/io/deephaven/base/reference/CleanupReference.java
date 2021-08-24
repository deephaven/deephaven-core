/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.reference;

/**
 * SimpleReference with an additional cleanup callback.
 */
public interface CleanupReference<T> extends SimpleReference<T> {

    /**
     * Allow for a reference queue consumer to invoke a custom cleanup method, for post-GC resource
     * reclamation.
     */
    void cleanup();
}
