//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.locks;

import java.util.concurrent.locks.Lock;

/**
 * Extension to the {@link Lock} interface to make awareness of the current thread's state accessible.
 */
public interface AwareLock extends Lock {

    /**
     * Query whether the current thread holds this lock.
     *
     * @return True if the current thread holds this lock, else false
     */
    boolean isHeldByCurrentThread();
}
