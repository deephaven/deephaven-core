//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.hotspot;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public interface HotSpot {
    static Optional<HotSpot> loadImpl() {
        final Iterator<HotSpot> it = ServiceLoader.load(HotSpot.class).iterator();
        if (!it.hasNext()) {
            return Optional.empty();
        }
        final HotSpot impl = it.next();
        if (it.hasNext()) {
            throw new IllegalStateException("Found multiple implementations for " + HotSpot.class.getSimpleName());
        }
        return Optional.of(impl);
    }

    /**
     * Returns the number of safepoints taken place since the Java virtual machine started.
     *
     * @return the number of safepoints taken place since the Java virtual machine started.
     */
    long getSafepointCount();

    /**
     * Returns the accumulated time spent at safepoints in milliseconds. This is the accumulated elapsed time that the
     * application has been stopped for safepoint operations.
     *
     * @return the accumulated time spent at safepoints in milliseconds.
     */
    long getTotalSafepointTimeMillis();

    /**
     * Returns the accumulated time spent getting to safepoints in milliseconds.
     *
     * @return the accumulated time spent getting to safepoints in milliseconds.
     */
    long getSafepointSyncTimeMillis();
}
