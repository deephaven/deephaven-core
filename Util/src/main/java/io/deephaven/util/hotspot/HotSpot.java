package io.deephaven.util.hotspot;

import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceLoader;

public interface HotSpot {
    Optional<HotSpot> instance = loadImpl();

    private static Optional<HotSpot> loadImpl() {
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

    long getSafepointCount();

    long getTotalSafepointTimeMillis();
}
