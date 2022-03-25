package io.deephaven.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Utility class to facilitate obtaining the number of garbage collections and the time in garbage collection
 * between two points in code.
 */
public class GcIntrospectionContext {
    public final List<GarbageCollectorMXBean> gcBeans;
    private long lastStartTotalCollections;
    private long lastStartTotalCollectionsTimeMs;
    private long lastEndTotalCollections;
    private long lastEndTotalCollectionsTimeMs;

    public GcIntrospectionContext() {
        // Technically these /could/ change any time; we assume they don't.
        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    }

    public void startSample() {
        long collections = 0;
        long collectionsMs = 0;
        for (final GarbageCollectorMXBean gcBean : gcBeans) {
            collections += gcBean.getCollectionCount();
            collectionsMs += gcBean.getCollectionTime();
        }
        lastStartTotalCollections = collections;
        lastStartTotalCollectionsTimeMs = collectionsMs;
    }
    /**
     * Sample garbage collection count and times at the point of call.
     */
    public void endSample() {
        long collections = 0;
        long collectionsMs = 0;
        for (final GarbageCollectorMXBean gcBean : gcBeans) {
            collections += gcBean.getCollectionCount();
            collectionsMs += gcBean.getCollectionTime();
        }
        lastEndTotalCollections = collections;
        lastEndTotalCollectionsTimeMs = collectionsMs;
    }

    /**
     * Number of collections between the last two calls to {@code sample()}
     * @return Number of collections
     */
    public long deltaCollections() {
        return lastEndTotalCollections - lastStartTotalCollections;
    }

    /**
     * Time in milliseconds in collections between the last two calls to {@code sample()}
     * @return Time in milliseconds
     */
    public long deltaCollectionTimeMs() {
        return lastEndTotalCollectionsTimeMs - lastStartTotalCollectionsTimeMs;
    }
}
