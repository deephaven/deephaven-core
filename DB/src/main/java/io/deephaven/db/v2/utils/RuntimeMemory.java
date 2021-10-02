package io.deephaven.db.v2.utils;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.internal.log.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Cache memory utilization.
 *
 * <p>
 * >Calling Runtime.getRuntime().getFreeMemory() is expensive; and we may do it a lot when we have automatically
 * computed tables, such as in a byExternal. Instead of calling the runtime directly from the performance
 * instrumentation framework, we call this class's methods; which cache the result for a configurable number of
 * milliseconds to avoid repeated calls that are not likely any different./p>
 *
 * <p>
 * A dditionally, we log our JVM heap usage on a regular basis; to enable users to quickly examine their worker logs and
 * understand memory issues.
 * </p>
 */
public class RuntimeMemory {
    /** The singleton instance. */
    private static volatile RuntimeMemory instance;

    /** The logger for Jvm heap lines */
    private final Logger log;
    /** The singleton runtime object. */
    private final Runtime runtime;
    /** The format object for heap bytes */
    private final DecimalFormat commaFormat;

    /** How long between fetches of the Runtime memory (in ms). */
    private final int cacheInterval;
    /** How long between logging Jvm Heap lines (in ms). */
    private final int logInterval;

    /** When should we next check for free/total memory. */
    private volatile long nextCheck;
    /** When should we next log free/total/max memory. */
    private volatile long nextLog;
    /** What is the last free memory value we retrieved from the Runtime. */
    private volatile long lastFreeMemory;
    /** What is the last total memory value we retrieved from the Runtime. */
    private volatile long lastTotalMemory;

    /** The total number of GC collections */
    private volatile long totalCollections;
    /** The approximated total time of GC collections, in milliseconds. */
    private volatile long totalCollectionTimeMs;

    /** The runtime provided max memory (at startup, because it should not change). */
    private final long maxMemory;

    private List<GarbageCollectorMXBean> gcBeans;

    private RuntimeMemory(Logger log) {
        this.log = log;
        this.runtime = Runtime.getRuntime();
        logInterval = Configuration.getInstance().getIntegerWithDefault("RuntimeMemory.logIntervalMillis", 60 * 1000);
        this.nextLog = System.currentTimeMillis() + logInterval;
        cacheInterval = Configuration.getInstance().getIntegerWithDefault("RuntimeMemory.cacheIntervalMillis", 1);
        maxMemory = runtime.maxMemory();

        commaFormat = new DecimalFormat();
        commaFormat.setGroupingUsed(true);

        totalCollections = 0;
        totalCollectionTimeMs = 0;
        // Technically these /could/ change any time; we assume they don't.
        gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
    }

    /**
     * Return a singleton RuntimeMemory object.
     *
     * @return the RuntimeMemory singleton
     */
    public static RuntimeMemory getInstance() {
        // I don't really care if we make more than one and overwrite this at startup.
        if (instance == null) {
            instance = new RuntimeMemory(LoggerFactory.getLogger(RuntimeMemory.class));
        }
        return instance;
    }

    /**
     * See {@link Runtime#freeMemory()}.
     */
    public long freeMemory() {
        maybeUpdateValues();
        return lastFreeMemory;
    }

    /**
     * See {@link Runtime#totalMemory()}.
     */
    public long totalMemory() {
        maybeUpdateValues();
        return lastTotalMemory;
    }

    public long totalCollections() {
        maybeUpdateValues();
        return totalCollections;
    }

    public long getTotalCollectionTimeMs() {
        maybeUpdateValues();
        return totalCollectionTimeMs;
    }

    public static class Sample {
        public long freeMemory;
        public long totalMemory;
        public long totalCollections;
        public long totalCollectionTimeMs;

        void reset() {
            freeMemory = totalMemory = totalCollections = totalCollectionTimeMs = 0L;
        }

        void copy(final Sample s) {
            freeMemory = s.freeMemory;
            totalMemory = s.totalMemory;
            totalCollections = s.totalCollections;
            totalCollectionTimeMs = s.totalCollectionTimeMs;
        }

    }

    public void read(Sample buf) {
        maybeUpdateValues();
        buf.freeMemory = lastFreeMemory;
        buf.totalMemory = lastTotalMemory;
        buf.totalCollections = totalCollections;
        buf.totalCollectionTimeMs = totalCollectionTimeMs;
    }

    /**
     * See {@link Runtime#maxMemory()}.
     */
    long getMaxMemory() {
        return maxMemory;
    }

    /**
     * If we are past the deadline for fetching free memory values, fetch the values.
     *
     * Additionally, if we are past the deadline for logging; log the values.
     */
    private void maybeUpdateValues() {
        final long now = System.currentTimeMillis();
        if (now >= nextCheck) {
            lastFreeMemory = runtime.freeMemory();
            lastTotalMemory = runtime.totalMemory();
            nextCheck = now + cacheInterval;
            long collections = 0;
            long collectionsMs = 0;
            for (final GarbageCollectorMXBean gcBean : gcBeans) {
                collections += gcBean.getCollectionCount();
                collectionsMs += gcBean.getCollectionTime();
            }
            totalCollections = collections;
            totalCollectionTimeMs = collectionsMs;
        }
        if (logInterval > 0 && now >= nextLog) {
            log.info().append("Jvm Heap: ").append(commaFormat.format(lastFreeMemory)).append(" Free / ")
                    .append(commaFormat.format(lastTotalMemory)).append(" Total (")
                    .append(commaFormat.format(maxMemory)).append(" Max)").endl();
            nextLog = now + logInterval;
        }
    }
}
