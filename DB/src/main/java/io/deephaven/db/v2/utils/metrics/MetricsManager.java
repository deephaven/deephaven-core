package io.deephaven.db.v2.utils.metrics;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.internal.log.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Consumer;

public class MetricsManager {
    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    public static final boolean enabled = Configuration.getInstance().getBooleanForClassWithDefault(
            MetricsManager.class, "enabled", false);
    private static final boolean toStdout = Configuration.getInstance().getBooleanForClassWithDefault(
            MetricsManager.class, "toStdout", false);
    private static final long logPeriodNanos =
            1_000_000_000L * Configuration.getInstance().getIntegerForClassWithDefault(
                    MetricsManager.class, "logPeriodSeconds", 120);
    private static final boolean periodicUpdates = Configuration.getInstance().getBooleanForClassWithDefault(
            MetricsManager.class, "periodicUpdates", false);

    private static final int maxMetricsPerType =
            Configuration.getInstance().getIntegerForClassWithDefault(MetricsManager.class, "maxMetricsPerType", 256);

    // This class is a singleton.
    public static final MetricsManager instance = new MetricsManager();

    private MetricsManager() {}

    private static final Consumer<String> logger = (final String s) -> {
        if (toStdout) {
            System.out.println(s);
        } else {
            log.info(s);
        }
    };

    private static abstract class MetricsFamily<ArrayType> {
        protected abstract ArrayType makeMetricsArray();

        protected abstract String familyName();

        protected abstract void clear(ArrayType dst, int size);

        protected abstract void accumulateSnapshot(ArrayType src, ArrayType dst, int size);

        protected abstract void log(String updateTag, Consumer<String> logger);

        // The member variables in the following block are accessed synchronized(this).
        // They are updated atomically together when metrics are added.
        protected int count;
        protected final String[] names = new String[maxMetricsPerType];
        protected final TObjectIntMap<String> nameToMetricId = new TObjectIntHashMap<>(maxMetricsPerType);

        // Threads have their own version of the counters in a ThreadLocal.
        // We need to know all of them to do updates and other operations,
        // so we keep them in a collection.
        // Reading and writing to this collection is done synchronizing on the collection object itself.
        // TODO: We should consider using a container of weak references to avoid holding to counters for threads that
        // won't update them anymore; that means a fair bit of additional book keeping tho since those counts would have
        // to live somewhere (a separate deceased count?) plus we would need to reclaim weak references gone null etc.
        private final ArrayList<ArrayType> pendingPerThreadCounters = new ArrayList<>(maxMetricsPerType);
        private ThreadLocal<ArrayType> counters = ThreadLocal.withInitial(() -> {
            if (!enabled) {
                return null;
            }
            final ArrayType threadMetrics = makeMetricsArray();
            synchronized (pendingPerThreadCounters) {
                pendingPerThreadCounters.add(threadMetrics);
            }
            return threadMetrics;
        });

        int registerMetric(final String name) {
            if (!enabled) {
                return 0;
            }
            synchronized (this) {
                if (count == maxMetricsPerType) {
                    throw new IllegalStateException(
                            "Max number of " + familyName() + " metrics (=" + maxMetricsPerType + ") already reached!");
                }
                if (nameToMetricId.containsKey(name)) {
                    throw new IllegalArgumentException(familyName() + " name=" + name + " already exists!");
                }
                final int id = count++;
                nameToMetricId.put(name, id);
                names[id] = name;
                return id;
            }
        }

        // The member variables in the block below are only accessed from the timer thread.
        protected final ArrayType countersSnapshot = makeMetricsArray();
        protected int snapshotCount;
        protected final String[] namesSortedSnapshot = new String[maxMetricsPerType]; // we log in alphabetical metric
                                                                                      // name order.
        protected final ArrayList<ArrayType> perThreadCounters = new ArrayList<>();

        void snapshotCounters() {
            boolean needsToSort = false;
            synchronized (this) {
                if (count > snapshotCount) {
                    // get what we are missing from the previous go around.
                    System.arraycopy(
                            names, snapshotCount,
                            namesSortedSnapshot, snapshotCount,
                            count - snapshotCount);
                    snapshotCount = count;
                    needsToSort = true;
                }
            }

            if (needsToSort) {
                Arrays.sort(namesSortedSnapshot, 0, snapshotCount);
            }
            clear(countersSnapshot, snapshotCount);

            // A new metric may be inserted after we snapshot above and before
            // we check/copy new per threads counters below; that's fine.
            synchronized (pendingPerThreadCounters) {
                if (!pendingPerThreadCounters.isEmpty()) {
                    perThreadCounters.addAll(pendingPerThreadCounters);
                    pendingPerThreadCounters.clear();
                }
            }

            // Note we don't have any protection against races here:
            // we don't use synchronized blocks neither we access volatile variables.
            // Strictly speaking, in the Java Memory Model we are not guaranteed to see any updates.
            // We do know however that this code will see updates in an Intel x64 + HotSpot platform,
            // due to how that particular implementation is known to work.
            for (final ArrayType threadCounters : perThreadCounters) {
                accumulateSnapshot(threadCounters, countersSnapshot, snapshotCount);
            }
        }

        // Note this is very crude and not intended to implement rate-type counters;
        // it is intended only for restart-of-test-iteration type scenarios (eg, JMH benchmark iteration teardown).
        // The correct implementation of rate-type counters needs to avoid losing updates;
        // the looping below is prone to lose updates: a value not included in the previous update
        // might be cleared thus preventing it from being included in the next update.
        // Proper implementation of rate-type counters can be done with a double-buffering approach, atomic-swapping
        // of a second, zeroed counters array buffer during update.
        // We avoid the additional complexity and performance costs of that since we don't have a need for rate
        // counters at the moment.
        void bluntResetCounters() {
            final int size;
            synchronized (this) {
                size = count;
            }
            synchronized (pendingPerThreadCounters) {
                for (final ArrayType threadCounters : perThreadCounters) {
                    clear(threadCounters, size);
                }
            }
        }
    }

    private static abstract class MetricsCounterFamily<ArrayType> extends MetricsFamily<ArrayType> {
        protected abstract long get(ArrayType counters, final int i);

        @Override
        protected void log(final String updateTag, final Consumer<String> logger) {
            final String prefix = "Metrics " + familyName() + " " + updateTag + " update: ";
            if (snapshotCount == 0) {
                logger.accept(prefix + "No counters defined.");
                return;
            }
            final StringBuilder sb = new StringBuilder(prefix);
            final TObjectIntMap<String> nameToMetricIdCopy;
            synchronized (this) {
                nameToMetricIdCopy = new TObjectIntHashMap<>(nameToMetricId);
            }
            for (int i = 0; i < snapshotCount; ++i) {
                final String name = namesSortedSnapshot[i];
                final int metricId = nameToMetricIdCopy.get(name);
                final long v = get(countersSnapshot, metricId);
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(name).append('=').append(v);
            }
            logger.accept(sb.toString());
        }
    }

    private final MetricsFamily<int[]> intCounterMetrics = new MetricsCounterFamily<int[]>() {
        @Override
        protected int[] makeMetricsArray() {
            return new int[maxMetricsPerType];
        }

        @Override
        protected long get(final int[] counters, final int i) {
            return counters[i];
        }

        @Override
        protected String familyName() {
            return "IntCounter";
        }

        @Override
        protected void clear(final int[] counters, final int size) {
            for (int i = 0; i < size; ++i) {
                counters[i] = 0;
            }
        }

        @Override
        protected void accumulateSnapshot(final int[] src, final int[] dst, final int size) {
            for (int i = 0; i < size; ++i) {
                dst[i] += src[i];
            }
        }
    };

    private final MetricsFamily<long[]> longCounterMetrics = new MetricsCounterFamily<long[]>() {
        @Override
        protected long[] makeMetricsArray() {
            return new long[maxMetricsPerType];
        }

        @Override
        protected long get(final long[] counters, final int i) {
            return counters[i];
        }

        @Override
        protected String familyName() {
            return "LongCounter";
        }

        @Override
        protected void clear(final long[] counters, final int size) {
            for (int i = 0; i < size; ++i) {
                counters[i] = 0;
            }
        }

        @Override
        protected void accumulateSnapshot(final long[] src, final long[] dst, final int size) {
            for (int i = 0; i < size; ++i) {
                dst[i] += src[i];
            }
        }
    };

    private final MetricsFamily<int[][]> longCounterLog2HistogramMetrics = new MetricsFamily<int[][]>() {
        @Override
        protected int[][] makeMetricsArray() {
            return new int[maxMetricsPerType][65];
        }

        @Override
        protected String familyName() {
            return "LongCounterLog2Histogram";
        }

        @Override
        protected void clear(final int[][] counters, final int size) {
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < 65; ++j) {
                    counters[i][j] = 0;
                }
            }
        }

        @Override
        protected void accumulateSnapshot(final int[][] src, final int[][] dst, final int size) {
            for (int i = 0; i < size; ++i) {
                for (int j = 0; j < 65; ++j) {
                    dst[i][j] += src[i][j];
                }
            }
        }

        @Override
        protected void log(final String updateTag, final Consumer<String> logger) {
            final String prefix = "Metrics " + familyName() + " " + updateTag + ": ";
            if (snapshotCount == 0) {
                final String s = prefix + "No counters defined.";
                logger.accept(s);
                return;
            }
            final TObjectIntHashMap nameToMetricIdCopy;
            synchronized (this) {
                nameToMetricIdCopy = new TObjectIntHashMap<>(nameToMetricId);
            }
            for (int i = 0; i < snapshotCount; ++i) {
                long nsamples = 0;
                final StringBuilder sb = new StringBuilder(prefix);
                final String name = namesSortedSnapshot[i];
                // We will log our histogram as a sequence of strings "msb:count"
                // where count is the total number of times samples with
                // msb as its most significant bit were sampled.
                // For instance: in the output "[ ..., 3:7, ...]" the element
                // "3:7" means values x such that 2^3 <= x < 2^4 were sampled 7 times.
                // In histogram terms, 7 values in the interval [ 2^3, 2^4 - 1 ] were sampled.
                final String key = "|key: i:n => 2^i <= x < 2^(i+1), z:n => x = 0.| ";
                sb.append(key).append(name).append("={ ");
                final int metricId = nameToMetricIdCopy.get(name);
                boolean haveBefore = false;
                for (int j = 64; j >= 0; --j) {
                    final int v = countersSnapshot[metricId][j];
                    if (v == 0) {
                        continue;
                    }
                    if (haveBefore) {
                        sb.append(", ");
                    }
                    if (j == 64) {
                        sb.append("z");
                    } else {
                        final int msb = 63 - j;
                        sb.append(msb);
                    }
                    sb.append(":").append(v);
                    haveBefore = true;
                    nsamples += v;
                }
                sb.append(" }, nsamples=").append(nsamples);
                logger.accept(sb.toString());
            }
        }
    };

    int registerIntCounterMetric(final String name) {
        if (!enabled) {
            return 0;
        }
        timerThread.ensureStarted();
        return intCounterMetrics.registerMetric(name);
    }

    int registerLongCounterMetric(final String name) {
        if (!enabled) {
            return 0;
        }
        timerThread.ensureStarted();
        return longCounterMetrics.registerMetric(name);
    }

    int registerLongCounterLog2HistogramMetric(final String name) {
        if (!enabled) {
            return 0;
        }
        timerThread.ensureStarted();
        return longCounterLog2HistogramMetrics.registerMetric(name);
    }

    // This is part of the fast path. We should avoid as much as possible holding up the calling thread.
    void sampleIntCounter(final int id, final long n) {
        if (!enabled) {
            return;
        }
        final int[] threadMetrics = intCounterMetrics.counters.get();
        threadMetrics[id] += n;
    }

    // This is part of the fast path. We should avoid as much as possible holding up the calling thread.
    void sampleLongCounter(final int id, final long n) {
        if (!enabled) {
            return;
        }
        final long[] threadMetrics = longCounterMetrics.counters.get();
        threadMetrics[id] += n;
    }

    // This is part of the fast path. We should avoid as much as possible holding up the calling thread.
    void sampleLongCounterLog2HistogramCount(final int id, final long v) {
        if (!enabled) {
            return;
        }
        final int[][] threadMetrics = longCounterLog2HistogramMetrics.counters.get();
        ++threadMetrics[id][Long.numberOfLeadingZeros(v)];
    }

    public void update(final String updateTag) {
        update(updateTag, logger);
    }

    public void update(final String updateTag, final Consumer<String> logger) {
        intCounterMetrics.snapshotCounters();
        longCounterMetrics.snapshotCounters();
        longCounterLog2HistogramMetrics.snapshotCounters();
        intCounterMetrics.log(updateTag, logger);
        longCounterMetrics.log(updateTag, logger);
        longCounterLog2HistogramMetrics.log(updateTag, logger);
    }

    // Note this is very crude and not intended to implement rate-type counters;
    // it is intended only for restart-of-test-iteration type scenarios (eg, JMH benchmark iteration teardown)
    // See comment in the implementation of the methods called below.
    public void bluntResetAllCounters() {
        intCounterMetrics.bluntResetCounters();
        longCounterMetrics.bluntResetCounters();
        longCounterLog2HistogramMetrics.bluntResetCounters();
    }

    private static class TimerThread extends Thread {
        private boolean running = false;
        private volatile boolean shutdownRequested = false;

        public TimerThread() {
            setDaemon(true);
        }

        public boolean shouldBeRunning() {
            return enabled && periodicUpdates && !shutdownRequested;
        }

        public void ensureStarted() {
            if (!shouldBeRunning()) {
                return;
            }
            synchronized (this) {
                if (!shouldBeRunning()) {
                    return;
                }
                start();
                running = true;
            }
        }

        @Override
        public void run() {
            logger.accept(MetricsManager.class.getSimpleName() + ": Starting periodic updates.");
            final String updateStr = "periodic";
            MetricsManager.instance.update(updateStr);
            long nowNanos = System.nanoTime();
            long lastPeriodNanosTs = nowNanos;
            long nextUpdateNanos = lastPeriodNanosTs + logPeriodNanos;
            while (!shutdownRequested) {
                if (nowNanos < nextUpdateNanos) {
                    final long sleepMillis = (nextUpdateNanos - nowNanos) / 1_000_000;
                    try {
                        Thread.sleep(sleepMillis);
                    } catch (InterruptedException e) {
                        /* Ignored, but forces us to recheck if we need to shutdown */ }
                    nowNanos = System.nanoTime();
                    continue;
                }
                MetricsManager.instance.update(updateStr);
                nowNanos = System.nanoTime();
                lastPeriodNanosTs = nowNanos;
                nextUpdateNanos = lastPeriodNanosTs + logPeriodNanos;
            }
        }

        public void shutdown() {
            if (!enabled || !periodicUpdates) {
                return;
            }
            synchronized (this) {
                if (shutdownRequested) {
                    return;
                }
                shutdownRequested = true;
                if (!running) {
                    return;
                }
            }
            try {
                interrupt();
                join();
            } catch (InterruptedException e) {
                /* Ignored */ } finally {
                running = false;
            }
        }
    }

    private final TimerThread timerThread = new TimerThread();

    public void stopPeriodicUpdates() {
        logger.accept(MetricsManager.class.getSimpleName() + ": Period updates stopped.");
        timerThread.shutdown();
    }

    public void noPeriodicUpdates() {
        stopPeriodicUpdates();
    }

    public static void resetCounters() {
        instance.bluntResetAllCounters();
    }

    public static void updateCounters() {
        updateCounters("");
    }

    public static String getCounters() {
        return getCounters("");
    }

    private static void updateCounters(final String m) {
        instance.update(m);
    }

    private static String getCounters(final String m) {
        final StringBuilder sb = new StringBuilder();
        instance.update(m, (final String s) -> sb.append(s).append('\n'));
        return sb.toString();
    }
}
