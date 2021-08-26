package io.deephaven.benchmarking.runner;

import org.jetbrains.annotations.NotNull;

public class StatsGatherer implements Runnable {
    private static final long SAMPLE_INTERVAL = 10; // milliseconds to sleep between samples
    private final ProfilerStats stats;

    public StatsGatherer(@NotNull final ProfilerStats stats) {
        this.stats = stats;
    }

    private double max(double current_value, double new_value) {
        if (current_value < new_value) {
            return new_value;
        } else {
            return current_value;
        }
    }

    private long max(long current_value, long new_value) {
        if (current_value < new_value) {
            return new_value;
        } else {
            return current_value;
        }
    }

    private void getStats() {
        stats.totalHeap = max(stats.totalHeap, Runtime.getRuntime().totalMemory());
        stats.freeHeap = max(stats.freeHeap, Runtime.getRuntime().freeMemory());
        stats.usedHeap = max(stats.usedHeap,
                java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        stats.activeThreads = max(stats.activeThreads, Thread.activeCount() - 1);
        stats.cpuLoad = max(stats.cpuLoad,
                java.lang.management.ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage() /
                        java.lang.management.ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors()
                        * 100.0);
    }

    public void run() {
        getStats();
        boolean keepGoing = true;
        while (keepGoing) {
            try {
                Thread.sleep(SAMPLE_INTERVAL);
            } catch (InterruptedException e) {
                keepGoing = false;
                getStats();
            }
        }
    }
}
