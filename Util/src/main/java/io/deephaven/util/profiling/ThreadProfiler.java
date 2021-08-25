package io.deephaven.util.profiling;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.QueryConstants;

/**
 * Interface for thread profiling utilities which may be platform or JVM dependent.
 */
public interface ThreadProfiler {

    boolean CPU_PROFILING_ENABLED = Configuration.getInstance().getBooleanForClassWithDefault(ThreadProfiler.class,
            "cpuProfilingEnabled", true);
    boolean MEMORY_PROFILING_ENABLED = Configuration.getInstance().getBooleanForClassWithDefault(ThreadProfiler.class,
            "memoryProfilingEnabled", true);

    /**
     * Check if memory profiling (e.g. {@link #getCurrentThreadAllocatedBytes()}) is available (supported and enabled).
     *
     * @return Whether memory profiling is available.
     */
    boolean memoryProfilingAvailable();

    /**
     * Get the approximate number of total bytes allocated by the current thread.
     *
     * @return The approximate number of total bytes allocated by the current thread, or
     *         {@link QueryConstants#NULL_LONG} if unavailable.
     */
    long getCurrentThreadAllocatedBytes();

    /**
     * Check if CPU profiling (e.g. {@link #getCurrentThreadCpuTime()} and {@link #getCurrentThreadUserTime()}) is
     * available (supported and enabled).
     *
     * @return Whether CPU profiling is available.
     */
    boolean cpuProfilingAvailable();

    /**
     * Get the approximate number of total nanoseconds the current thread has executed (in system or user mode) since
     * CPU time measurement started.
     *
     * @return The approximate number of total nanoseconds the current thread has executed, or
     *         {@link QueryConstants#NULL_LONG} if unavailable.
     */
    long getCurrentThreadCpuTime();

    /**
     * Get the approximate number of total nanoseconds the current thread has executed (in user mode) since CPU time
     * measurement started.
     *
     * @return The approximate number of total nanoseconds the current thread has executed in user mode, or
     *         {@link QueryConstants#NULL_LONG} if unavailable.
     */
    long getCurrentThreadUserTime();

    /**
     * Make a new ThreadProfiler for this JVM. The result may not support all measurements, if there's no suitable
     * implementation available.
     *
     * @return A new ThreadProfiler for this JVM.
     */
    static ThreadProfiler make() {
        if (!CPU_PROFILING_ENABLED && !MEMORY_PROFILING_ENABLED) {
            return NullThreadProfiler.INSTANCE;
        }
        final String vendor = System.getProperty("java.vendor");
        if (vendor.contains("Sun") || vendor.contains("Oracle") || vendor.contains("OpenJDK")) {
            return new SunThreadMXBeanThreadProfiler();
        }
        return new BaselineThreadMXBeanThreadProfiler();
    }

    /**
     * Default instance for almost all usage.
     */
    ThreadProfiler DEFAULT = make();
}
