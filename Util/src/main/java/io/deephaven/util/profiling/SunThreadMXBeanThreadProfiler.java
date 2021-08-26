package io.deephaven.util.profiling;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.VisibleForTesting;

import java.lang.management.ThreadMXBean;

/**
 * A {@link ThreadMXBean}-based {@link ThreadProfiler} implementation for use on Oracle and OpenJDK JVMs, adding support
 * for memory measurements.
 */
public final class SunThreadMXBeanThreadProfiler extends ThreadMXBeanThreadProfiler<com.sun.management.ThreadMXBean> {

    @VisibleForTesting
    static final boolean TRY_ENABLE_THREAD_ALLOCATED_MEMORY = Configuration.getInstance()
            .getBooleanForClassWithDefault(SunThreadMXBeanThreadProfiler.class, "tryEnableThreadAllocatedMemory", true);

    // NB: This class may need to be moved to a JDK-specific source set at some future date, if and when we add support
    // to compile on other JDKs.

    /**
     * Whether thread allocated memory measurements are supported.
     */
    private final boolean memoryProfilingAvailable;

    public SunThreadMXBeanThreadProfiler() {
        if (!MEMORY_PROFILING_ENABLED) {
            memoryProfilingAvailable = false;
            return;
        }

        if (threadMXBean.isThreadAllocatedMemorySupported() && !threadMXBean.isThreadAllocatedMemoryEnabled()
                && TRY_ENABLE_THREAD_ALLOCATED_MEMORY) {
            try {
                threadMXBean.setThreadAllocatedMemoryEnabled(true);
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(
                        "Failed to enable thread allocated memory - set SunThreadMXBeanThreadProfiler.tryEnableThreadAllocatedMemory=false to proceed without it",
                        e);
            }
        }
        memoryProfilingAvailable =
                threadMXBean.isThreadAllocatedMemorySupported() && threadMXBean.isThreadAllocatedMemoryEnabled();
    }

    @Override
    public final boolean memoryProfilingAvailable() {
        return memoryProfilingAvailable;
    }

    @Override
    public final long getCurrentThreadAllocatedBytes() {
        if (!memoryProfilingAvailable) {
            return QueryConstants.NULL_LONG;
        }
        final long rawResult = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        return rawResult < 0 ? QueryConstants.NULL_LONG : rawResult;
    }
}
