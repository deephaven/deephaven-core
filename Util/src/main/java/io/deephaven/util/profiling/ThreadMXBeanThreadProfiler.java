package io.deephaven.util.profiling;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.VisibleForTesting;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * An abstract generic {@link ThreadMXBean}-based {@link ThreadProfiler} implementation, with
 * support for baseline measurements available on all JVMs.
 */
public abstract class ThreadMXBeanThreadProfiler<BEAN_TYPE extends ThreadMXBean>
    implements ThreadProfiler {

    @VisibleForTesting
    static final boolean TRY_ENABLE_THREAD_CPU_TIME =
        Configuration.getInstance().getBooleanForClassWithDefault(ThreadMXBeanThreadProfiler.class,
            "tryEnableThreadCpuTime", true);

    /**
     * The bean for measurements.
     */
    protected final BEAN_TYPE threadMXBean;

    /**
     * Whether current-thread CPU and User Mode CPU time measurements are supported and enabled.
     */
    private final boolean cpuProfilingAvailable;

    protected ThreadMXBeanThreadProfiler() {
        // noinspection unchecked
        threadMXBean = (BEAN_TYPE) ManagementFactory.getThreadMXBean();

        if (!CPU_PROFILING_ENABLED) {
            cpuProfilingAvailable = false;
            return;
        }

        if (threadMXBean.isCurrentThreadCpuTimeSupported() && !threadMXBean.isThreadCpuTimeEnabled()
            && TRY_ENABLE_THREAD_CPU_TIME) {
            try {
                threadMXBean.setThreadCpuTimeEnabled(true);
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(
                    "Failed to enable thread cpu time - set ThreadMXBeanThreadProfiler.tryEnableThreadCpuTime=false to proceed without it",
                    e);
            }
        }
        cpuProfilingAvailable =
            threadMXBean.isCurrentThreadCpuTimeSupported() && threadMXBean.isThreadCpuTimeEnabled();
    }

    @Override
    public final boolean cpuProfilingAvailable() {
        return cpuProfilingAvailable;
    }

    @Override
    public final long getCurrentThreadCpuTime() {
        if (!cpuProfilingAvailable) {
            return QueryConstants.NULL_LONG;
        }
        final long rawResult = threadMXBean.getCurrentThreadCpuTime();
        return rawResult < 0 ? QueryConstants.NULL_LONG : rawResult;
    }

    @Override
    public final long getCurrentThreadUserTime() {
        if (!cpuProfilingAvailable) {
            return QueryConstants.NULL_LONG;
        }
        final long rawResult = threadMXBean.getCurrentThreadUserTime();
        return rawResult < 0 ? QueryConstants.NULL_LONG : rawResult;
    }
}
