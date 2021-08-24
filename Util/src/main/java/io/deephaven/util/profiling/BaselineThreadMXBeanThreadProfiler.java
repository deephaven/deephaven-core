package io.deephaven.util.profiling;

import io.deephaven.util.QueryConstants;

import java.lang.management.ThreadMXBean;

/**
 * A concrete generic {@link ThreadMXBean}-based {@link ThreadProfiler} implementation, with support
 * for baseline measurements available on all JVMs <em>only</em>.
 */
public final class BaselineThreadMXBeanThreadProfiler
    extends ThreadMXBeanThreadProfiler<ThreadMXBean> {

    @Override
    public final boolean memoryProfilingAvailable() {
        return false;
    }

    @Override
    public final long getCurrentThreadAllocatedBytes() {
        return QueryConstants.NULL_LONG;
    }
}
