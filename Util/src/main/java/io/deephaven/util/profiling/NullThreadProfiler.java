package io.deephaven.util.profiling;

import io.deephaven.util.QueryConstants;

/**
 * The "null" {@link ThreadProfiler} implementation, which supports no actual measurements.
 */
public final class NullThreadProfiler implements ThreadProfiler {

    public static final ThreadProfiler INSTANCE = new NullThreadProfiler();

    private NullThreadProfiler() {}

    @Override
    public boolean memoryProfilingAvailable() {
        return false;
    }

    @Override
    public final long getCurrentThreadAllocatedBytes() {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public boolean cpuProfilingAvailable() {
        return false;
    }

    @Override
    public final long getCurrentThreadCpuTime() {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public final long getCurrentThreadUserTime() {
        return QueryConstants.NULL_LONG;
    }
}
