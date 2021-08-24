package io.deephaven.util.profiling;

import io.deephaven.util.QueryConstants;
import com.sun.management.ThreadMXBean;
import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link ThreadProfiler}.
 */
public class TestThreadProfiler {

    private static ThreadMXBean threadMXBean;
    private static boolean oldThreadCpuTimeEnabled;
    private static boolean oldThreadAllocatedMemoryEnabled;
    private static ThreadProfiler SUT;

    /**
     * Depending on the environment and architecture, it can be very hard to have strict guarantees
     * about predictable memory allocation or CPU usage. Rather than introduce unpredictable tests
     * to CI, we disable such assertions unless specifically desired.
     */
    private static final boolean STRICT_MODE = false;

    @BeforeClass
    public static void setUpOnce() {
        threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final ThreadMXBean threadMXBean =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (threadMXBean.isCurrentThreadCpuTimeSupported()) {
            if (ThreadMXBeanThreadProfiler.TRY_ENABLE_THREAD_CPU_TIME
                && (oldThreadCpuTimeEnabled = threadMXBean.isThreadCpuTimeEnabled())) {
                threadMXBean.setThreadCpuTimeEnabled(false);
            }
        }
        if (threadMXBean.isThreadAllocatedMemorySupported()) {
            if (SunThreadMXBeanThreadProfiler.TRY_ENABLE_THREAD_ALLOCATED_MEMORY
                && (oldThreadAllocatedMemoryEnabled =
                    threadMXBean.isThreadAllocatedMemoryEnabled())) {
                threadMXBean.setThreadAllocatedMemoryEnabled(false);
            }
        }
        SUT = ThreadProfiler.make();
    }

    @AfterClass
    public static void tearDownOnce() {
        if (threadMXBean.isCurrentThreadCpuTimeSupported()
            && ThreadMXBeanThreadProfiler.TRY_ENABLE_THREAD_CPU_TIME && oldThreadCpuTimeEnabled) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }
        if (threadMXBean.isThreadAllocatedMemorySupported()
            && SunThreadMXBeanThreadProfiler.TRY_ENABLE_THREAD_ALLOCATED_MEMORY
            && oldThreadAllocatedMemoryEnabled) {
            threadMXBean.setThreadAllocatedMemoryEnabled(true);
        }
    }

    @Test
    public void testMemoryMeasurement() {
        if (!SUT.memoryProfilingAvailable()) {
            System.out.println("TestThreadProfiler: Memory measurement unsupported");
            return;
        }

        final List<Object> items = new ArrayList<>(10);
        final int count = 10;
        final int size = 1024;
        final long startBytes = SUT.getCurrentThreadAllocatedBytes();
        TestCase.assertFalse(startBytes == QueryConstants.NULL_LONG);

        for (int ii = 0; ii < count; ++ii) {
            items.add(new byte[size]);
        }

        final long endBytes = SUT.getCurrentThreadAllocatedBytes();
        TestCase.assertFalse(endBytes == QueryConstants.NULL_LONG);
        final long allocatedBytes = endBytes - startBytes;

        System.out.println("TestThreadProfiler: Allocated " + allocatedBytes + " with items hash "
            + items.hashCode());
        if (STRICT_MODE) {
            final long minimumExpectedBytes = count * size;
            final long maximumExpectedBytes = count * (128 + size);
            TestCase.assertTrue(
                allocatedBytes >= minimumExpectedBytes && allocatedBytes <= maximumExpectedBytes);
        } else {
            TestCase.assertTrue(allocatedBytes >= 0);
        }
    }

    @Test
    public void testCpuMeasurement() {
        if (!SUT.cpuProfilingAvailable()) {
            System.out.println("TestThreadProfiler: CPU measurement unsupported");
            return;
        }

        final long startCpuNanos = SUT.getCurrentThreadCpuTime();
        TestCase.assertFalse(startCpuNanos == QueryConstants.NULL_LONG);
        final long startUserNanos = SUT.getCurrentThreadUserTime();
        TestCase.assertFalse(startUserNanos == QueryConstants.NULL_LONG);

        long fib_prev = 0;
        long fib_curr = 1;
        for (int ii = 2; ii <= 92; ++ii) {
            final long old_fib_curr = fib_curr;
            fib_curr = fib_curr + fib_prev;
            fib_prev = old_fib_curr;
        }

        final long endUserNanos = SUT.getCurrentThreadUserTime();
        TestCase.assertFalse(endUserNanos == QueryConstants.NULL_LONG);
        final long endCpuNanos = SUT.getCurrentThreadCpuTime();
        TestCase.assertFalse(endCpuNanos == QueryConstants.NULL_LONG);
        final long elapsedCpuNanos = endCpuNanos - startCpuNanos;
        final long elapsedUserNanos = endUserNanos - startUserNanos;

        System.out.println("TestThreadProfiler: Spent " + elapsedCpuNanos + "ns ("
            + elapsedUserNanos + " ns user) calculating fib(92) == " + fib_curr);
        TestCase.assertEquals(7540113804746346429L, fib_curr);
        if (STRICT_MODE) {
            TestCase.assertTrue(elapsedUserNanos <= elapsedCpuNanos);
        } else {
            TestCase.assertTrue(elapsedUserNanos >= 0);
            TestCase.assertTrue(elapsedCpuNanos >= 0);
        }
    }
}
