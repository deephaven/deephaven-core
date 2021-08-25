package io.deephaven.db.v2.utils;

public class PerfMeasure {
    private long t0;
    private long t1;
    private long m0; // used memory
    private long m1;
    private long a0; // allocated during, even if not still used.
    private long a1;
    TimeReader tr;
    UsedMemoryReader mr;
    AllocatedMemoryReader ar;

    interface TimeReader {
        long getNanos();
    }

    private static TimeReader systemNanoTimeReader = new TimeReader() {
        @Override
        public long getNanos() {
            return System.nanoTime();
        }
    };
    private static TimeReader threadMXBeanTimeReader = new TimeReader() {
        @Override
        public long getNanos() {
            return tmb.getThreadCpuTime(Thread.currentThread().getId());
        }
    };

    interface UsedMemoryReader {
        long getUsedBytesAfterGC();
    }

    private static UsedMemoryReader runtimeUsedMemoryReader = new UsedMemoryReader() {
        @Override
        public long getUsedBytesAfterGC() {
            final Runtime rt = Runtime.getRuntime();
            rt.gc();
            return rt.totalMemory() - rt.freeMemory();
        }
    };
    private static UsedMemoryReader nullUsedMemoryReader = new UsedMemoryReader() {
        @Override
        public long getUsedBytesAfterGC() {
            return 0;
        }
    };

    interface AllocatedMemoryReader {
        long getAllocatedBytes();
    }

    private static AllocatedMemoryReader threadMXBeanMemoryReader = new AllocatedMemoryReader() {
        @Override
        public long getAllocatedBytes() {
            final Runtime rt = Runtime.getRuntime();
            rt.gc();
            return amb.getThreadAllocatedBytes(Thread.currentThread().getId());
        }
    };
    private static AllocatedMemoryReader nullAllocatedMemoryReader = new AllocatedMemoryReader() {
        @Override
        public long getAllocatedBytes() {
            return 0;
        }
    };

    private static final java.lang.management.ThreadMXBean tmb;
    private static final com.sun.management.ThreadMXBean amb;
    private static final String conf;

    static {
        String s = "Used allocated memory readings using ";
        java.lang.management.ThreadMXBean t = java.lang.management.ManagementFactory.getThreadMXBean();
        if (t instanceof com.sun.management.ThreadMXBean) {
            amb = (com.sun.management.ThreadMXBean) t;
            amb.setThreadAllocatedMemoryEnabled(true);
            s += "ThreadMXBean.getThreadAllocatedBytes.";
        } else {
            amb = null;
            s += "Runtime.(total - free).";
        }
        s += "  Time readings using ";
        if (t != null && t.isCurrentThreadCpuTimeSupported()) {
            t.setThreadCpuTimeEnabled(true);
            tmb = t;
            s += "ThreadMXBean.getThreadCPUTime.";
        } else {
            tmb = null;
            s += "System.nanoTime.";
        }
        conf = s;
    }

    public static String conf() {
        return conf;
    }

    public PerfMeasure(final boolean doMem) {
        if (doMem) {
            ar = (amb != null) ? threadMXBeanMemoryReader : nullAllocatedMemoryReader;
            mr = runtimeUsedMemoryReader;
        } else {
            ar = nullAllocatedMemoryReader;
            mr = nullUsedMemoryReader;
        }
        tr = (tmb != null) ? threadMXBeanTimeReader : systemNanoTimeReader;
    }

    public void start() {
        m0 = mr.getUsedBytesAfterGC();
        a0 = ar.getAllocatedBytes();
        t0 = tr.getNanos();
    }

    public void mark() {
        t1 = tr.getNanos();
        a1 = ar.getAllocatedBytes();
        m1 = mr.getUsedBytesAfterGC();
    }

    public long dt() {
        return t1 - t0;
    }

    public long dm() {
        return m1 - m0;
    }

    public long ar() {
        return a1 - a0;
    }

    public void reset() {
        t0 = t1 = 0;
        m0 = m1 = 0;
        a0 = a1 = 0;
    }
}
