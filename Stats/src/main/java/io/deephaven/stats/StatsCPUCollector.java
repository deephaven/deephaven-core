/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stats;

import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.OSUtil;
import io.deephaven.base.stats.*;
import io.deephaven.hash.KeyedLongObjectHash;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;

import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.*;
import java.nio.channels.FileChannel;

/**
 * Collects statistic related to CPU and memory usage of the entire system, the process, and each thread in the process.
 */
public class StatsCPUCollector {
    private static final Logger log = LoggerFactory.getLogger(StatsCPUCollector.class);

    public static final boolean MEASURE_PER_THREAD_CPU =
            Configuration.getInstance().getBoolean("measurement.per_thread_cpu");
    private static final String PROC_STAT_PSEUDOFILE = "/proc/stat";
    private static final String PROC_SELF_STAT_PSEUDOFILE = "/proc/self/stat";
    private static final String PROC_STAT_FD_PSUEDOFILE = "/proc/self/fd";

    private static final long NANOS = 1000000000;
    private static final long MILLIS = 1000;

    // the divisor needed to express the CPU usage in "percent of 1 core * 10"
    private final long divisor;

    // true, if we can open /proc/stat
    private boolean hasProcStat = true;

    // true, if we can open /proc/self/stat
    private boolean hasProcPidStat = true;

    // true, if we can list the contents of /proc/self/fd
    private boolean hasProcFd = true;

    // state for the machine as a whole
    private Counter statSysUserJiffies = null;
    private Counter statSysSystemJiffies = null;
    private Counter statSysIOWait = null;
    private Counter statSysPageIn = null;
    private Counter statSysPageOut = null;
    private Counter statSysSwapIn = null;
    private Counter statSysSwapOut = null;
    private Counter statSysInterrupts = null;
    private Counter statSysCtxt = null;

    // state for this process
    private Counter statProcMinorFaults = null;
    private Counter statProcMajorFaults = null;
    private Counter statProcUserJiffies = null;
    private Counter statProcSystemJiffies = null;
    private State statProcVSZ = null;
    private State statProcRSS = null;
    private State statProcNumFDs = null;
    private State statProcMaxFD = null;
    private byte[] statBuffer = null;

    // the interval between updates
    private final long interval;
    private final boolean getFdStats;

    StatsCPUCollector(long interval, boolean getFdStats) {
        this.interval = interval;
        this.getFdStats = getFdStats;
        long seconds = interval / MILLIS;
        this.divisor = NANOS / (seconds * 10);
        Stats.makeGroup("Kernel", "Unix kernel statistics, as read from " + PROC_STAT_PSEUDOFILE);
        Stats.makeGroup("Proc",
                "Unix process statistics, as read from " + PROC_SELF_STAT_PSEUDOFILE + " and " + PROC_STAT_FD_PSUEDOFILE);
        Stats.makeGroup("CPU", "JMX CPU usage data, per-thread and for the entire process");

        if (OSUtil.runningMacOS() || OSUtil.runningWindows()) {
            hasProcStat = false;
            hasProcPidStat = false;
            hasProcFd = false;
        }
    }

    private static class ThreadState {
        private final long id;
        private String name;
        private long lastCpuTime;
        private long lastUserTime;
        private State userTime;
        private State systemTime;

        public ThreadState(long id) {
            this.id = id;
        }

        public static KeyedLongObjectKey<ThreadState> keyDef = new KeyedLongObjectKey<ThreadState>() {
            public Long getKey(ThreadState v) {
                return v.id;
            }

            public long getLongKey(ThreadState v) {
                return v.id;
            }

            public int hashKey(Long k) {
                return (int) k.longValue();
            }

            public int hashLongKey(long k) {
                return (int) k;
            }

            public boolean equalKey(Long k, ThreadState v) {
                return k == v.id;
            }

            public boolean equalLongKey(long k, ThreadState v) {
                return k == v.id;
            }
        };

        public static KeyedLongObjectHash.ValueFactory<ThreadState> factory =
                new KeyedLongObjectHash.ValueFactory<>() {
                    public ThreadState newValue(long key) {
                        return new ThreadState(key);
                    }

                    public ThreadState newValue(Long key) {
                        return new ThreadState(key);
                    }
                };
    }

    /** the map containing all thread states */
    private static final KeyedLongObjectHashMap<ThreadState> threadStates =
            new KeyedLongObjectHashMap<>(100, ThreadState.keyDef);

    /** the user time for the process as a whole */
    private State processUserTime;

    /** the system time for the process as a whole */
    private State processSystemTime;

    private boolean startsWith(String match, int nb) {
        for (int i = 0; i < match.length(); i++) {
            final int nextIdx = i + statBufferIndex;
            if (nextIdx >= nb || statBuffer[nextIdx] != match.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private boolean skipWhiteSpace(int nb) {
        while (statBufferIndex < nb && statBuffer[statBufferIndex] == ' ') {
            if (statBuffer[statBufferIndex] == '\n') {
                return false;
            }
            statBufferIndex++;
        }
        return statBufferIndex < nb;
    }

    private boolean skipNextField(int nb) {
        while (statBufferIndex < nb && statBuffer[statBufferIndex] != ' ') {
            if (statBuffer[statBufferIndex] == '\n') {
                return false;
            }
            statBufferIndex++;
        }
        return skipWhiteSpace(nb);
    }

    private void getNextFieldSampleKilobytes(State v, int nb) {
        v.sample(getNextFieldLong(nb) / 1024);
        skipWhiteSpace(nb);
    }

    private boolean getNextFieldDeltaJiffies(Counter v, int nb) {
        v.incrementFromSample(getNextFieldLong(nb) * (10 * MILLIS) / interval);
        return skipWhiteSpace(nb);
    }

    private boolean getNextFieldDelta(Counter v, int nb) {
        v.incrementFromSample(getNextFieldLong(nb));
        return skipWhiteSpace(nb);
    }

    private void getNextFieldSample(State v, int nb) {
        v.sample(getNextFieldLong(nb));
        skipWhiteSpace(nb);
    }

    private long getNextFieldLong(int nb) {
        long result = 0;
        while (peekNextLong(nb)) {
            result *= 10;
            result += statBuffer[statBufferIndex] - '0';
            statBufferIndex++;
        }
        return result;
    }

    private boolean peekNextLong(int nb) {
        return statBufferIndex < nb && statBuffer[statBufferIndex] >= '0' && statBuffer[statBufferIndex] <= '9';
    }

    /**
     * Attempt to read the entire contents of an already-opened file to a pre-defined buffer. If the buffer is not large
     * enough for the entire contents of the file, then the caller should re-allocate the buffer, and try again
     *
     * @param inFile a file whose bytes we intend to read
     * @param buffer an array of bytes, which will be populated with the contents of the file
     *
     * @return the number of bytes successfully read into the buffer
     */
    private static int readToSizedBuffer(final FileChannel inFile, final byte[] buffer) throws IOException {
        int nb = 0;

        while (nb < buffer.length) {
            final int thisNb = inFile.read(buffer, nb, buffer.length - nb);

            if (thisNb == -1) {
                break;
            } else {
                nb += thisNb;
            }
        }

        return nb;
    }

    /**
     * Update the system-wide kernel statistics
     */
    private int statBufferIndex;
    private FileInputStream statFile;

    private void updateSys() {
        if (hasProcStat) {
            try {
                if (statFile == null) {
                    statFile = new FileInputStream(PROC_STAT_PSEUDOFILE);
                }

                int nb;
                while (true) {
                    FileChannel channel = statFile.getChannel();
                    channel.position(0);
                    nb = readToSizedBuffer(statFile, statBuffer);

                    if (nb == statBuffer.length) {
                        // allocate larger read-buffer, and try again
                        statBuffer = new byte[statBuffer.length * 2];

//                        statFile.close();
//                        statFile = new FileInputStream(PROC_STAT_PSEUDOFILE);
                        channel.position(0);
                    } else if (nb == 0) {
                        throw new RuntimeException(PROC_STAT_PSEUDOFILE + " zero read");
                    } else {
                        break;
                    }
                }

                statBufferIndex = 0;
                while (statBufferIndex < nb) {
                    while (statBufferIndex < nb && statBuffer[statBufferIndex] < 33) {
                        statBufferIndex++;
                    }
                    if (startsWith("cpu ", nb)) {
                        if (skipNextField(nb) && peekNextLong(nb)) {
                            if (statSysUserJiffies == null) {
                                statSysUserJiffies = Stats
                                        .makeItem("Kernel", "UserJiffies", Counter.FACTORY,
                                                "User jiffies per 10 second interval (1000 equals 1 full CPU)")
                                        .getValue();
                                statSysSystemJiffies = Stats
                                        .makeItem("Kernel", "SystemJiffies", Counter.FACTORY,
                                                "System jiffies per 10 second interval (1000 equals 1 full CPU)")
                                        .getValue();
                            }
                            if (getNextFieldDeltaJiffies(statSysUserJiffies, nb) && skipNextField(nb)
                                    && peekNextLong(nb)) {
                                if (getNextFieldDeltaJiffies(statSysSystemJiffies, nb) && skipNextField(nb)
                                        && skipNextField(nb) && peekNextLong(nb)) {
                                    if (statSysIOWait == null) {
                                        statSysIOWait = Stats.makeItem("Kernel", "IOWait", Counter.FACTORY,
                                                "IOWait jiffies per 10 second interval (1000 equals 1 full CPU)")
                                                .getValue();
                                    }
                                    getNextFieldDeltaJiffies(statSysIOWait, nb);
                                }
                            }
                        }
                    } else if (startsWith("page", nb)) {
                        if (skipNextField(nb) && peekNextLong(nb)) {
                            if (statSysPageIn == null) {
                                statSysPageIn = Stats.makeItem("Kernel", "PageIn", Counter.FACTORY,
                                        "Number of pages read in from disk").getValue();
                                statSysPageOut = Stats.makeItem("Kernel", "PageOut", Counter.FACTORY,
                                        "Number of pages written to disk").getValue();
                            }
                            if (getNextFieldDelta(statSysPageIn, nb) && peekNextLong(nb)) {
                                getNextFieldDelta(statSysPageOut, nb);
                            }
                        }
                    } else if (startsWith("swap", nb)) {
                        if (statSysSwapIn == null) {
                            statSysSwapIn = Stats.makeItem("Kernel", "SwapIn", Counter.FACTORY,
                                    "Number of pages read from swap space").getValue();
                            statSysSwapOut = Stats.makeItem("Kernel", "SwapOut", Counter.FACTORY,
                                    "Number of pages written to swap space").getValue();
                        }
                        if (skipNextField(nb) && getNextFieldDelta(statSysSwapIn, nb) && peekNextLong(nb)) {
                            getNextFieldDelta(statSysSwapOut, nb);
                        }
                    } else if (startsWith("intr", nb)) {
                        if (statSysInterrupts == null) {
                            statSysInterrupts =
                                    Stats.makeItem("Kernel", "Interrupts", Counter.FACTORY, "Number of interrupts")
                                            .getValue();
                        }
                        if (skipNextField(nb)) {
                            getNextFieldDelta(statSysInterrupts, nb);
                        }
                    } else if (startsWith("ctxt", nb)) {
                        if (statSysCtxt == null) {
                            statSysCtxt =
                                    Stats.makeItem("Kernel", "Ctxt", Counter.FACTORY, "Number of context switches")
                                            .getValue();
                        }
                        if (skipNextField(nb)) {
                            getNextFieldDelta(statSysCtxt, nb);
                        }
                    }
                    while (statBufferIndex < nb && statBuffer[statBufferIndex++] != '\n') {
                    }
                }
            } catch (Exception x) {
                if (statFile != null) {
                    try {
                        statFile.close();
                    } catch (final IOException ignore) {
                    }
                    statFile = null;
                }

                // if we get any exception, don't try to read it again
                if (hasProcStat) {
                    log.error("got an exception reading " + PROC_STAT_PSEUDOFILE + ": " + x);
                }
                hasProcStat = false;
            }
        }
    }

    /**
     * Update the per-process statistics
     */
    FileInputStream procFile;

    private void updateProc() {
        if (hasProcPidStat) {
            try {
                if (procFile == null) {
                    procFile = new FileInputStream(PROC_SELF_STAT_PSEUDOFILE);
                }
                if (statProcMinorFaults == null) {
                    statProcMinorFaults = Stats
                            .makeItem("Proc", "MinorFaults", Counter.FACTORY, "Minor faults the process has incurred")
                            .getValue();
                    statProcMajorFaults = Stats
                            .makeItem("Proc", "MajorFaults", Counter.FACTORY, "Major faults the process has incurred")
                            .getValue();
                    statProcUserJiffies = Stats.makeItem("Proc", "UserJiffies", Counter.FACTORY,
                            "User jiffies per 10 second interval (1000 equals 1 full CPU)").getValue();
                    statProcSystemJiffies = Stats.makeItem("Proc", "SystemJiffies", Counter.FACTORY,
                            "System jiffies per 10 second interval (1000 equals 1 full CPU)").getValue();
                    statProcVSZ =
                            Stats.makeItem("Proc", "VSZ", State.FACTORY, "Virtual size of the process in kilobytes")
                                    .getValue();
                    statProcRSS =
                            Stats.makeItem("Proc", "RSS", State.FACTORY, "Resident set size of the process in pages")
                                    .getValue();
                }
                statBufferIndex = 0;

                int nb;
                while (true) {
                    procFile.getChannel().position(0);
                    nb = readToSizedBuffer(procFile, statBuffer);

                    if (nb == statBuffer.length) {
                        // allocate larger read-buffer, and try again
                        statBuffer = new byte[statBuffer.length * 2];

                        procFile.close();
                        procFile = new FileInputStream(PROC_SELF_STAT_PSEUDOFILE);
                    } else if (nb == 0) {
                        throw new RuntimeException(PROC_SELF_STAT_PSEUDOFILE + " zero read");
                    } else {
                        break;
                    }
                }

                for (int i = 0; i < 9; i++) {
                    skipNextField(nb);
                }
                getNextFieldDelta(statProcMinorFaults, nb);
                skipNextField(nb);
                getNextFieldDelta(statProcMajorFaults, nb);
                skipNextField(nb);
                getNextFieldDeltaJiffies(statProcUserJiffies, nb);
                getNextFieldDeltaJiffies(statProcSystemJiffies, nb);
                for (int i = 15; i < 22; i++) {
                    skipNextField(nb);
                }
                getNextFieldSampleKilobytes(statProcVSZ, nb);
                getNextFieldSample(statProcRSS, nb);
            } catch (Exception x) {
                if (procFile != null) {
                    try {
                        procFile.close();
                    } catch (final IOException ignore) {
                    }
                    procFile = null;
                }

                // if we get any exception, don't try to read it again
                if (hasProcPidStat) {
                    log.error("got an exception reading " + PROC_SELF_STAT_PSEUDOFILE + ": " + x);
                }
                hasProcPidStat = false;
            }
        }
    }

    /**
     * Update the per-process file descriptor statistics
     */
    private void updateProcFD() {
        if (hasProcFd) {
            try {
                File procFd = new File(PROC_STAT_FD_PSUEDOFILE);
                String[] entries = procFd.list();
                if (entries == null) {
                    // if the directory is not readable, don't try to read it again
                    hasProcFd = false;
                    return;
                }
                if (statProcNumFDs == null) {
                    statProcNumFDs = Stats
                            .makeItem("Proc", "NumFDs", State.FACTORY, "Number of open file descriptors in the process")
                            .getValue();
                    statProcMaxFD = Stats.makeItem("Proc", "MaxFD", State.FACTORY,
                            "Highest-numbered file descriptors in the process").getValue();
                }
                int maxFd = -1;
                for (String s : entries) {
                    try {
                        int fd = Integer.parseInt(s);
                        maxFd = Math.max(fd, maxFd);
                    } catch (NumberFormatException x) {
                        // ignore
                    }
                }
                statProcNumFDs.sample(entries.length);
                statProcMaxFD.sample(maxFd);
            } catch (Exception x) {
                // if we get any exception, don't try to read it again
                hasProcFd = false;
            }
        }
    }

    /**
     * Update JMX per-thread cpu usage
     */
    private void updatePerThreadCPU() {
        if (MEASURE_PER_THREAD_CPU) {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            long[] threadIds = threadMXBean.getAllThreadIds();
            ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds);
            long deltaProcessCpuTime = 0;
            long deltaProcessUserTime = 0;
            for (ThreadInfo tinfo : threadInfos) {
                if (tinfo == null) {
                    continue;
                }
                ThreadState state = threadStates.putIfAbsent(tinfo.getThreadId(), ThreadState.factory);
                long cpuTime = threadMXBean.getThreadCpuTime(state.id);
                long userTime = threadMXBean.getThreadUserTime(state.id);
                if (state.name == null) {
                    // first time we've seen this thread, no sample
                    state.name = tinfo.getThreadName();
                    state.userTime = Stats.makeItem("CPU", state.name + "-userTime", State.FACTORY,
                            "Per-thread CPU usage in user mode, 1000 equals 1 full CPU, as reported by Java")
                            .getValue();
                    state.systemTime = Stats.makeItem("CPU", state.name + "-systemTime", State.FACTORY,
                            "Per-thread CPU usage in system mode, 1000 equals 1 full CPU, as reported by Java")
                            .getValue();
                } else {
                    long deltaCpuTime = cpuTime - state.lastCpuTime;
                    long deltaUserTime = userTime - state.lastUserTime;
                    deltaProcessCpuTime += deltaCpuTime;
                    deltaProcessUserTime += deltaUserTime;
                    if (deltaUserTime / divisor != 0) {
                        state.userTime.sample(deltaUserTime / divisor);
                    }
                    if ((deltaCpuTime - deltaUserTime) / divisor != 0) {
                        state.systemTime.sample((deltaCpuTime - deltaUserTime) / divisor);
                    }
                }
                state.lastCpuTime = cpuTime;
                state.lastUserTime = userTime;
            }
            if (processUserTime == null) {
                processUserTime = Stats.makeItem("CPU", "process-userTime", State.FACTORY,
                        "Process CPU usage in user mode, 1000 equals 1 full CPU, as reported by JMX").getValue();
                processSystemTime = Stats.makeItem("CPU", "process-systemTime", State.FACTORY,
                        "Process CPU usage in system mode, 1000 equals 1 full CPU, as reported by JMX").getValue();
            }
            processUserTime.sample(deltaProcessUserTime / divisor);
            processSystemTime.sample((deltaProcessCpuTime - deltaProcessUserTime) / divisor);
        }
    }

    /**
     * update all statistics in the driver's timer task
     */
    public void update() {
        if (statBuffer == null) {
            statBuffer = new byte[4096];
        }
        updateSys();
        updateProc();
        if (getFdStats) {
            updateProcFD();
        }
        updatePerThreadCPU();
    }
}
