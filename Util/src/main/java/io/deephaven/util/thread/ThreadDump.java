/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.thread;

import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.io.logger.Logger;

import java.io.PrintStream;
import java.lang.management.*;
import java.util.Objects;

/**
 * A simple method for generating a Thread dump for this JVM; it doesn't do all the stuff that the
 * kill -3 does; but you can easily run it from inside the JVM without having to send yourself a
 * signal.
 */
public class ThreadDump {
    /**
     * Write a thread dump to the given PrintStream.
     *
     * @param out the output stream to write the thread dump of the current process to
     */
    public static void threadDump(final PrintStream out) {
        out.print(threadDump());
    }

    /**
     * Write a thread dump to the provided logger.
     *
     * @param logger the logger to write the thread dump of the current process to
     */
    public static void threadDump(final Logger logger) {
        final ThreadInfo[] threadInfos = getThreadInfos();
        for (final ThreadInfo threadInfo : threadInfos) {
            final LogEntry info = logger.info();
            info.append(THREAD_INFO_OBJ_FORMATTER, threadInfo);
            info.end();
        }
    }

    /**
     * Generate a thread dump as a String.
     *
     * @return a String containing a thread dump of the curent process.
     */
    public static String threadDump() {
        final LogOutputStringImpl lo = new LogOutputStringImpl();
        final ThreadInfo[] threadInfos = getThreadInfos();
        for (final ThreadInfo threadInfo : threadInfos) {
            lo.append(THREAD_INFO_OBJ_FORMATTER, threadInfo);
        }
        return lo.toString();
    }

    /**
     * Get the ThreadInfo array for the thread dump.
     *
     * @return an array of ThreadInfo elements for each thread
     */
    private static ThreadInfo[] getThreadInfos() {
        final ThreadMXBean threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);
        return threadMXBean.dumpAllThreads(true, true);
    }

    private static final LogOutput.ObjFormatter<ThreadInfo> THREAD_INFO_OBJ_FORMATTER =
        (logOutput, threadInfo) -> {
            if (threadInfo == null) {
                logOutput.append("null");
                return;
            }
            logOutput.append("\"").append(threadInfo.getThreadName()).append("\" Id=")
                .append(threadInfo.getThreadId()).append(" ")
                .append(Objects.toString(threadInfo.getThreadState()));

            if (threadInfo.getLockName() != null) {
                logOutput.append(" on ").append(threadInfo.getLockName());
            }
            if (threadInfo.getLockOwnerName() != null) {
                logOutput.append(" owned by \"").append(threadInfo.getLockOwnerName())
                    .append("\" Id=").append(threadInfo.getLockOwnerId());
            }
            if (threadInfo.isSuspended()) {
                logOutput.append(" (suspended)");
            }
            if (threadInfo.isInNative()) {
                logOutput.append(" (in native)");
            }
            logOutput.append('\n');
            int i = 0;
            final StackTraceElement[] stackTrace = threadInfo.getStackTrace();
            for (; i < stackTrace.length; i++) {
                final StackTraceElement ste = stackTrace[i];
                logOutput.append("\tat ").append(ste.toString());
                logOutput.append('\n');
                if (i == 0 && threadInfo.getLockInfo() != null) {
                    final Thread.State ts = threadInfo.getThreadState();
                    switch (ts) {
                        case BLOCKED:
                            logOutput.append("\t-  blocked on ")
                                .append(Objects.toString(threadInfo.getLockInfo()));
                            logOutput.append('\n');
                            break;
                        case WAITING:
                        case TIMED_WAITING:
                            logOutput.append("\t-  waiting on ")
                                .append(Objects.toString(threadInfo.getLockInfo()));
                            logOutput.append('\n');
                            break;
                        default:
                    }
                }

                for (final MonitorInfo mi : threadInfo.getLockedMonitors()) {
                    if (mi.getLockedStackDepth() == i) {
                        logOutput.append("\t-  locked ").append(Objects.toString(mi));
                        logOutput.append('\n');
                    }
                }
            }

            final LockInfo[] locks = threadInfo.getLockedSynchronizers();
            if (locks.length > 0) {
                logOutput.append("\n\tNumber of locked synchronizers = ").append(locks.length);
                logOutput.append('\n');
                for (final LockInfo li : locks) {
                    logOutput.append("\t- ").append(Objects.toString(li));
                    logOutput.append('\n');
                }
            }
            logOutput.append('\n');
        };
}
