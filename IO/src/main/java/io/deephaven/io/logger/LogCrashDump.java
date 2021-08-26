/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.io.log.LogEntry;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class LogCrashDump {
    public static void logCrashDump(Logger log) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 32);

        LogEntry entry = null;

        for (int i = 0; i < threadInfos.length; ++i) {
            ThreadInfo ti = threadInfos[i];
            if (ti != null) {
                StackTraceElement stack[] = ti.getStackTrace();
                String lockName = ti.getLockName();
                String lockOwnerName = lockName == null ? null : ti.getLockOwnerName();
                long lockOwnerId = lockName == null ? -1 : ti.getLockOwnerId();
                if (lockName != null && lockOwnerName != null) {
                    (entry == null ? (entry = log.info()) : entry).append("ThreadInfo: ")
                        .append(ti.getThreadName()).append(", id=").append(ti.getThreadId())
                        .append(", state=").append(ti.getThreadState().name())
                        .append(" on ").append(lockName + " owned by ").append(lockOwnerName)
                        .append(" id=").append(lockOwnerId).nl();
                } else {
                    (entry == null ? (entry = log.info()) : entry).append("ThreadInfo: ")
                        .append(ti.getThreadName()).append(", id=").append(ti.getThreadId())
                        .append(", state=").append(ti.getThreadState().name()).nl();
                }
                if (stack == null || stack.length == 0) {
                    (entry == null ? (entry = log.info()) : entry)
                        .append("   <no stack trace available>").nl();
                } else {
                    for (StackTraceElement e : stack) {
                        (entry == null ? (entry = log.info()) : entry).append("   ")
                            .append(e.toString()).nl();
                    }
                }
            }
        }

        if (entry != null) {
            entry.end();
        }
    }
}
