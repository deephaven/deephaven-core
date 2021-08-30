/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.threads;

import io.deephaven.base.Procedure;
import io.deephaven.io.logger.Logger;

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * A simple method for generating a Thread dump for this JVM; it doesn't do all the stuff that the
 * kill -3 does; but you can easily run it from inside the JVM without having to send yourself a
 * signal.
 */
public class ThreadDump {
    @SuppressWarnings("WeakerAccess")
    public static void threadDump(final PrintStream out) {
        doDump(out::print);
    }

    public static void threadDump(final Logger logger) {
        doDump(arg -> logger.info().append(arg).endl());
    }

    @SuppressWarnings("WeakerAccess")
    public static String threadDump() {
        final StringBuilder builder = new StringBuilder();
        doDump(builder::append);
        return builder.toString();
    }

    private static void doDump(Procedure.Unary<String> output) {
        ThreadMXBean threadMXBean = ManagementFactory.getPlatformMXBean(ThreadMXBean.class);

        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);

        for (ThreadInfo threadInfo : threadInfos) {
            output.call(threadInfo.toString());
        }
    }

    public static void main(String[] args) {
        threadDump(System.out);
    }
}
