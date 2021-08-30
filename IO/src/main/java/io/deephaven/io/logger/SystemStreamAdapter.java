/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.io.log.LogLevel;

import java.io.PrintStream;

public class SystemStreamAdapter {

    /**
     * Redirect System.out to the specified logger, at level STDOUT.
     * 
     * @param logger
     */
    public static void sendSystemOutToLogger(final Logger logger) {
        System.setOut(new PrintStream(new LoggerOutputStream(logger, LogLevel.STDOUT), true));
    }

    /**
     * Redirect System.err to the specified logger, at level STDERR.
     * 
     * @param logger
     */
    public static void sendSystemErrToLogger(final Logger logger) {
        System.setErr(new PrintStream(new LoggerOutputStream(logger, LogLevel.STDERR), true));
    }
}
