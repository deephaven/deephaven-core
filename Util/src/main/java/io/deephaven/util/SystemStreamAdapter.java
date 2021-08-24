package io.deephaven.util;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.LoggerOutputStream;

import java.io.PrintStream;

/**
 * Class to allow logs to receive sysout and syserr output.
 */
public class SystemStreamAdapter {

    static {
        // ensures that we capture System.out and System.err before we redirect
        PrintStreamGlobals.init();
    }

    /**
     * Redirect System.out to the specified logger, at level STDOUT.
     * 
     * @param logger the receiving logger
     */
    public static void sendSystemOutToLogger(final Logger logger) {
        System.setOut(new PrintStream(new LoggerOutputStream(logger, LogLevel.STDOUT), true));
    }

    /**
     * Redirect System.err to the specified logger, at level STDERR.
     * 
     * @param logger the receiving logger
     */
    public static void sendSystemErrToLogger(final Logger logger) {
        System.setErr(new PrintStream(new LoggerOutputStream(logger, LogLevel.STDERR), true));
    }
}
