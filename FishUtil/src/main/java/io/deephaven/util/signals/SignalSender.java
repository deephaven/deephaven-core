/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.signals;

import io.deephaven.base.verify.Require;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class SignalSender {

    private final Logger log;
    private final boolean useNative;

    public SignalSender(@NotNull final Logger log, final boolean useNative) {
        this.log = log;
        this.useNative = useNative;
        if (useNative) {
            SignalUtils.loadNative();
        }
    }

    /**
     * Helper method - sends SIQQUIT to a process. If this process is a JVM, it will send a stack dump to stdout.
     *
     * @param processId The process ID to send the signal to
     * @return true on success, false on error
     */
    public boolean sendQuit(final int processId) {
        return sendSignal(processId, SignalUtils.Signal.SIGQUIT);
    }

    /**
     * Helper method - sends SIQKILL to a process.
     *
     * @param processId The process ID to send the signal to
     * @return true on success, false on error
     */
    public boolean kill(final int processId) {
        return sendSignal(processId, SignalUtils.Signal.SIGKILL);
    }

    /**
     * Helper method - sends SIGCONT to a process.
     *
     * @param processId The process ID to send the signal to
     * @return true on success, false on error
     */
    public boolean resume(final int processId) {
        return sendSignal(processId, SignalUtils.Signal.SIGCONT);
    }

    /**
     * Helper method - sends SIGSTOP to a process.
     *
     * @param processId The process ID to send the signal to
     * @return true on success, false on error
     */
    public boolean suspend(final int processId) {
        return sendSignal(processId, SignalUtils.Signal.SIGSTOP);
    }

    /**
     * Send the specified signal to the target process.
     *
     * @param processId The process ID to send the signal to
     * @param signal The signal to send
     * @return true on success, false on error
     */
    private boolean sendSignal(final int processId, final SignalUtils.Signal signal) {
        Require.gtZero(processId, "processId"); // Don't want to allow fancier usages for now. See 'man -s 2 kill'.
        Require.neqNull(signal, "signal");

        final int rc;
        if (useNative) {
            rc = SignalUtils.sendSignalNative(processId, signal.getSignalNumber());
        } else {
            try {
                rc = SignalUtils.sendSignalWithBinKill(processId, signal.getSignalName());
            } catch (IOException e) {
                log.error().append("sendSignal: Exception while using /bin/kill to send ").append(signal.toString())
                        .append(" to processId ").append(processId).append(": ").append(e).endl();
                return false;
            }
        }

        if (rc == 0) {
            return true;
        }
        log.error().append("sendSignal: Error while using ").append(useNative ? "native code" : "/bin/kill")
                .append(" to send ").append(signal.toString())
                .append(" to processId ").append(processId)
                .append(": kill returned ").append(rc).endl();
        return false;
    }

    /**
     * Simple program for functionality testing.
     * 
     * @param args [ <pid> <signal> <use native?> ]
     */
    public static void main(final String... args) {
        final int pid = Integer.parseInt(args[0]);
        final SignalUtils.Signal signal = SignalUtils.Signal.valueOf(args[1]);
        final boolean useNative = Boolean.valueOf(args[2]);
        new SignalSender(new StreamLoggerImpl(), useNative).sendSignal(pid, signal);
    }
}
