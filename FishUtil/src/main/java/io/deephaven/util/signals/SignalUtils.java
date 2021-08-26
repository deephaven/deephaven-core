/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.signals;

import io.deephaven.util.OSUtil;

import java.io.IOException;

public class SignalUtils {

    /**
     * What operating system does the JVM think we're on?
     */
    private static final OSUtil.OSFamily OPERATING_SYSTEM = OSUtil.getOSFamily();

    /**
     * Placeholder value when we don't know a signal's number on the current OS.
     */
    private static final int UNDEFINED_SIGNAL_NUMBER = Integer.MIN_VALUE;

    /**
     * Supported signals. Be careful when adding new entries - as you can see, signal numbers don't always line up
     * across operating systems.
     */
    public enum Signal {
        SIGINT("int", 2, 2, 2), SIGTERM("term", 15, 15, 15), SIGQUIT("quit", 3, 3, 3), SIGKILL("kill", 9, 9,
                9), SIGSTOP("stop", 19, 23, 17), SIGCONT("cont", 18, 25, 19);

        private final String signalName;
        private final int signalNumber;

        Signal(final String signalName, final int linuxSignalNumber, final int solarisSignalNumber,
                final int macOsSignalNumber) {
            this.signalName = signalName;
            switch (OPERATING_SYSTEM) {
                case LINUX:
                    signalNumber = linuxSignalNumber;
                    break;
                case MAC_OS:
                    signalNumber = macOsSignalNumber;
                    break;
                case SOLARIS:
                    signalNumber = solarisSignalNumber;
                    break;
                case WINDOWS:
                default:
                    signalNumber = UNDEFINED_SIGNAL_NUMBER;
                    break;
            }
        }

        public String getSignalName() {
            return signalName;
        }

        public int getSignalNumber() {
            if (signalNumber == UNDEFINED_SIGNAL_NUMBER) {
                throw new UnsupportedOperationException(this + " is undefined on " + OPERATING_SYSTEM);
            }
            return signalNumber;
        }
    }

    /**
     * Use /bin/kill to send a signal by name.
     *
     * @param processId The process ID to send the signal to
     * @param signalName The name of the signal to send
     * @return The exit value of the child process.
     */
    @SuppressWarnings("WeakerAccess")
    public static int sendSignalWithBinKill(final int processId, final String signalName) throws IOException {
        final ProcessBuilder pb = new ProcessBuilder("/bin/kill", "-s", signalName, Integer.toString(processId));
        final Process p = pb.start();

        try {
            p.getErrorStream().close();
            p.getInputStream().close();
            p.getOutputStream().close();
        } catch (IOException e) {
            throw new AssertionError("sendSignalWithBinKill: unexpected exception while closing child process streams: "
                    + e.getMessage(), e);
        }

        while (true) {
            try {
                return p.waitFor();
            } catch (InterruptedException ignored) {
            }
        }
    }

    /**
     * Ensure that libraries have been loaded, before using sendSignalNative(...).
     */
    @SuppressWarnings("WeakerAccess")
    public static void loadNative() {
        System.loadLibrary("FishCommon");
    }

    /**
     * Use native code to send a signal by number.
     *
     * @param processId The process ID to send the signal to
     * @param signalNumber The signal number to send
     * @return The return value of kill(2).
     */
    public static native int sendSignalNative(final int processId, final int signalNumber);
}
