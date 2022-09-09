/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.system;

import io.deephaven.base.verify.Assert;
import java.io.PrintStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * A set of conventions for logging exceptions and asynchronously exiting the JVM.
 */
public class AsyncSystem {

    private static class AsyncSystemExitUncaughtExceptionHandler implements
            UncaughtExceptionHandler {

        private final PrintStream out;
        private final int status;

        AsyncSystemExitUncaughtExceptionHandler(PrintStream out, int status) {
            Assert.neqNull(out, "out");
            this.out = out;
            this.status = status;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            exitUncaught(t, e, status, out);
        }
    }

    /**
     * Creates an {@link UncaughtExceptionHandler} which calls out to
     * {@link #exitUncaught(Thread, Throwable, int, PrintStream)}.
     *
     * @param status the exit status
     * @return the uncaught exception handler
     */
    public static UncaughtExceptionHandler uncaughtExceptionHandler(int status, PrintStream out) {
        return new AsyncSystemExitUncaughtExceptionHandler(out, status);
    }

    /**
     * Starts an asynchronous call to {@link System#exit(int)}. A new daemon thread will be started and it will invoke
     * only {@code System.exit(status)}. In the event that {@link System#exit(int)} throws an exception, the name of the
     * thread and the stacktrace will be printed out.
     *
     * <p>
     * Note: this call will return, unlike a direct call to {@link System#exit(int)}. Callers should manage this as
     * appropriate.
     *
     * @param name the name to attach to the thread
     * @param status exit status
     * @param out the output print stream (on exception)
     */
    public static void exit(String name, int status, PrintStream out) {
        createThread(name, status, out)
                .start();
    }

    /**
     * Prints out a message and stacktrace, and then calls {@link #exit(String, int, PrintStream)}. This should
     * <b>only</b> be called from {@link UncaughtExceptionHandler uncaught exception handlers}.
     *
     * @param thread the thread
     * @param throwable the throwable
     * @param status the status
     * @param out the print stream
     */
    public static void exitUncaught(Thread thread, Throwable throwable, int status, PrintStream out) {
        try {
            out.println(String.format(
                    "Uncaught exception in thread %s. Shutting down with asynchronous system exit.",
                    thread));
            throwable.printStackTrace(out);
        } finally {
            exit(thread.getName(), status, out);
        }
    }

    /**
     * Equivalent to {@code exitCaught(thread, throwable, status, out, null).
     */
    public static void exitCaught(Thread thread, Throwable throwable, int status, PrintStream out) {
        exitCaught(thread, throwable, status, out, null);
    }

    /**
     * Prints out a message and stacktrace, and then calls {@link #exit(String, int, PrintStream)}. This can be called
     * from a thread which catches its own exceptions and wants to exit.
     *
     * @param thread the thread
     * @param throwable the throwable
     * @param status the status
     * @param out the print stream
     * @param message the optional additional message
     */
    public static void exitCaught(Thread thread, Throwable throwable, int status, PrintStream out,
            @Nullable String message) {
        try {
            if (message == null) {
                out.println(String.format(
                        "Caught exception in thread %s. Shutting down with asynchronous system exit.",
                        thread));
            } else {
                out.println(String.format(
                        "Caught exception in thread %s: %s. Shutting down with asynchronous system exit.",
                        thread, message));
            }
            throwable.printStackTrace(out);
        } finally {
            exit(thread.getName(), status, out);
        }
    }

    private static Thread createThread(String name, int status, PrintStream out) {
        return new AsyncSystemExitThread(
                String.format("AsyncSystemExit[%d,%s]", status, name),
                status,
                out);
    }

    private static class AsyncSystemExitThread extends Thread {
        private final int status;

        AsyncSystemExitThread(String name, int status, PrintStream out) {
            super(name);
            this.setDaemon(true);
            this.setUncaughtExceptionHandler(new Handler(out));
            this.status = status;
        }

        @Override
        public void run() {
            System.exit(status);
        }
    }

    private static class Handler implements UncaughtExceptionHandler {
        private final PrintStream out;

        Handler(PrintStream out) {
            this.out = Objects.requireNonNull(out);
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            // We are not in good shape if we get an exception in our System.exit call...
            // System.exit *swallows* exceptions from hooks and we should have already checked for
            // SecurityException before creation... and so this must be really bad.
            out.println(String.format("Uncaught exception in thread %s", t));
            e.printStackTrace(out);
        }
    }
}
