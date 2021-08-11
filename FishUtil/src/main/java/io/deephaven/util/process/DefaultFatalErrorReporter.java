/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.base.system.AsyncSystem;
import java.io.PrintStream;
import org.jetbrains.annotations.NotNull;

public class DefaultFatalErrorReporter extends FatalErrorReporterBase {

    private static final int EXIT_STATUS = 1;

    private final PrintStream err;

    public DefaultFatalErrorReporter() {
        err = PrintStreamGlobals.getErr();
    }

    @Override
    protected void reportImpl(@NotNull final String message, @NotNull final Throwable throwable,
            boolean isUncaughtException) {
        // Similar code has Thread.setDefaultUncaughtExceptionHandler(null); here "to prevent deadlocks."
        // I think our control over the actual System.exit() call is sufficient.
        final boolean initiateShutdown = !ProcessEnvironment.getGlobalShutdownManager().tasksInvoked();

        // It's a tricky proposition to try and write out to a io.deephaven.io.logger.Logger here.
        // Instead, we log to a PrintStream, ideally the original System.err.
        err.println(String.format("%s: %s",
                initiateShutdown ? "Initiating shutdown due to" : "After shutdown initiated", message));
        throwable.printStackTrace(err);
        if (err != System.err) {
            throwable.printStackTrace(System.err);
        }
        err.flush();

        if (initiateShutdown) {
            // We can't universally call System.exit(...) on this thread, unless we have very tight
            // control over the expected concurrency primitives we use wrt this thread, and shutdown
            // logic. It's easier to manage the call to System.exit(...) if it is off-thread.
            AsyncSystem.exit("DefaultFatalErrorReporter", EXIT_STATUS, err);
        }

        // If called from the uncaught exception handler, we should prefer to return and let the
        // thread die.
        if (isUncaughtException) {
            return;
        }

        neverReturn();
    }

    /**
     * The semantics of {@link System#exit(int)} indicate that "This method never returns normally". We would like to
     * preserve that property for our calls to {@link #report(String, Throwable)} even in the case where we aren't
     * initiating {@link AsyncSystem#exit(String, int, PrintStream)}.
     *
     * <p>
     * Note: the JVM will still exit once {@link System#exit(int)} finishes, even if the current thread is a non-daemon
     * thread.
     */
    private void neverReturn() {
        // noinspection InfiniteLoopStatement
        while (true) {
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }
}
