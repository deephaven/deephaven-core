/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tool for centralizing error reports that should result in process termination.
 */
public abstract class FatalErrorReporterBase implements FatalErrorReporter {

    private final List<Interceptor> interceptors = new CopyOnWriteArrayList<>();

    private final class FatalException extends RuntimeException {
    }

    /**
     * Report a fatal error in an implementation specific way. Implementations should invoke appropriate shutdown tasks
     * and initiate process shutdown (e.g. via {@link System#exit(int)}).
     *
     * @param message the message
     * @param throwable the throwable
     * @param isFromUncaught true iff called from
     *        {@link java.lang.Thread.UncaughtExceptionHandler#uncaughtException(Thread, Throwable)}.
     */
    protected abstract void reportImpl(@NotNull String message, @NotNull Throwable throwable, boolean isFromUncaught);

    @Override
    public final void report(@NotNull final String message, @NotNull final Throwable throwable) {
        interceptors.forEach(interceptor -> interceptor.intercept(message, throwable));
        reportImpl(message, throwable, false);
    }

    @Override
    public final void report(@NotNull final String message) {
        report(message, new FatalException());
    }

    @Override
    public final void reportAsync(@NotNull final String message, @NotNull final Throwable throwable) {
        new Thread(() -> report(message, throwable), Thread.currentThread().getName() + "-AsyncFatalErrorSignaller")
                .start();
    }

    @Override
    public final void reportAsync(@NotNull final String message) {
        report(message, new FatalException());
    }

    @Override
    public final void uncaughtException(@NotNull final Thread thread, @NotNull final Throwable throwable) {
        final String message = "Uncaught exception in thread " + thread.getName();
        interceptors.forEach(interceptor -> interceptor.intercept(message, throwable));
        reportImpl(message, throwable, true);
    }

    @Override
    public void addInterceptor(@NotNull final Interceptor interceptor) {
        interceptors.add(Require.neqNull(interceptor, "interceptor"));
    }
}
