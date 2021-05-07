/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public interface FatalErrorReporter extends Thread.UncaughtExceptionHandler {

    void report(@NotNull String message, @NotNull Throwable throwable);

    void report(@NotNull String message);

    void reportAsync(@NotNull String message, @NotNull Throwable throwable);

    void reportAsync(@NotNull String message);

    void addInterceptor(@NotNull Interceptor interceptor);

    @FunctionalInterface
    interface Interceptor {
        void intercept(@NotNull String message, @NotNull Throwable throwable);
    }
}
