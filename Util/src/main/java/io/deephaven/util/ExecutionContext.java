/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;

import java.util.function.Supplier;

public abstract class ExecutionContext {
    public static final ExecutionContext[] ZERO_LENGTH_EXECUTION_CONTEXT_ARRAY = new ExecutionContext[0];

    // -----------------------------------------------------------------------------------------------------------------
    // ThreadLocal Management
    // -----------------------------------------------------------------------------------------------------------------

    private static final ThreadLocal<ExecutionContext> currentContext = ThreadLocal.withInitial(() -> null);

    /**
     * @return an object representing the current execution context
     */
    public static ExecutionContext getThreadLocal() {
        return currentContext.get();
    }

    /**
     * Installs the executionContext to be used for the current thread.
     */
    public static void setContext(final ExecutionContext context) {
        currentContext.set(context);
    }

    /**
     * Installs the executionContext and returns a function that will restore the original executionContext.
     *
     * @return a closeable to cleanup the execution context
     */
    public abstract SafeCloseable open();

    /**
     * Execute runnable within this execution context.
     */
    public abstract void apply(final Runnable runnable);

    /**
     * Executes supplier within this execution context.
     */
    public abstract <T> T apply(final Supplier<T> supplier);

    /**
     * This exception is thrown when the thread-local QueryScope, QueryLibrary, or CompilerTools.Context are accessed
     * from user-code without an explicit ExecutionContext.
     */
    public static final class NotRegistered extends UncheckedDeephavenException {
        public NotRegistered() {
            super("ExecutionContext not registered");
        }
    }
}
