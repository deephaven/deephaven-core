//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.testcase;

import io.deephaven.util.process.FatalErrorReporter;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of {@link io.deephaven.util.process.ProcessEnvironment} that's suitable for unit tests.
 */
public class FakeProcessEnvironment implements ProcessEnvironment {

    public static final ProcessEnvironment INSTANCE = new FakeProcessEnvironment();

    private FakeProcessEnvironment() {}

    @Override
    public ShutdownManager getShutdownManager() {
        return FakeShutdownManager.INSTANCE;
    }

    @Override
    public FatalErrorReporter getFatalErrorReporter() {
        return FakeFatalErrorReporter.INSTANCE;
    }

    @Override
    public String getMainClassName() {
        return "UnitTest";
    }

    @Override
    public void onStartup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onShutdown() {
        throw new UnsupportedOperationException();
    }

    private static final class FakeShutdownManager implements ShutdownManager {

        private static final ShutdownManager INSTANCE = new FakeShutdownManager();

        private FakeShutdownManager() {}

        @Override
        public void addShutdownHookToRuntime() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerTask(@NotNull OrderingCategory orderingCategory, @NotNull Task task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deregisterTask(@NotNull OrderingCategory orderingCategory, @NotNull Task task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {}

        @Override
        public boolean tasksInvoked() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean maybeInvokeTasks() {
            throw new UnsupportedOperationException();
        }
    }

    public static final class FakeFatalException extends RuntimeException {

        public FakeFatalException(@NotNull final String message, @NotNull final Throwable throwable) {
            super(message, throwable);
        }

        public FakeFatalException(@NotNull final String message) {
            super(message);
        }
    }

    private static final class FakeFatalErrorReporter implements FatalErrorReporter {

        private static final FatalErrorReporter INSTANCE = new FakeFatalErrorReporter();

        private FakeFatalErrorReporter() {}

        @Override
        public void report(@NotNull final String message, @NotNull final Throwable throwable) {
            throw new FakeFatalException(message, throwable);
        }

        @Override
        public void report(@NotNull final String message) {
            throw new FakeFatalException(message);
        }

        @Override
        public void reportAsync(@NotNull final String message, @NotNull final Throwable throwable) {
            report(message, throwable);
        }

        @Override
        public void reportAsync(@NotNull final String message) {
            report(message);
        }

        @Override
        public void addInterceptor(@NotNull final Interceptor interceptor) {}

        @Override
        public void uncaughtException(@NotNull final Thread t, @NotNull final Throwable e) {
            report("Uncaught exception in thread " + t.getName(), e);
        }
    }
}
