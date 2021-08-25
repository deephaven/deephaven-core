/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class DefaultProcessEnvironment extends BaseProcessEnvironment {

    private DefaultProcessEnvironment(@NotNull final String mainClassName,
        @NotNull final Logger log) {
        super(new ShutdownManagerImpl(), new DefaultFatalErrorReporter(), mainClassName, log);
    }

    @Override
    public void onStartup() {
        log.info().append(mainClassName).append(": starting up").endl();
        shutdownManager.registerTask(ShutdownManager.OrderingCategory.LAST,
            new LoggerShutdownTask());
        shutdownManager.addShutdownHookToRuntime();
    }

    @Override
    public void onShutdown() {
        log.warn().append(mainClassName).append(": shutting down").endl();
        shutdownManager.maybeInvokeTasks();
    }

    static class Factory implements ProcessEnvironment.Factory {

        @Override
        public ProcessEnvironment make(@NotNull final Configuration configuration,
            @NotNull final String mainClassName, @NotNull final Logger log) {
            return new DefaultProcessEnvironment(mainClassName, log);
        }
    }
}
