/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.base.verify.Require;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("WeakerAccess")
public abstract class BaseProcessEnvironment implements ProcessEnvironment {

    /**
     * The shared manager for tasks to be executed on shutdown.
     */
    protected final ShutdownManager shutdownManager;

    /**
     * The shared fatal error reporter.
     */
    protected final FatalErrorReporter fatalErrorReporter;

    /**
     * The class name for "main".
     */
    protected final String mainClassName;

    /**
     * The process trace log.
     */
    protected final Logger log;

    /**
     * Constructor, intended only for sub-classes.
     *
     * @param shutdownManager
     * @param fatalErrorReporter
     * @param mainClassName
     * @param log
     */
    protected BaseProcessEnvironment(@NotNull final ShutdownManager shutdownManager,
            @NotNull final FatalErrorReporter fatalErrorReporter,
            @NotNull final String mainClassName,
            @NotNull final Logger log) {
        this.shutdownManager = Require.neqNull(shutdownManager, "shutdownManager");
        this.fatalErrorReporter = Require.neqNull(fatalErrorReporter, "fatalErrorReporter");
        this.mainClassName = Require.nonempty(mainClassName, "mainClassName");
        this.log = Require.neqNull(log, "log");
    }

    @Override
    public final ShutdownManager getShutdownManager() {
        return shutdownManager;
    }

    @Override
    public final FatalErrorReporter getFatalErrorReporter() {
        return fatalErrorReporter;
    }

    @Override
    public final String getMainClassName() {
        return mainClassName;
    }
}
