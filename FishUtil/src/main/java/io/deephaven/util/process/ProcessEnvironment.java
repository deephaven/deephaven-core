/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.PropertyException;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for installation-specific environment. Processes that use instances of this interface are responsible for
 * determining when to call the various methods.
 *
 * All implementations must setup a ShutdownManager and an implementation of FatalErrorReporter.
 */
@SuppressWarnings("unused")
public interface ProcessEnvironment {

    /**
     * Access a shared manager for tasks to be executed on shutdown.
     *
     * @return The ShutdownManager
     */
    ShutdownManager getShutdownManager();

    /**
     * Access a shared fatal error reporter.
     *
     * @return The FatalErrorReporter
     */
    FatalErrorReporter getFatalErrorReporter();

    /**
     * Access the main class name.
     *
     * @return The main class name
     */
    String getMainClassName();

    /**
     * Hook for setting up an installation-specific environment for a given process.
     */
    void onStartup();

    /**
     * Hook for shutting down an installation-specific environment for a given process. This is intended for controlled
     * invocation, rather than as part of a shutdown hook - {@code onStartup()} should setup any mandatory shutdown
     * hooks, and code in said shutdown hooks should be idempotent w.r.t. onShutdown().
     */
    void onShutdown();

    /**
     * Factory interface for constructing instances of ProcessEnvironment implementations.
     */
    interface Factory {

        /**
         * Factory method.
         *
         * @param configuration The configuration
         * @param mainClassName The main class name
         * @param log The log
         * @return A new instance of the appropriate ProcessEnvironment implementation
         */
        ProcessEnvironment make(@NotNull Configuration configuration, @NotNull String mainClassName,
                @NotNull Logger log);
    }

    /**
     * State and private methods for maintaining a shared global instance of ProcessEnvironment.
     */
    class GlobalHelper {

        /**
         * The global instance.
         */
        private static volatile ProcessEnvironment instance;

        private static void reportInstantiationError(@NotNull final String message, @NotNull final Exception exception,
                final int exitStatus) {
            System.err.println(message + ": " + exception);
            exception.printStackTrace(System.err);
            System.exit(exitStatus);
        }
    }

    /**
     * Accessor the for the global instance. Fails if the instance is null, in order to make programming errors clear.
     *
     * @return The global instance
     */
    static ProcessEnvironment get() {
        return Require.neqNull(tryGet(), "GlobalHelper.instance");
    }

    /**
     * Accessor the for the global instance.
     *
     * @return The global instance, or null if it is not set.
     */
    static ProcessEnvironment tryGet() {
        return GlobalHelper.instance;
    }

    static ShutdownManager getGlobalShutdownManager() {
        return get().getShutdownManager();
    }

    static FatalErrorReporter getGlobalFatalErrorReporter() {
        return get().getFatalErrorReporter();
    }

    static String getGlobalMainClassName() {
        return get().getMainClassName();
    }

    /**
     * Setter for the global instance. It is an error to invoke this without allowReplace if the global instance may
     * already have been set.
     *
     * @param instance The new global instance
     * @param allowReplace Whether to allow replacing an existing global instance
     * @return The global instance
     */
    static ProcessEnvironment set(@NotNull final ProcessEnvironment instance, final boolean allowReplace) {
        synchronized (GlobalHelper.class) {
            if (!allowReplace && GlobalHelper.instance != null) {
                throw new IllegalStateException("Can not replace ProcessEnvironment " + GlobalHelper.instance +
                        " with " + instance + " unless allowReplace=true");
            }
            return GlobalHelper.instance = Require.neqNull(instance, "instance");
        }
    }

    /**
     * Set the global instance to null, and reset its shutdown manager. Intended for use in unit tests, only.
     */
    static void clear() {
        synchronized (GlobalHelper.class) {
            GlobalHelper.instance.getShutdownManager().reset();
            GlobalHelper.instance = null;
        }
    }

    /**
     * Instantiate (and set as the global instance) a ProcessEnvironment specified according to the following factory
     * class name properties, in descending order of precedence: {@code mainClassName}.processEnvironmentFactory
     * default.processEnvironmentFactory
     *
     * Checked exceptions thrown in this process always result in process termination.
     *
     * @param configuration The configuration
     * @param mainClassName The main class name
     * @param log The log
     * @return The new ProcessEnvironment
     */
    @SuppressWarnings("ConstantConditions")
    static @NotNull ProcessEnvironment instantiateFromConfiguration(@NotNull final Configuration configuration,
            @NotNull final String mainClassName,
            @NotNull final Logger log) {
        final String factoryPropertyNameSuffix = ".processEnvironmentFactory";
        final String factoryClassName;
        try {
            factoryClassName = configuration.getStringWithDefault(mainClassName + factoryPropertyNameSuffix,
                    configuration.getProperty("default" + factoryPropertyNameSuffix));
        } catch (PropertyException e) {
            GlobalHelper.reportInstantiationError("Failed to get process environment factory name for " + mainClassName,
                    e, -11);
            return null;
        }
        final Factory factory;
        try {
            factory = (Factory) Class.forName(factoryClassName).newInstance();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | ClassCastException e) {
            GlobalHelper.reportInstantiationError(
                    "Failed to instantiate process environment factory " + factoryClassName, e, -12);
            return null;
        }
        try {
            return factory.make(configuration, mainClassName, log);
        } catch (Exception e) {
            GlobalHelper.reportInstantiationError("Unable to instantiate process environment for " + mainClassName, e,
                    -13);
            return null;
        }
    }

    /**
     * Do the basic installation most interactive (usually client) processes need: (1) instantiateFromConfiguration (2)
     * set (3) Invoke onStartup()
     *
     * @param configuration The configuration
     * @param mainClassName The main class name
     * @param log The log
     * @return The ProcessEnvironment
     */
    static @NotNull ProcessEnvironment basicInteractiveProcessInitialization(@NotNull final Configuration configuration,
            @NotNull final String mainClassName,
            @NotNull final Logger log) {
        final ProcessEnvironment processEnvironment = instantiateFromConfiguration(configuration, mainClassName, log);
        set(processEnvironment, false);
        processEnvironment.onStartup();
        return processEnvironment;
    }

    /**
     * Do the basic installation most server processes need: (1) basicInteractiveProcessInitialization (as a subset of
     * what servers need) (2) Set current thread name as {@code mainClassName}.main (3) Install the fatal error reporter
     * as default uncaught exception handler
     *
     * @param configuration The configuration
     * @param mainClassName The main class name
     * @param log The log
     * @return The ProcessEnvironment
     */
    static @NotNull ProcessEnvironment basicServerInitialization(@NotNull final Configuration configuration,
            @NotNull final String mainClassName,
            @NotNull final Logger log) {
        final ProcessEnvironment processEnvironment =
                basicInteractiveProcessInitialization(configuration, mainClassName, log);
        Thread.currentThread().setName(mainClassName + ".main");
        Thread.setDefaultUncaughtExceptionHandler(processEnvironment.getFatalErrorReporter());
        return processEnvironment;
    }
}
