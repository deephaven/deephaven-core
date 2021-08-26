/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.net;

import io.deephaven.base.FatalErrorHandler;
import io.deephaven.base.FatalErrorHandlerFactory;
import io.deephaven.configuration.Configuration;
import io.deephaven.net.impl.nio.NIODriver;
import io.deephaven.io.NioUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.sched.*;

import java.io.IOException;
import java.nio.channels.Selector;

public class CommBase {

    private static volatile FatalErrorHandler defaultFatalErrorHandler;

    public static FatalErrorHandler getDefaultFatalHandler() {
        if (defaultFatalErrorHandler == null) {
            synchronized (CommBase.class) {
                if (defaultFatalErrorHandler == null) {
                    final String defaultFatalErrorHandlerClassName =
                            Configuration.getInstance().getProperty("Comm.fatalErrorHandlerFactoryClass");
                    final Class defaultFatalErrorHandlerClass;
                    try {
                        defaultFatalErrorHandlerClass = Class.forName(defaultFatalErrorHandlerClassName);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException(
                                "Could not find envelopeHandlerFactoryClass " + defaultFatalErrorHandlerClassName, e);
                    }
                    final FatalErrorHandlerFactory defaultFatalErrorHandlerFactory;
                    try {
                        defaultFatalErrorHandlerFactory =
                                (FatalErrorHandlerFactory) defaultFatalErrorHandlerClass.newInstance();
                    } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
                        throw new IllegalArgumentException(
                                "Could not instantiate envelopeHandlerFactoryClass " + defaultFatalErrorHandlerClass,
                                e);
                    }
                    defaultFatalErrorHandler = defaultFatalErrorHandlerFactory.get();
                }
            }
        }
        return defaultFatalErrorHandler;
    }

    public static void signalFatalError(final String message, Throwable x) {
        try {
            FatalErrorHandler feh = getDefaultFatalHandler();
            feh.signalFatalError(message, x);
        } catch (Throwable fehx) {
            // dump this to stderr, it's not great, but we had an error raising an error and really do want both of
            // these in the log
            fehx.printStackTrace(System.err);
            x.printStackTrace(System.err);
            throw new RuntimeException("Could not raise fatal error: " + message, x);
        }
    }

    /**
     * Return the scheduler used by the NIO implementation
     */
    public static Scheduler getScheduler() {
        NIODriver.init();
        return NIODriver.getScheduler();
    }

    /**
     * Create a private, single-threaded scheduler and driver thread
     */
    public static class SingleThreadedScheduler extends YASchedulerImpl {
        private final Thread driver;
        private volatile boolean done = false;

        public SingleThreadedScheduler(final String name, Logger log) throws IOException {
            super(name, NioUtil.reduceSelectorGarbage(Selector.open()), log);
            this.driver = new Thread(() -> {
                try {
                    while (!SingleThreadedScheduler.this.done) {
                        work(10, null);
                    }
                } catch (Throwable x) {
                    signalFatalError(name + " exception", x);
                }
            });
            driver.setName(name + "-Driver");
            driver.setDaemon(true);
        }

        public SingleThreadedScheduler start() {
            driver.start();
            return this;
        }

        public void stop() {
            done = true;
        }
    }

    public static SingleThreadedScheduler singleThreadedScheduler(final String name, Logger log) {
        try {
            return new SingleThreadedScheduler(name, log);
        } catch (IOException x) {
            signalFatalError(name + " exception", x);
            return null;
        }
    }
}
