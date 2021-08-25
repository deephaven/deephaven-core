/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.logger.Logger;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Creates a ThreadPoolExecutor which can then be used to submit or execute tasks. This is intended for cases where a
 * relatively small number of threads can handle most circumstances, but occasional abnormal events may exceed
 * expectations. The executor has the following characteristics:
 * <ul>
 * <li>Starting core and maximum thread pool sizes defined at creation time</li>
 * <li>If an attempted execution exceeds the maximum number of allowed executor threads, a new Thread will be created
 * dynamically instead of generating an exception</li>
 * </ul>
 * If the executor has been shut down, any excess events will be discarded.
 *
 * To create one of these executors, use {@link ExpandingThreadPoolExecutorFactory#createThreadPoolExecutor}.
 */
public class ExpandingThreadPoolExecutorFactory {

    // Stop anything from creating one of these - only use the createThreadPoolExecutor method
    private ExpandingThreadPoolExecutorFactory() {}

    /**
     * Class to handle rejection events from a ThreadPoolExecutor by creating a new Thread to run the task, unless the
     * executor has been shut down, in which case the task is discarded.
     */
    private static class RejectedExecutionPolicy implements RejectedExecutionHandler, LogOutputAppendable {
        final Logger log;
        final String executorName;
        final String threadName;
        private final AtomicInteger executorThreadNumber;

        /**
         * Creates a {@code RejectedExecutionPolicy}.
         *
         * @param log a Logger
         * @param executorName a name to be used when logging thread startup messages
         * @param threadName the name prefix for new threads
         */
        private RejectedExecutionPolicy(final Logger log,
                final String executorName,
                final String threadName,
                final AtomicInteger executorThreadNumber) {
            this.log = log;
            this.executorName = executorName;
            this.threadName = threadName;
            this.executorThreadNumber = executorThreadNumber;
        }

        /**
         * Executes task r in a new thread, unless the executor has been shut down, in which case the task is discarded.
         *
         * @param r the runnable task requested to be executed
         * @param e the executor attempting to execute this task
         */
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (!e.isShutdown()) {
                final String newThreadName = threadName + executorThreadNumber.getAndIncrement();
                log.warn().append("Executor has run out of threads for ").append(this).append(", creating new thread ")
                        .append(newThreadName).endl();
                newDaemonThread(r, newThreadName).start();
            }
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("executor ").append(executorName);
        }
    }

    private static Thread newDaemonThread(Runnable r, final String name) {
        final Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /**
     * Create a {@link ThreadPoolExecutor} with the characteristics defined in
     * {@link ExpandingThreadPoolExecutorFactory}.
     *
     * @param log a Logger to log messages
     * @param corePoolSize the core pool size (the executor will use this value for the initial core and maximum pool
     *        sizes)
     * @param keepAliveMinutes the number of minutes to keep alive core threads
     * @param executorName the name of the executor, used when logging dynamic thread creation
     * @param poolThreadNamePrefix the prefix for thread pool threads
     * @param dynamicThreadNamePrefix the prefix for dynamic (overflow) threads created when the maximum number of pool
     *        threads is exceeded
     */
    public static ThreadPoolExecutor createThreadPoolExecutor(final Logger log,
            final int corePoolSize,
            final int keepAliveMinutes,
            final String executorName,
            final String poolThreadNamePrefix,
            final String dynamicThreadNamePrefix) {
        final AtomicInteger executorThreadNumber = new AtomicInteger(1);
        return new ThreadPoolExecutor(corePoolSize,
                corePoolSize,
                keepAliveMinutes,
                TimeUnit.MINUTES,
                new SynchronousQueue<>(),
                r -> newDaemonThread(r, poolThreadNamePrefix + executorThreadNumber.getAndIncrement()),
                new RejectedExecutionPolicy(log, executorName, dynamicThreadNamePrefix, executorThreadNumber));
    }
}
