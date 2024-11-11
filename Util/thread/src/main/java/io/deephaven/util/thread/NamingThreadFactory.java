//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.thread;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamingThreadFactory implements ThreadFactory {
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    private final String name;
    private final Class<?> clazz;
    private final boolean daemon;
    private final ThreadGroup threadGroup;

    /**
     * Creates a thread factory using the provided class and name as part of the thread name. All created threads will
     * be daemon threads.
     * 
     * @param clazz a class to use when naming each thread
     * @param name a name component to add after the class name when naming each thread
     */
    public NamingThreadFactory(final Class<?> clazz, final String name) {
        this(clazz, name, true);
    }

    /**
     * Creates a thread factory using the provided class and name as part of the thread name.
     * 
     * @param clazz a class to use when naming each thread
     * @param name a name component to add after the class name when naming each thread
     * @param daemon true to make each thread a daemon thread
     */
    public NamingThreadFactory(final Class<?> clazz, final String name, boolean daemon) {
        this(null, clazz, name, daemon);
    }

    /**
     * Creates a thread factory using the provided class and name as part of the thread name.
     * 
     * @param threadGroup a thread group to add each thread to
     * @param clazz a class to use when naming each thread
     * @param name a name component to add after the class name when naming each thread
     * @param daemon true to make each thread a daemon thread
     */
    public NamingThreadFactory(ThreadGroup threadGroup, final Class<?> clazz, final String name, boolean daemon) {
        this.threadGroup = threadGroup;
        this.clazz = clazz;
        this.name = name;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(@NotNull final Runnable r) {
        final Thread thread =
                new Thread(threadGroup, r, clazz.getSimpleName() + "-" + name + "-" + threadCounter.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}
