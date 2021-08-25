package io.deephaven.util.thread;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamingThreadFactory implements ThreadFactory {
    private final AtomicInteger threadCounter = new AtomicInteger(0);
    public final String name;
    private final Class clazz;
    private boolean daemon;
    private final ThreadGroup threadGroup;

    public NamingThreadFactory(final Class clazz, final String name) {
        this(clazz, name, false);
    }

    public NamingThreadFactory(final Class clazz, final String name, boolean daemon) {
        this(null, clazz, name, daemon);
    }

    public NamingThreadFactory(ThreadGroup threadGroup, final Class clazz, final String name, boolean daemon) {
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
