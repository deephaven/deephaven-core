/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.util;

import io.deephaven.server.util.Scheduler.DelegatingImpl;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

public final class SchedulerShutdown {
    private final DelegatingImpl scheduler;

    @Inject
    public SchedulerShutdown(Scheduler scheduler) {
        this.scheduler = (DelegatingImpl) scheduler;
    }

    public void run() throws InterruptedException {
        scheduler.concurrentDelegate().shutdownNow();
        scheduler.serialDelegate().shutdownNow();
        if (!scheduler.concurrentDelegate().awaitTermination(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("concurrentDelegate not shutdown within 5 seconds");
        }
        if (!scheduler.serialDelegate().awaitTermination(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("serialDelegate not shutdown within 5 seconds");
        }
    }
}
