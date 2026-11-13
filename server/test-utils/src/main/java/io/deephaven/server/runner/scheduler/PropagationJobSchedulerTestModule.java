//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner.scheduler;

import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.util.thread.NamingThreadFactory;

import javax.inject.Named;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Provides the scheduler that {@link BarrageMessageProducer}s write each propagation phase to their subscribers on, for
 * test components that provide their own {@link io.deephaven.server.util.Scheduler} instead of including
 * {@link SchedulerModule}. Like the server's, it is backed by a pool of helper threads, so these tests write to their
 * subscribers in parallel too.
 */
@Module
public interface PropagationJobSchedulerTestModule {
    @Provides
    @Named(BarrageMessageProducer.PROPAGATION_JOB_SCHEDULER)
    static Supplier<JobScheduler> providePropagationJobScheduler() {
        final int threads = Math.max(2, BarrageMessageProducer.PROPAGATION_THREADS);
        final Executor helperPool = ExecutorJobScheduler.newHelperPool(threads - 1,
                new NamingThreadFactory(PropagationJobSchedulerTestModule.class, "Barrage-Propagation"));
        return () -> new ExecutorJobScheduler(helperPool, threads);
    }
}
