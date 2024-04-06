//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner.scheduler;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.jetbrains.annotations.NotNull;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Provides a {@link Scheduler}.
 */
@Module
public class SchedulerModule {
    @Provides
    @ElementsIntoSet
    static Set<ThreadInitializationFactory> primeThreadInitializers() {
        return Collections.emptySet();
    }

    @Provides
    static ThreadInitializationFactory provideThreadInitializationFactory(Set<ThreadInitializationFactory> factories) {
        return ThreadInitializationFactory.of(factories);
    }

    @Provides
    @Singleton
    public static Scheduler provideScheduler(
            final @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) UpdateGraph updateGraph,
            final @Named("scheduler.poolSize") int poolSize,
            final ThreadInitializationFactory initializationFactory) {
        final ThreadFactory concurrentThreadFactory =
                new ThreadFactory("Scheduler-Concurrent", updateGraph, initializationFactory);
        final ScheduledExecutorService concurrentExecutor =
                new ScheduledThreadPoolExecutor(poolSize, concurrentThreadFactory) {
                    @Override
                    protected void afterExecute(final Runnable task, final Throwable error) {
                        super.afterExecute(task, error);
                        SchedulerModule.afterExecute("concurrentExecutor", task, error);
                    }
                };

        final ThreadFactory serialThreadFactory =
                new ThreadFactory("Scheduler-Serial", updateGraph, initializationFactory);
        final ExecutorService serialExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), serialThreadFactory) {

            @Override
            protected void afterExecute(final Runnable task, final Throwable error) {
                super.afterExecute(task, error);
                SchedulerModule.afterExecute("serialExecutor", task, error);
            }
        };

        return new Scheduler.DelegatingImpl(serialExecutor, concurrentExecutor, Clock.system());
    }

    private static class ThreadFactory extends NamingThreadFactory {
        private final UpdateGraph updateGraph;
        private final ThreadInitializationFactory initializationFactory;

        public ThreadFactory(final String name, final UpdateGraph updateGraph,
                ThreadInitializationFactory initializationFactory) {
            super(DeephavenApiServer.class, name);
            this.updateGraph = updateGraph;
            this.initializationFactory = initializationFactory;
        }

        @Override
        public Thread newThread(@NotNull final Runnable r) {
            return super.newThread(initializationFactory.createInitializer(() -> {
                MultiChunkPool.enableDedicatedPoolForThisThread();
                // noinspection resource
                ExecutionContext.getContext().withUpdateGraph(updateGraph).open();
                r.run();
            }));
        }
    }

    private static void afterExecute(final String executorType, final Runnable task, final Throwable error) {
        if (error != null) {
            report(executorType, error);
        } else if (task instanceof Future<?>) {
            try {
                // Note: this paradigm is not compatible with
                // TODO(deephaven-core#3396): Add io.deephaven.server.util.Scheduler fixed delay support
                ((Future<?>) task).get();
            } catch (final InterruptedException ignored) {
                // noinspection ResultOfMethodCallIgnored
                Thread.interrupted();
            } catch (final CancellationException ignored) {
            } catch (final ExecutionException e) {
                report(executorType, e.getCause());
            }
        }
    }

    private static void report(final String executorType, final Throwable error) {
        ProcessEnvironment.getGlobalFatalErrorReporter().report("Exception while processing " + executorType + " task",
                error);
    }
}
