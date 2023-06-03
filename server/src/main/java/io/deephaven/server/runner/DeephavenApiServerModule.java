/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.base.clock.Clock;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.appmode.ApplicationsModule;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.hierarchicaltable.HierarchicalTableServiceModule;
import io.deephaven.server.notebook.FilesystemStorageServiceModule;
import io.deephaven.server.object.ObjectServiceModule;
import io.deephaven.server.partitionedtable.PartitionedTableServiceModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.appmode.AppModeModule;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.table.inputtables.InputTableModule;
import io.deephaven.server.uri.UriModule;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.thread.NamingThreadFactory;
import io.deephaven.util.thread.ThreadInitializationFactory;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import org.jetbrains.annotations.NotNull;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.Map;
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

@Module(includes = {
        AppModeModule.class,
        ApplicationsModule.class,
        ArrowModule.class,
        AuthContextModule.class,
        UriModule.class,
        SessionModule.class,
        TableModule.class,
        InputTableModule.class,
        ConsoleModule.class,
        ObjectServiceModule.class,
        PluginsModule.class,
        PartitionedTableServiceModule.class,
        HierarchicalTableServiceModule.class,
        FilesystemStorageServiceModule.class,
        ConfigServiceModule.class,
})
public class DeephavenApiServerModule {

    @Provides
    @ElementsIntoSet
    static Set<BindableService> primeServices() {
        return Collections.emptySet();
    }

    @Provides
    @ElementsIntoSet
    static Set<ServerInterceptor> primeInterceptors() {
        return Collections.emptySet();
    }

    @Provides
    @Singleton
    public ScriptSession provideScriptSession(Map<String, Provider<ScriptSession>> scriptTypes) {
        // Check which script language is configured
        String scriptSessionType = Configuration.getInstance().getProperty("deephaven.console.type");

        // Emit an error if the selected language isn't provided
        if (!scriptTypes.containsKey(scriptSessionType)) {
            throw new IllegalArgumentException("Console type not found: " + scriptSessionType);
        }
        return scriptTypes.get(scriptSessionType).get();
    }

    @Provides
    @Singleton
    public static Scheduler provideScheduler(
            final @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) UpdateGraph updateGraph,
            final @Named("scheduler.poolSize") int poolSize) {
        final ThreadFactory concurrentThreadFactory = new ThreadFactory("Scheduler-Concurrent", updateGraph);
        final ScheduledExecutorService concurrentExecutor =
                new ScheduledThreadPoolExecutor(poolSize, concurrentThreadFactory) {
                    @Override
                    protected void afterExecute(final Runnable task, final Throwable error) {
                        super.afterExecute(task, error);
                        DeephavenApiServerModule.afterExecute("concurrentExecutor", task, error);
                    }
                };

        final ThreadFactory serialThreadFactory = new ThreadFactory("Scheduler-Serial", updateGraph);
        final ExecutorService serialExecutor = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), serialThreadFactory) {

            @Override
            protected void afterExecute(final Runnable task, final Throwable error) {
                super.afterExecute(task, error);
                DeephavenApiServerModule.afterExecute("serialExecutor", task, error);
            }
        };

        return new Scheduler.DelegatingImpl(serialExecutor, concurrentExecutor, Clock.system());
    }

    private static void report(final String executorType, final Throwable error) {
        ProcessEnvironment.getGlobalFatalErrorReporter().report("Exception while processing " + executorType + " task",
                error);
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

    @Provides
    @Singleton
    @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
    public static UpdateGraph provideUpdateGraph() {
        return PeriodicUpdateGraph.newBuilder(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME)
                .numUpdateThreads(PeriodicUpdateGraph.NUM_THREADS_DEFAULT_UPDATE_GRAPH)
                .existingOrBuild();
    }

    private static class ThreadFactory extends NamingThreadFactory {
        private final UpdateGraph updateGraph;

        public ThreadFactory(final String name, final UpdateGraph updateGraph) {
            super(DeephavenApiServer.class, name);
            this.updateGraph = updateGraph;
        }

        @Override
        public Thread newThread(final @NotNull Runnable r) {
            return super.newThread(ThreadInitializationFactory.wrapRunnable(() -> {
                MultiChunkPool.enableDedicatedPoolForThisThread();
                // noinspection resource
                ExecutionContext.getContext().withUpdateGraph(updateGraph).open();
                r.run();
            }));
        }
    }
}
