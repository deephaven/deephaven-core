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
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
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
        final String DEEPHAVEN_CONSOLE_TYPE = "deephaven.console.type";
        boolean configuredConsole = Configuration.getInstance().hasProperty(DEEPHAVEN_CONSOLE_TYPE);

        if (!configuredConsole && scriptTypes.size() == 1) {
            // if there is only one; use it
            return scriptTypes.values().iterator().next().get();
        }

        // otherwise, assume we want python...
        String scriptSessionType = Configuration.getInstance().getStringWithDefault(DEEPHAVEN_CONSOLE_TYPE, "python");
        if (!scriptTypes.containsKey(scriptSessionType)) {
            throw new IllegalArgumentException("Console type not found: " + scriptSessionType);
        }
        return scriptTypes.get(scriptSessionType).get();
    }

    @Provides
    @Singleton
    public static Scheduler provideScheduler(final @Named("scheduler.poolSize") int poolSize) {
        final ThreadFactory concurrentThreadFactory = new ThreadFactory("Scheduler-Concurrent");
        final ScheduledExecutorService concurrentExecutor =
                new ScheduledThreadPoolExecutor(poolSize, concurrentThreadFactory) {
                    @Override
                    protected void afterExecute(final Runnable task, final Throwable error) {
                        super.afterExecute(task, error);
                        DeephavenApiServerModule.afterExecute("concurrentExecutor", task, error);
                    }
                };

        final ThreadFactory serialThreadFactory = new ThreadFactory("Scheduler-Serial");
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
    public static UpdateGraphProcessor provideUpdateGraphProcessor() {
        return UpdateGraphProcessor.DEFAULT;
    }

    private static class ThreadFactory extends NamingThreadFactory {
        public ThreadFactory(final String name) {
            super(DeephavenApiServer.class, name, true);
        }

        @Override
        public Thread newThread(final @NotNull Runnable r) {
            return super.newThread(() -> {
                MultiChunkPool.enableDedicatedPoolForThisThread();
                r.run();
            });
        }
    }
}
