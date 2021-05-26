package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.server.object.ObjectServiceModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.appmode.AppMode;
import io.deephaven.server.appmode.AppModeModule;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.table.inputtables.InputTableModule;
import io.deephaven.server.uri.UriModule;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.thread.NamingThreadFactory;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.protobuf.services.HealthStatusManager;
import org.jetbrains.annotations.NotNull;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.File;
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

@Module(includes = {
        AppModeModule.class,
        ArrowModule.class,
        AuthContextModule.class,
        LogModule.class,
        UriModule.class,
        SessionModule.class,
        TableModule.class,
        InputTableModule.class,
        ConsoleModule.class,
        ObjectServiceModule.class,
        PluginsModule.class,
        GroovyConsoleSessionModule.class,
        PythonConsoleSessionModule.class
})
public class DeephavenApiServerModule {

    @Provides
    @ElementsIntoSet
    static Set<BindableService> primeServices(HealthStatusManager healthStatusManager) {
        return Collections.singleton(healthStatusManager.getHealthService());
    }

    @Provides
    @ElementsIntoSet
    static Set<ServerInterceptor> primeInterceptors() {
        return Collections.emptySet();
    }

    @Provides
    @Singleton
    public static AppMode provideAppMode() {
        return AppMode.currentMode();
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

        return new Scheduler.DelegatingImpl(serialExecutor, concurrentExecutor);
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
