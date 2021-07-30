package io.deephaven.grpc_api.runner;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.sources.chunk.util.pools.MultiChunkPool;
import io.deephaven.grpc_api.appmode.AppModeModule;
import io.deephaven.grpc_api.arrow.ArrowModule;
import io.deephaven.grpc_api.auth.AuthContextModule;
import io.deephaven.grpc_api.console.ConsoleModule;
import io.deephaven.grpc_api.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.grpc_api.console.python.PythonConsoleSessionModule;
import io.deephaven.grpc_api.log.LogModule;
import io.deephaven.grpc_api.session.SessionModule;
import io.deephaven.grpc_api.table.TableModule;
import io.deephaven.grpc_api.util.Scheduler;
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
        SessionModule.class,
        TableModule.class,
        ConsoleModule.class,
        GroovyConsoleSessionModule.class,
        PythonConsoleSessionModule.class
})
public class DeephavenApiServerModule {

    @Provides
    @Singleton
    static Server buildServer(
            final ServerBuilder<?> builder,
            final Set<BindableService> services,
            final Set<ServerInterceptor> interceptors) {

        String chainLoc = System.getenv("DH_TLS_CHAIN");
        if (chainLoc != null) {
            String chainKey = System.getenv("DH_TLS_KEY");
            final File chain = new File(chainLoc);
            final File key = new File(chainKey);
            if (!chain.exists()) {
                throw new IllegalArgumentException("TLS chain " + chain + " does not exist!");
            }
            if (!key.exists()) {
                throw new IllegalArgumentException("TLS key " + key + " does not exist!");
            }
            builder.useTransportSecurity(chain, key);
        }
        for (final BindableService service : services) {
            builder.addService(service);
        }
        for (final ServerInterceptor interceptor : interceptors) {
            builder.intercept(interceptor);
        }

        // none of our rpc methods are allowed to block so we use a direct executor
        return builder.directExecutor().build();
    }


    @Singleton
    static HealthStatusManager healthStatusManager() {
        return new HealthStatusManager();
    }

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
    public static LiveTableMonitor provideLiveTableMonitor() {
        return LiveTableMonitor.DEFAULT;
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
