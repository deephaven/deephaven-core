package io.deephaven.grpc_api.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.grpc_api.appmode.AppMode;
import io.deephaven.grpc_api.healthcheck.HealthCheckModule;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.ManagedChannelBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        ServerBuilderInProcessModule.class,
        HealthCheckModule.class
})
public interface DeephavenApiServerInProcessComponent {

    @Singleton
    DeephavenApiServer getServer();

    @Singleton
    SessionService getSessionService();

    ManagedChannelBuilder<?> channelBuilder();

    @Component.Builder
    interface Builder {

        @BindsInstance
        Builder withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

        @BindsInstance
        Builder withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

        @BindsInstance
        Builder withOut(@Named("out") PrintStream out);

        @BindsInstance
        Builder withErr(@Named("err") PrintStream err);

        @BindsInstance
        Builder withAppMode(AppMode appMode);

        DeephavenApiServerInProcessComponent build();
    }
}
