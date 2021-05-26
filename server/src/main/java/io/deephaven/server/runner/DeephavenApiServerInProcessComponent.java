package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.grpc.ManagedChannelBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        ServerBuilderInProcessModule.class,
        HealthCheckModule.class
})
public interface DeephavenApiServerInProcessComponent {

    DeephavenApiServer getServer();

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

        DeephavenApiServerInProcessComponent build();
    }
}
