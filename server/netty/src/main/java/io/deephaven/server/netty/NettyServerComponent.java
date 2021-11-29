package io.deephaven.server.netty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.runner.DeephavenApiServerModule;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        HealthCheckModule.class,
        NettyServerModule.class
})
public interface NettyServerComponent {
    DeephavenApiServer getServer();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder withPort(@Named("http.port") int port);

        @BindsInstance
        Builder withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);

        @BindsInstance
        Builder withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

        @BindsInstance
        Builder withOut(@Named("out") PrintStream out);

        @BindsInstance
        Builder withErr(@Named("err") PrintStream err);

        NettyServerComponent build();
    }
}
