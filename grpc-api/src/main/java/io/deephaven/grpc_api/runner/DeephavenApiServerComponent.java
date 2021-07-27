package io.deephaven.grpc_api.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.grpc_api.appmode.AppMode;
import io.deephaven.grpc_api.healthcheck.HealthCheckModule;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        HealthCheckModule.class,
        ServerBuilderModule.class
})
public interface DeephavenApiServerComponent {

    @Singleton
    DeephavenApiServer getServer();

    @Singleton
    SessionService getSessionService();

    @Component.Builder
    interface Builder {
        @BindsInstance
        Builder withPort(@Named("grpc.port") int port);

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

        DeephavenApiServerComponent build();
    }

    static void startMain(PrintStream out, PrintStream err, final Configuration config)
            throws IOException, InterruptedException, ClassNotFoundException {
        final DeephavenApiServerComponent injector = DaggerDeephavenApiServerComponent
                .builder()
                .withSchedulerPoolSize(4)
                .withPort(config.getIntegerWithDefault("grpc-api.port", 8888))
                .withSessionTokenExpireTmMs(300000) // defaults to 5 min
                .withOut(out)
                .withErr(err)
                .withAppMode(AppMode.currentMode())
                .build();
        final DeephavenApiServer server = injector.getServer();
        final SessionService sessionService = injector.getSessionService();
        DeephavenApiServer.start(server, sessionService);
    }
}
