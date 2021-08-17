package io.deephaven.grpc_api.runner;

import io.deephaven.db.util.AbstractScriptSession;
import io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.MemoryTableLoggers;
import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.grpc_api.log.LogInit;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.Server;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

public class DeephavenApiServer {
    @Singleton
    @Component(modules = {
            DeephavenApiServerModule.class,
    })
    public interface ServerComponent {
        @Singleton
        DeephavenApiServer getServer();
        @Singleton
        SessionService getSessionService();

        @Component.Builder
        interface Builder {
            @BindsInstance Builder withPort(@Named("grpc.port") int port);
            @BindsInstance Builder withSchedulerPoolSize(@Named("scheduler.poolSize") int numThreads);
            @BindsInstance Builder withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);
            @BindsInstance Builder withOut(@Named("out") PrintStream out);
            @BindsInstance Builder withErr(@Named("err") PrintStream err);
            ServerComponent build();
        }
    }

    public static void startMain(PrintStream out, PrintStream err) throws IOException, InterruptedException {
        final ServerComponent injector = DaggerDeephavenApiServer_ServerComponent
                .builder()
                .withPort(8080)
                .withSchedulerPoolSize(4)
                .withSessionTokenExpireTmMs(300000) // defaults to 5 min
                .withOut(out)
                .withErr(err)
                .build();
        final DeephavenApiServer server = injector.getServer();
        final SessionService sessionService = injector.getSessionService();

        // Stop accepting new gRPC requests.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.FIRST, server.server::shutdown);

        // Close outstanding sessions to give any gRPCs closure.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.MIDDLE, sessionService::closeAllSessions);

        // Finally wait for gRPC to exit now.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.LAST, () -> {
            try {
                if (!server.server.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error().append("The gRPC server did not terminate in a reasonable amount of time. Invoking shutdownNow().").endl();
                    server.server.shutdownNow();
                }
            } catch (final InterruptedException ignored) {
            }
        });

        server.start();
        server.blockUntilShutdown();
    }

    private static final Logger log = LoggerFactory.getLogger(DeephavenApiServer.class);

    private final Server server;
    private final LiveTableMonitor ltm;
    private final LogInit logInit;
    private final ConsoleServiceGrpcImpl consoleService;

    @Inject
    public DeephavenApiServer(
            final Server server,
            final LiveTableMonitor ltm,
            final LogInit logInit,
            final ConsoleServiceGrpcImpl consoleService) {
        this.server = server;
        this.ltm = ltm;
        this.logInit = logInit;
        this.consoleService = consoleService;
    }

    private void start() throws IOException {
        log.info().append("Configuring logging...").endl();
        logInit.run();

        MemoryTableLoggers.maybeStartStatsCollection();

        log.info().append("Creating/Clearing Script Cache...").endl();
        AbstractScriptSession.createScriptCache();

        log.info().append("Initializing Script Session...").endl();
        consoleService.initializeGlobalScriptSession();

        log.info().append("Starting LTM...").endl();
        ltm.start();

        log.info().append("Starting server...").endl();
        server.start();
    }

    private void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }
}
