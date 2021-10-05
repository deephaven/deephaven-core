package io.deephaven.grpc_api.runner;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.util.AbstractScriptSession;
import io.deephaven.db.v2.utils.MemoryTableLoggers;
import io.deephaven.grpc_api.appmode.ApplicationInjector;
import io.deephaven.grpc_api.appmode.ApplicationServiceGrpcImpl;
import io.deephaven.grpc_api.console.ConsoleServiceGrpcImpl;
import io.deephaven.grpc_api.log.LogInit;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DeephavenApiServer {

    private static final Logger log = LoggerFactory.getLogger(DeephavenApiServer.class);

    public static void start(DeephavenApiServer server, SessionService sessionService)
            throws IOException, ClassNotFoundException, InterruptedException {
        // Stop accepting new gRPC requests.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.FIRST,
                server.server::shutdown);

        // Close outstanding sessions to give any gRPCs closure.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.MIDDLE,
                sessionService::onShutdown);

        // Finally wait for gRPC to exit now.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.LAST, () -> {
            try {
                if (!server.server.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error().append(
                            "The gRPC server did not terminate in a reasonable amount of time. Invoking shutdownNow().")
                            .endl();
                    server.server.shutdownNow();
                }
            } catch (final InterruptedException ignored) {
            }
        });

        server.start();
        server.blockUntilShutdown();

    }

    private final Server server;
    private final LiveTableMonitor ltm;
    private final LogInit logInit;
    private final ConsoleServiceGrpcImpl consoleService;
    private final ApplicationInjector applicationInjector;
    private final ApplicationServiceGrpcImpl applicationService;
    private final HealthStatusManager healthStatusManager;

    @Inject
    public DeephavenApiServer(
            final Server server,
            final LiveTableMonitor ltm,
            final LogInit logInit,
            final ConsoleServiceGrpcImpl consoleService,
            final ApplicationInjector applicationInjector,
            final ApplicationServiceGrpcImpl applicationService,
            final HealthStatusManager healthStatusManager) {
        this.server = server;
        this.ltm = ltm;
        this.logInit = logInit;
        this.consoleService = consoleService;
        this.applicationInjector = applicationInjector;
        this.applicationService = applicationService;
        this.healthStatusManager = healthStatusManager;
    }

    public Server server() {
        return server;
    }

    public void start() throws IOException, ClassNotFoundException {
        log.info().append("Configuring logging...").endl();
        logInit.run();

        MemoryTableLoggers.maybeStartStatsCollection();

        log.info().append("Creating/Clearing Script Cache...").endl();
        AbstractScriptSession.createScriptCache();

        log.info().append("Initializing Script Session...").endl();
        consoleService.initializeGlobalScriptSession();

        log.info().append("Starting LTM...").endl();
        ltm.start();

        // inject applications before we start the gRPC server
        applicationInjector.run();

        log.info().append("Starting server...").endl();
        server.start();
    }

    void startForUnitTests() throws IOException {
        // log.info().append("Configuring logging...").endl();
        // logInit.run();

        // MemoryTableLoggers.maybeStartStatsCollection();

        // log.info().append("Creating/Clearing Script Cache...").endl();
        // AbstractScriptSession.createScriptCache();

        // Don't do script sessions yet...
        // log.info().append("Initializing Script Session...").endl();
        // consoleService.initializeGlobalScriptSession();

        // Can't start LTM in Unit test mode ATM
        // log.info().append("Starting LTM...").endl();
        // ltm.start();

        // inject applications before we start the gRPC server
        // applicationInjector.run();

        log.info().append("Starting server...").endl();
        server.start();
        healthStatusManager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
    }

    private void blockUntilShutdown() throws InterruptedException {
        server.awaitTermination();
    }
}
