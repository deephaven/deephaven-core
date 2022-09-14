/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.table.impl.util.MemoryTableLoggers;
import io.deephaven.engine.table.impl.util.ServerStateTracker;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.appmode.ApplicationInjector;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.log.LogInit;
import io.deephaven.server.plugin.PluginRegistration;
import io.deephaven.server.session.SessionService;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolvers;
import io.deephaven.uri.resolver.UriResolversInstance;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Entrypoint for the Deephaven gRPC server, starting the various engine and script components, running any specified
 * application, and enabling the gRPC endpoints to be accessed by consumers.
 */
public class DeephavenApiServer {
    private static final Logger log = LoggerFactory.getLogger(DeephavenApiServer.class);

    private final GrpcServer server;
    private final UpdateGraphProcessor ugp;
    private final LogInit logInit;
    private final Provider<ScriptSession> scriptSessionProvider;
    private final PluginRegistration pluginRegistration;
    private final ApplicationInjector applicationInjector;
    private final UriResolvers uriResolvers;
    private final SessionService sessionService;
    private final Map<String, AuthenticationRequestHandler> authenticationHandlers;
    private final Provider<ExecutionContext> executionContextProvider;
    private final ServerConfig serverConfig;
    private final HealthStatusManager healthStatusManager;

    @Inject
    public DeephavenApiServer(
            final GrpcServer server,
            final UpdateGraphProcessor ugp,
            final LogInit logInit,
            final Provider<ScriptSession> scriptSessionProvider,
            final PluginRegistration pluginRegistration,
            final ApplicationInjector applicationInjector,
            final UriResolvers uriResolvers,
            final SessionService sessionService,
            final Map<String, AuthenticationRequestHandler> authenticationHandlers,
            final Provider<ExecutionContext> executionContextProvider,
            final ServerConfig serverConfig,
            final HealthStatusManager healthStatusManager) {
        this.server = server;
        this.ugp = ugp;
        this.logInit = logInit;
        this.scriptSessionProvider = scriptSessionProvider;
        this.pluginRegistration = pluginRegistration;
        this.applicationInjector = applicationInjector;
        this.uriResolvers = uriResolvers;
        this.sessionService = sessionService;
        this.authenticationHandlers = authenticationHandlers;
        this.executionContextProvider = executionContextProvider;
        this.serverConfig = serverConfig;
        this.healthStatusManager = healthStatusManager;
    }

    @VisibleForTesting
    public GrpcServer server() {
        return server;
    }

    @VisibleForTesting
    SessionService sessionService() {
        return sessionService;
    }


    /**
     * Starts the various server components, and returns without blocking. Shutdown is mediated by the ShutdownManager,
     * who will call the gRPC server to shut it down when the process is itself shutting down.
     *
     * @throws IOException thrown in event of an error with logging, finding and running an application, and starting
     *         the gRPC service.
     * @throws ClassNotFoundException thrown if a class can't be found while finding and running an application.
     */
    public DeephavenApiServer run() throws IOException, ClassNotFoundException, TimeoutException {
        // Stop accepting new gRPC requests.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.FIRST,
                () -> {
                    // healthStatusManager.enterTerminalState() must be called before server.stopWithTimeout().
                    // If we add multiple `OrderingCategory.FIRST` callbacks, they'll execute in the wrong order.
                    healthStatusManager.enterTerminalState();
                    server.stopWithTimeout(10, TimeUnit.SECONDS);
                });

        // Close outstanding sessions to give any gRPCs closure.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.MIDDLE,
                sessionService::onShutdown);

        // Finally wait for gRPC to exit now.
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.LAST, () -> {
            try {
                server.join();
            } catch (final InterruptedException ignored) {
            }
        });

        log.info().append("Configuring logging...").endl();
        logInit.run();

        log.info().append("Creating/Clearing Script Cache...").endl();
        AbstractScriptSession.createScriptCache();

        log.info().append("Initializing Script Session...").endl();

        scriptSessionProvider.get();
        pluginRegistration.registerAll();

        log.info().append("Starting UGP...").endl();
        ugp.start();

        MemoryTableLoggers.maybeStartStatsCollection();

        log.info().append("Starting Performance Trackers...").endl();
        QueryPerformanceRecorder.installPoolAllocationRecorder();
        QueryPerformanceRecorder.installUpdateGraphLockInstrumentation();
        UpdatePerformanceTracker.start();
        ServerStateTracker.start();

        for (UriResolver resolver : uriResolvers.resolvers()) {
            log.debug().append("Found table resolver ").append(resolver.getClass().toString()).endl();
        }
        UriResolversInstance.init(uriResolvers);

        // inject applications before we start the gRPC server
        applicationInjector.run();

        log.info().append("Initializing Authentication...").endl();
        final String targetUrl = serverConfig.targetUrlOrDefault();
        authenticationHandlers.forEach((name, handler) -> handler.initialize(targetUrl));

        log.info().append("Starting server...").endl();
        server.start();
        log.info().append("Server started on port ").append(server.getPort()).endl();
        healthStatusManager.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        return this;
    }

    /**
     * Blocks until the server exits.
     *
     * @throws InterruptedException thrown if this thread is interrupted while blocking for the server to halt.
     */
    public void join() throws InterruptedException {
        server.join();
    }


    void startForUnitTests() throws Exception {
        pluginRegistration.registerAll();
        applicationInjector.run();
        executionContextProvider.get().getQueryLibrary().updateVersionString("DEFAULT");

        log.info().append("Starting server...").endl();
        server.start();
    }

}
