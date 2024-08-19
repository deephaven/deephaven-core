//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorderState;
import io.deephaven.engine.table.impl.util.AsyncErrorLogger;
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.engine.table.impl.util.ServerStateTracker;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.server.appmode.ApplicationInjector;
import io.deephaven.server.session.SessionFactoryCreator;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.log.LogInit;
import io.deephaven.server.plugin.PluginRegistration;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolvers;
import io.deephaven.uri.resolver.UriResolversInstance;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.InternalUseOnly;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Entrypoint for the Deephaven gRPC server, starting the various engine and script components, running any specified
 * application, and enabling the gRPC endpoints to be accessed by consumers.
 */
public class DeephavenApiServer {
    private static final Logger log = LoggerFactory.getLogger(DeephavenApiServer.class);

    private static final long CHECK_SCOPE_CHANGES_INTERVAL_MILLIS =
            Configuration.getInstance().getLongForClassWithDefault(
                    DeephavenApiServer.class, "checkScopeChangesIntervalMillis", 100);

    private static DeephavenApiServer INSTANCE;

    @InternalUseOnly
    public static DeephavenApiServer getInstance() {
        synchronized (DeephavenApiServer.class) {
            return Objects.requireNonNull(INSTANCE);
        }
    }

    private static void setInstance(DeephavenApiServer instance) {
        Objects.requireNonNull(instance);
        synchronized (DeephavenApiServer.class) {
            if (INSTANCE != null) {
                throw new IllegalStateException();
            }
            INSTANCE = instance;
        }
    }

    private static void clearInstance(DeephavenApiServer expected) {
        Objects.requireNonNull(expected);
        synchronized (DeephavenApiServer.class) {
            if (INSTANCE != expected) {
                throw new IllegalStateException();
            }
            INSTANCE = null;
        }
    }

    private final GrpcServer server;
    private final UpdateGraph ug;
    private final LogInit logInit;
    private final Provider<Set<BusinessCalendar>> calendars;
    private final Scheduler scheduler;
    private final Provider<ScriptSession> scriptSessionProvider;
    private final PluginRegistration pluginRegistration;
    private final ApplicationInjector applicationInjector;
    private final UriResolvers uriResolvers;
    private final SessionService sessionService;
    private final Map<String, AuthenticationRequestHandler> authenticationHandlers;
    private final Provider<ExecutionContext> executionContextProvider;
    private final ServerConfig serverConfig;
    private final SessionFactoryCreator sessionFactoryCreator;

    @Inject
    public DeephavenApiServer(
            final GrpcServer server,
            @Named(PeriodicUpdateGraph.DEFAULT_UPDATE_GRAPH_NAME) final UpdateGraph ug,
            final LogInit logInit,
            final Provider<Set<BusinessCalendar>> calendars,
            final Scheduler scheduler,
            final Provider<ScriptSession> scriptSessionProvider,
            final PluginRegistration pluginRegistration,
            final ApplicationInjector applicationInjector,
            final UriResolvers uriResolvers,
            final SessionService sessionService,
            final Map<String, AuthenticationRequestHandler> authenticationHandlers,
            final Provider<ExecutionContext> executionContextProvider,
            final ServerConfig serverConfig,
            final SessionFactoryCreator sessionFactoryCreator) {
        this.server = server;
        this.ug = ug;
        this.logInit = logInit;
        this.calendars = calendars;
        this.scheduler = scheduler;
        this.scriptSessionProvider = scriptSessionProvider;
        this.pluginRegistration = pluginRegistration;
        this.applicationInjector = applicationInjector;
        this.uriResolvers = uriResolvers;
        this.sessionService = sessionService;
        this.authenticationHandlers = authenticationHandlers;
        this.executionContextProvider = executionContextProvider;
        this.serverConfig = serverConfig;
        this.sessionFactoryCreator = sessionFactoryCreator;
    }

    @VisibleForTesting
    public GrpcServer server() {
        return server;
    }

    @VisibleForTesting
    public SessionService sessionService() {
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
        setInstance(this);

        // Prevent new gRPC calls from being started
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.FIRST,
                server::beginShutdown);

        // Now that no new gRPC calls may be made, close outstanding sessions to give any clients closure
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.MIDDLE,
                sessionService::onShutdown);

        // Finally, wait for the http server to be finished stopping
        ProcessEnvironment.getGlobalShutdownManager().registerTask(ShutdownManager.OrderingCategory.LAST, () -> {
            try {
                final Duration duration = serverConfig.shutdownTimeout();
                server.stopWithTimeout(duration.toNanos(), TimeUnit.NANOSECONDS);
                server.join();
                clearInstance(DeephavenApiServer.this);
            } catch (final InterruptedException ignored) {
            }
        });

        log.info().append("Configuring logging...").endl();
        logInit.run();

        for (BusinessCalendar calendar : calendars.get()) {
            Calendars.addCalendar(calendar);
        }

        log.info().append("Initializing Script Session...").endl();
        checkScopeChanges(scriptSessionProvider.get());
        pluginRegistration.registerAll();

        log.info().append("Initializing Execution Context for Main Thread...").endl();
        // noinspection resource
        executionContextProvider.get().open();

        log.info().append("Starting Update Graph...").endl();
        getUpdateGraph().<PeriodicUpdateGraph>cast().start();

        EngineMetrics.maybeStartStatsCollection();

        log.info().append("Starting Performance Trackers...").endl();
        QueryPerformanceRecorderState.installPoolAllocationRecorder();
        QueryPerformanceRecorderState.installUpdateGraphLockInstrumentation();
        ServerStateTracker.start();
        AsyncErrorLogger.init();

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
        return this;
    }

    private void checkScopeChanges(ScriptSession scriptSession) {
        try (SafeCloseable ignored = LivenessScopeStack.open()) {
            scriptSession.observeScopeChanges();
        }
        scheduler.runAfterDelay(CHECK_SCOPE_CHANGES_INTERVAL_MILLIS, () -> {
            checkScopeChanges(scriptSession);
        });
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
        setInstance(this);
        pluginRegistration.registerAll();
        applicationInjector.run();
        executionContextProvider.get().getQueryLibrary().updateVersionString("DEFAULT");

        log.info().append("Starting server...").endl();
        server.start();
    }

    void teardownForUnitTests() throws InterruptedException {
        try {
            server.stopWithTimeout(5, TimeUnit.SECONDS);
            server.join();
        } finally {
            clearInstance(this);
        }
    }

    @VisibleForTesting
    public UpdateGraph getUpdateGraph() {
        return ug;
    }

    @InternalUseOnly
    @ScriptApi
    public SessionFactoryCreator sessionFactoryCreator() {
        return sessionFactoryCreator;
    }
}
