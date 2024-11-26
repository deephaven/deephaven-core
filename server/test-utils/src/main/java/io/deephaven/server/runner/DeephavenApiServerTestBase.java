//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.DeephavenChannelImpl;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.auth.CommunityAuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.console.NoConsoleSessionModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.js.JsPluginNoopConsumerModule;
import io.deephaven.server.runner.scheduler.SchedulerDelegatingImplModule;
import io.deephaven.server.session.ClientChannelFactoryModule;
import io.deephaven.server.session.ClientChannelFactoryModule.UserAgent;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;
import io.deephaven.server.session.SslConfigModule;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.calendar.CalendarsFromConfigurationModule;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Manages a single instance of {@link DeephavenApiServer}.
 */
public abstract class DeephavenApiServerTestBase {

    @Module(includes = {
            ClientChannelFactoryModule.class,
            SslConfigModule.class,
    })
    public interface TestClientChannelFactoryModule {

        @Provides
        @UserAgent
        static String providesUserAgent() {
            return BarrageSessionFactoryConfig.userAgent(List.of("deephaven-server-test-utils"));
        }
    }

    @Module(includes = {
            DeephavenApiServerModule.class,
            DeephavenApiConfigModule.class,
            LogModule.class,
            NoConsoleSessionModule.class,
            ServerBuilderInProcessModule.class,
            RpcServerStateInterceptor.Module.class,
            ExecutionContextUnitTestModule.class,
            ObfuscatingErrorTransformerModule.class,
            JsPluginNoopConsumerModule.class,
            SchedulerDelegatingImplModule.class,
            CalendarsFromConfigurationModule.class,
            TestClientChannelFactoryModule.class
    })
    public interface TestModule {

    }

    @Singleton
    @Component(modules = TestModule.class)
    public interface TestComponent {

        void injectFields(DeephavenApiServerTestBase instance);

        @Component.Builder
        interface Builder {

            @BindsInstance
            Builder withServerConfig(ServerConfig serverConfig);

            @BindsInstance
            Builder withOut(@Named("out") PrintStream out);

            @BindsInstance
            Builder withErr(@Named("err") PrintStream err);

            @BindsInstance
            Builder withAuthorizationProvider(AuthorizationProvider authorizationProvider);

            TestComponent build();
        }
    }

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Inject
    ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;

    private LogBuffer logBuffer;
    private SafeCloseable scopeCloseable;

    @Inject
    DeephavenApiServer server;

    @Inject
    Scheduler.DelegatingImpl scheduler;

    @Inject
    Provider<ScriptSession> scriptSessionProvider;

    @Inject
    Provider<ManagedChannelBuilder<?>> managedChannelBuilderProvider;

    @Inject
    RpcServerStateInterceptor serverStateInterceptor;

    protected DeephavenApiServerTestBase.TestComponent.Builder testComponentBuilder() {
        return DaggerDeephavenApiServerTestBase_TestComponent.builder();
    }

    @Before
    public void setUp() throws Exception {
        logBuffer = new LogBuffer(128);
        {
            // Prevent previous failures from cascading
            final Optional<LogBuffer> maybeOldLogBuffer = LogBufferGlobal.getInstance();
            maybeOldLogBuffer.ifPresent(LogBufferGlobal::clear);
        }
        LogBufferGlobal.setInstance(logBuffer);

        final DeephavenApiServerTestConfig config = DeephavenApiServerTestConfig.builder()
                .schedulerPoolSize(4)
                .tokenExpire(sessionTokenExpireTime())
                .port(-1)
                .build();

        testComponentBuilder()
                .withServerConfig(config)
                .withAuthorizationProvider(new CommunityAuthorizationProvider())
                .withOut(System.out)
                .withErr(System.err)
                .build()
                .injectFields(this);

        final PeriodicUpdateGraph updateGraph = server.getUpdateGraph().cast();
        executionContextCloseable = executionContext.open();
        if (updateGraph.isUnitTestModeAllowed()) {
            updateGraph.enableUnitTestMode();
            updateGraph.resetForUnitTests(false);
        }

        server.startForUnitTests();

        scopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);
    }

    @After
    public void tearDown() throws Exception {
        if (scopeCloseable != null) {
            scopeCloseable.close();
            scopeCloseable = null;
        }

        try {
            server.teardownForUnitTests();
        } finally {
            LogBufferGlobal.clear(logBuffer);
        }

        final PeriodicUpdateGraph updateGraph = server.getUpdateGraph().cast();
        if (updateGraph.isUnitTestModeAllowed()) {
            updateGraph.resetForUnitTests(true);
        }
        executionContextCloseable.close();

        scheduler.shutdown();
    }

    public DeephavenApiServer server() {
        return server;
    }

    public LogBuffer logBuffer() {
        return logBuffer;
    }

    public ScriptSession getScriptSession() {
        return scriptSessionProvider.get();
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public RpcServerStateInterceptor serverStateInterceptor() {
        return serverStateInterceptor;
    }

    /**
     * The session token expiration
     *
     * @return the session token expiration
     */
    public Duration sessionTokenExpireTime() {
        // Long expiration is useful for debugging sessions, and the majority of tests should not worry about
        // re-authentication. Any test classes that need an explicit token expiration should override this method.
        return Duration.ofDays(7);
    }

    public ManagedChannelBuilder<?> channelBuilder() {
        return managedChannelBuilderProvider.get();
    }

    public void register(ManagedChannel managedChannel) {
        grpcCleanup.register(managedChannel);
    }

    public DeephavenChannel createChannel() {
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        return new DeephavenChannelImpl(channel);
    }
}
