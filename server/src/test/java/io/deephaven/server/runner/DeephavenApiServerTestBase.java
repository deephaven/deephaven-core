/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.client.ClientDefaultsModule;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.DeephavenChannelImpl;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.auth.CommunityAuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.console.NoConsoleSessionModule;
import io.deephaven.server.log.LogModule;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Manages a single instance of {@link DeephavenApiServer}.
 */
public abstract class DeephavenApiServerTestBase {
    @Singleton
    @Component(modules = {
            DeephavenApiServerModule.class,
            DeephavenApiConfigModule.class,
            LogModule.class,
            NoConsoleSessionModule.class,
            ServerBuilderInProcessModule.class,
            ExecutionContextUnitTestModule.class,
            ClientDefaultsModule.class,
    })
    public interface TestComponent {

        DeephavenApiServer getServer();

        ManagedChannelBuilder<?> channelBuilder();

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

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private TestComponent serverComponent;
    private LogBuffer logBuffer;
    private DeephavenApiServer server;

    @Before
    public void setUp() throws Exception {
        if (((UpdateGraph) ExecutionContext.getContext().getUpdateGraph()).isUnitTestModeAllowed()) {
            ((UpdateGraph) ExecutionContext.getContext().getUpdateGraph()).enableUnitTestMode();
            ((UpdateGraph) ExecutionContext.getContext().getUpdateGraph()).resetForUnitTests(false);
        }

        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        final DeephavenApiServerTestConfig config = DeephavenApiServerTestConfig.builder()
                .schedulerPoolSize(4)
                .tokenExpire(sessionTokenExpireTime())
                .port(-1)
                .build();

        serverComponent = DaggerDeephavenApiServerTestBase_TestComponent.builder()
                .withServerConfig(config)
                .withAuthorizationProvider(new CommunityAuthorizationProvider())
                .withOut(System.out)
                .withErr(System.err)
                .build();

        server = serverComponent.getServer();
        server.startForUnitTests();
    }

    @After
    public void tearDown() throws Exception {

        try {
            server.server().stopWithTimeout(5, TimeUnit.SECONDS);
            server.server().join();
        } finally {
            LogBufferGlobal.clear(logBuffer);
        }

        if (((UpdateGraph) ExecutionContext.getContext().getUpdateGraph()).isUnitTestModeAllowed()) {
            ((UpdateGraph) ExecutionContext.getContext().getUpdateGraph()).resetForUnitTests(true);
        }
    }

    public DeephavenApiServer server() {
        return server;
    }

    public LogBuffer logBuffer() {
        return logBuffer;
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
        return serverComponent.channelBuilder();
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
