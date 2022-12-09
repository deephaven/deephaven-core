/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.auth.CommunityAuthorizationProvider;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.console.NoConsoleSessionModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.util.SafeCloseable;
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

    private TestComponent serverComponent;
    private LogBuffer logBuffer;
    private DeephavenApiServer server;
    private SafeCloseable scopeCloseable;

    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);

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

        scopeCloseable = LivenessScopeStack.open(new LivenessScope(true), true);
    }

    @After
    public void tearDown() throws Exception {
        scopeCloseable.close();

        try {
            server.server().stopWithTimeout(5, TimeUnit.SECONDS);
            server.server().join();
        } finally {
            LogBufferGlobal.clear(logBuffer);
        }
    }

    public DeephavenApiServer server() {
        return server;
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
        return new DeephavenChannel(channel);
    }
}
