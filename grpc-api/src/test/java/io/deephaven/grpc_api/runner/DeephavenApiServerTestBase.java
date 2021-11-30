package io.deephaven.grpc_api.runner;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.grpc_api.DeephavenChannel;
import io.deephaven.grpc_api.appmode.AppMode;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;

import java.util.concurrent.TimeUnit;

/**
 * Manages a single instance of {@link DeephavenApiServer}.
 */
public abstract class DeephavenApiServerTestBase {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private DeephavenApiServerInProcessComponent serverComponent;
    private LogBuffer logBuffer;
    private DeephavenApiServer server;
    private SafeCloseable scopeCloseable;

    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);

        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        serverComponent = DaggerDeephavenApiServerInProcessComponent
                .builder()
                .withSchedulerPoolSize(4)
                .withSessionTokenExpireTmMs(sessionTokenExpireTmMs())
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
            if (!server.server().shutdown().awaitTermination(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("Server not shutdown within 5 seconds");
            }
        } finally {
            LogBufferGlobal.clear(logBuffer);
            UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
        }
    }

    /**
     * The session token expiration, in milliseconds.
     *
     * @return the session token expiration, in milliseconds.
     */
    public long sessionTokenExpireTmMs() {
        // Long expiration is useful for debugging sessions, and the majority of tests should not worry about
        // re-authentication. Any test classes that need an explicit token expiration should override this method.
        return TimeUnit.DAYS.toMillis(7);
    }

    public ManagedChannelBuilder<?> channelBuilder() {
        return serverComponent.channelBuilder();
    }

    public void register(ManagedChannel managedChannel) {
        grpcCleanup.register(managedChannel);
    }

    public DeephavenChannel channel() {
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        return new DeephavenChannel(channel);
    }
}
