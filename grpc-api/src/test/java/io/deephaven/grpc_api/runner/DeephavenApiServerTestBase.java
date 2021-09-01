package io.deephaven.grpc_api.runner;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.grpc_api.DeephavenChannel;
import io.deephaven.grpc_api.appmode.AppMode;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
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

    @Before
    public void setUp() throws Exception {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);

        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        serverComponent = DaggerDeephavenApiServerInProcessComponent
                .builder()
                .withSchedulerPoolSize(4)
                .withSessionTokenExpireTmMs(300000) // defaults to 5 min
                .withOut(System.out)
                .withErr(System.err)
                .withAppMode(AppMode.API_ONLY)
                .build();

        server = serverComponent.getServer();
        server.startForUnitTests();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (!server.server().shutdown().awaitTermination(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("Server not shutdown within 5 seconds");
            }
        } finally {
            LogBufferGlobal.clear(logBuffer);
            LiveTableMonitor.DEFAULT.resetForUnitTests(true);
        }
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
