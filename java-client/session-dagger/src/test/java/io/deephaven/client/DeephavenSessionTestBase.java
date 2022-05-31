package io.deephaven.client;

import io.deephaven.client.impl.Session;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class DeephavenSessionTestBase extends DeephavenApiServerTestBase {

    private ScheduledExecutorService sessionScheduler;
    Session session;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        sessionScheduler = Executors.newScheduledThreadPool(2);
        session = DaggerDeephavenSessionRoot.create().factoryBuilder().managedChannel(channel)
                .scheduler(sessionScheduler).build().newSession();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        session.close();
        sessionScheduler.shutdownNow();
        if (!sessionScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Scheduler not shutdown within 5 seconds");
        }
        super.tearDown();
    }
}
