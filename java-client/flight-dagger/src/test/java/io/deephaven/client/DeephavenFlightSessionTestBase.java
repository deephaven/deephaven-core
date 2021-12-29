package io.deephaven.client;

import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.server.runner.DeephavenApiServerTestBase;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class DeephavenFlightSessionTestBase extends DeephavenApiServerTestBase {

    BufferAllocator bufferAllocator;
    ScheduledExecutorService sessionScheduler;
    FlightSession flightSession;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ManagedChannel channel = channelBuilder().build();
        register(channel);
        sessionScheduler = Executors.newScheduledThreadPool(2);
        bufferAllocator = new RootAllocator();
        flightSession = DaggerDeephavenFlightRoot.create().factoryBuilder().allocator(bufferAllocator)
                .managedChannel(channel).scheduler(sessionScheduler).build().newFlightSession();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        flightSession.close();
        flightSession.session().close();
        bufferAllocator.close();
        sessionScheduler.shutdownNow();
        if (!sessionScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new RuntimeException("Scheduler not shutdown within 5 seconds");
        }
        super.tearDown();
    }
}
