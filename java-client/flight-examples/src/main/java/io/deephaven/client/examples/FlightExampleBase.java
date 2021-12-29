package io.deephaven.client.examples;

import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactory;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.ArgGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class FlightExampleBase implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    BufferAllocator bufferAllocator = new RootAllocator();

    protected abstract void execute(FlightSession flight) throws Exception;

    @Override
    public final Void call() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel = ConnectOptions.open(connectOptions);
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        FlightSessionFactory flightSessionFactory =
                DaggerDeephavenFlightRoot.create().factoryBuilder()
                        .managedChannel(managedChannel)
                        .scheduler(scheduler)
                        .allocator(bufferAllocator)
                        .build();

        FlightSession flightSession = flightSessionFactory.newFlightSession();

        try {
            try {
                execute(flightSession);
            } finally {
                flightSession.close();
            }
        } finally {
            flightSession.session().closeFuture().get(5, TimeUnit.SECONDS);
        }

        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        return null;
    }

    private static void onShutdown(ScheduledExecutorService scheduler,
            ManagedChannel managedChannel) {
        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Scheduler not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            if (!managedChannel.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Channel not shutdown after 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
