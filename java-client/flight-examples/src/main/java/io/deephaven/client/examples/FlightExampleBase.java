package io.deephaven.client.examples;

import io.deephaven.client.DaggerSessionImplComponent;
import io.deephaven.client.impl.DaggerFlightComponent;
import io.deephaven.client.impl.FlightClientModule;
import io.deephaven.client.impl.FlightComponent;
import io.deephaven.client.impl.SessionAndFlight;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class FlightExampleBase implements Callable<Void> {

    @Option(names = {"-t", "--target"}, description = "The host target.",
        defaultValue = "localhost:10000")
    String target;

    protected abstract void execute(SessionAndFlight sessionAndFlight) throws Exception;

    @Override
    public final Void call() throws Exception {
        BufferAllocator bufferAllocator = new RootAllocator();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel =
            ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        // todo: fix dual-graph Dagger
        FlightComponent factory = DaggerFlightComponent.factory().create(
            new FlightClientModule(
                DaggerSessionImplComponent.factory().create(managedChannel, scheduler).session()),
            managedChannel, scheduler, bufferAllocator);
        SessionAndFlight sessionAndFlight = factory.sessionAndFlight();

        try {
            execute(sessionAndFlight);
        } finally {
            sessionAndFlight.session().closeFuture().get(5, TimeUnit.SECONDS);
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
