package io.deephaven.client.examples;

import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactory;
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

    @Option(names = {"-p", "--plaintext"}, description = "Use plaintext.")
    Boolean plaintext;

    @Option(names = {"-u", "--user-agent"}, description = "User-agent.")
    String userAgent;

    protected abstract void execute(FlightSession flight) throws Exception;

    @Override
    public final Void call() throws Exception {
        BufferAllocator bufferAllocator = new RootAllocator();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
        if ((plaintext != null && plaintext) || "localhost:10000".equals(target)) {
            channelBuilder.usePlaintext();
        } else {
            channelBuilder.useTransportSecurity();
        }
        if (userAgent != null) {
            channelBuilder.userAgent(userAgent);
        }
        ManagedChannel managedChannel = channelBuilder.build();

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
