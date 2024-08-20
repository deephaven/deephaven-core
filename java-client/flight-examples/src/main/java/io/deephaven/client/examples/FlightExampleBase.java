//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientChannelFactoryDefaulter;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.FlightSessionFactoryConfig.Factory;
import io.deephaven.client.impl.SessionConfig;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.ArgGroup;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class FlightExampleBase implements Callable<Void> {

    static final ClientChannelFactory CLIENT_CHANNEL_FACTORY = ClientChannelFactoryDefaulter.builder()
            .userAgent(FlightSessionFactoryConfig.userAgent(Collections.singletonList("deephaven-flight-examples")))
            .build();

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    BufferAllocator bufferAllocator = new RootAllocator();

    protected abstract void execute(FlightSession flight) throws Exception;

    @Override
    public final Void call() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .clientChannelFactory(CLIENT_CHANNEL_FACTORY)
                .allocator(bufferAllocator)
                .scheduler(scheduler)
                .build()
                .factory();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, factory.managedChannel())));

        try (final FlightSession flightSession = factory.newFlightSession(sessionConfig())) {
            try {
                execute(flightSession);
            } finally {
                flightSession.session().closeFuture().get(5, TimeUnit.SECONDS);
            }
        }
        scheduler.shutdownNow();
        factory.managedChannel().shutdownNow();
        return null;
    }

    private SessionConfig sessionConfig() {
        final SessionConfig.Builder builder = SessionConfig.builder();
        if (authenticationOptions != null) {
            authenticationOptions.ifPresent(builder::authenticationTypeAndValue);
        }
        return builder.build();
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
