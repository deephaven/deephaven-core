//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.client.impl.BarrageSessionFactoryConfig.Factory;
import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientChannelFactoryDefaulter;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.ArgGroup;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class BarrageClientExampleBase implements Callable<Void> {

    private static final ClientChannelFactory CLIENT_CHANNEL_FACTORY = ClientChannelFactoryDefaulter.builder()
            .userAgent(BarrageSessionFactoryConfig.userAgent(Collections.singletonList("deephaven-barrage-examples")))
            .build();

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    protected abstract void execute(BarrageSession session) throws Exception;

    @Override
    public final Void call() throws Exception {
        final BufferAllocator bufferAllocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final Factory factory = BarrageSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .clientChannelFactory(CLIENT_CHANNEL_FACTORY)
                .allocator(bufferAllocator)
                .scheduler(scheduler)
                .build()
                .factory();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, factory.managedChannel())));

        // Note that a DEFAULT update graph is required for engine operation. Users may wish to create additional update
        // graphs for their own purposes, but the DEFAULT must be created first.
        final PeriodicUpdateGraph updateGraph =
                PeriodicUpdateGraph.newBuilder("DEFAULT").existingOrBuild();

        // Prepare this thread for client-side Deephaven engine. We don't intend to create any subtables, so we'll use
        // an empty query scope, an empty query library, a poisoned query compiler, and the update graph we just made.
        // Note that it's a good habit to mark the most basic execution context as systemic, so that it's not
        // accidentally used in contexts when the calling-code should be providing their own context.
        final ExecutionContext executionContext = ExecutionContext.newBuilder()
                .markSystemic()
                .emptyQueryScope()
                .newQueryLibrary()
                .setUpdateGraph(updateGraph)
                .build();
        try (
                final BarrageSession deephavenSession = factory.newBarrageSession(sessionConfig());
                final SafeCloseable ignored = executionContext.open()) {
            try {
                execute(deephavenSession);
            } finally {
                deephavenSession.session().closeFuture().get(5, TimeUnit.SECONDS);
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

    private static void onShutdown(final ScheduledExecutorService scheduler,
            final ManagedChannel managedChannel) {
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
