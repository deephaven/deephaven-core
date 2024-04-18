//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactory;
import io.deephaven.client.impl.BarrageSubcomponent.Builder;
import io.deephaven.client.impl.DeephavenBarrageRoot;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.ArgGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class BarrageClientExampleBase implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    protected abstract void execute(BarrageSession session) throws Exception;

    @Override
    public final Void call() throws Exception {
        final BufferAllocator bufferAllocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel = ConnectOptions.open(connectOptions);

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

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

        final Builder builder = DeephavenBarrageRoot.of()
                .factoryBuilder()
                .managedChannel(managedChannel)
                .scheduler(scheduler)
                .allocator(bufferAllocator);
        if (authenticationOptions != null) {
            authenticationOptions.ifPresent(builder::authenticationTypeAndValue);
        }
        final BarrageSessionFactory barrageFactory = builder.build();
        final BarrageSession deephavenSession = barrageFactory.newBarrageSession();
        try {
            try (final SafeCloseable ignored = executionContext.open()) {
                execute(deephavenSession);
            } finally {
                deephavenSession.close();
            }
        } finally {
            deephavenSession.session().closeFuture().get(5, TimeUnit.SECONDS);
        }

        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        return null;
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
