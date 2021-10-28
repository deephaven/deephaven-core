/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.client.impl.BarrageSession;
import io.deephaven.client.impl.BarrageSessionFactory;
import io.deephaven.client.impl.DaggerDeephavenBarrageRoot;
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

    protected abstract void execute(BarrageSession session) throws Exception;

    @Override
    public final Void call() throws Exception {
        final BufferAllocator bufferAllocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel = ConnectOptions.open(connectOptions);

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        final BarrageSessionFactory barrageFactory =
                DaggerDeephavenBarrageRoot.create().factoryBuilder()
                        .managedChannel(managedChannel)
                        .scheduler(scheduler)
                        .allocator(bufferAllocator)
                        .build();

        final BarrageSession deephavenSession = barrageFactory.newBarrageSession();

        try {
            try {
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
