/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.client.impl.DaggerDeephavenClientRoot;
import io.deephaven.client.impl.DeephavenClientSession;
import io.deephaven.client.impl.DeephavenClientSessionFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class DeephavenClientExampleBase implements Callable<Void> {

    @Option(names = {"-t", "--target"}, description = "The host target.",
            defaultValue = "localhost:10000")
    String target;

    @Option(names = {"-p", "--plaintext"}, description = "Use plaintext.")
    Boolean plaintext;

    @Option(names = {"-u", "--user-agent"}, description = "User-agent.")
    String userAgent;

    protected abstract void execute(DeephavenClientSession session) throws Exception;

    @Override
    public final Void call() throws Exception {
        final BufferAllocator bufferAllocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(target);
        if ((plaintext != null && plaintext) || "localhost:10000".equals(target)) {
            channelBuilder.usePlaintext();
        } else {
            channelBuilder.useTransportSecurity();
        }
        if (userAgent != null) {
            channelBuilder.userAgent(userAgent);
        }
        final ManagedChannel managedChannel = channelBuilder.build();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        final DeephavenClientSessionFactory deephavenClientSessionFactory =
                DaggerDeephavenClientRoot.create().factoryBuilder()
                        .managedChannel(managedChannel)
                        .scheduler(scheduler)
                        .allocator(bufferAllocator)
                        .build();

        final DeephavenClientSession deephavenSession = deephavenClientSessionFactory.newDeephavenClientSession();

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
