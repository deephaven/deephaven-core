//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.DeephavenSessionRoot;
import io.deephaven.client.SessionSubcomponent.Builder;
import io.deephaven.client.impl.SessionFactory;
import io.grpc.ManagedChannel;
import picocli.CommandLine.ArgGroup;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class SessionExampleBase implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    protected abstract void execute(SessionFactory sessionFactory) throws Exception;

    @Override
    public final Void call() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        ManagedChannel managedChannel = ConnectOptions.open(connectOptions);

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        final Builder builder = DeephavenSessionRoot.of()
                .factoryBuilder()
                .managedChannel(managedChannel)
                .scheduler(scheduler);
        if (authenticationOptions != null) {
            authenticationOptions.ifPresent(builder::authenticationTypeAndValue);
        }
        SessionFactory factory = builder.build();
        execute(factory);

        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        return null;
    }

    protected void onShutdown(ScheduledExecutorService scheduler,
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
