//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientChannelFactoryDefaulter;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.client.impl.SessionConfig.Builder;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.SessionFactoryConfig.Factory;
import io.grpc.ManagedChannel;
import picocli.CommandLine.ArgGroup;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class SessionExampleBase implements Callable<Void> {

    private static final ClientChannelFactory CLIENT_CHANNEL_FACTORY = ClientChannelFactoryDefaulter.builder()
            .userAgent(SessionFactoryConfig.userAgent(Collections.singletonList("deephaven-session-examples")))
            .build();

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    protected abstract void execute(SessionFactory sessionFactory) throws Exception;

    @Override
    public final Void call() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final Factory factory = SessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .clientChannelFactory(CLIENT_CHANNEL_FACTORY)
                .scheduler(scheduler)
                .sessionConfig(sessionConfig())
                .build()
                .factory();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, factory.managedChannel())));

        execute(factory);

        scheduler.shutdownNow();
        factory.managedChannel().shutdownNow();
        return null;
    }

    private SessionConfig sessionConfig() {
        final Builder builder = SessionConfig.builder();
        if (authenticationOptions != null) {
            authenticationOptions.ifPresent(builder::authenticationTypeAndValue);
        }
        return builder.build();
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
