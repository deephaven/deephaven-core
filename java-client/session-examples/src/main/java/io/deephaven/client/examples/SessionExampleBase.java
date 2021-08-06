package io.deephaven.client.examples;

import io.deephaven.client.DaggerSessionImplComponent;
import io.deephaven.client.SessionImplComponent;
import io.deephaven.client.impl.SessionFactory;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class SessionExampleBase implements Callable<Void> {
    @Option(names = {"-t", "--target"}, description = "The host target.",
        defaultValue = "localhost:10000")
    String target;

    protected abstract void execute(SessionFactory sessionFactory) throws Exception;

    @Override
    public final Void call() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel =
            ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        SessionImplComponent factory =
            DaggerSessionImplComponent.factory().create(managedChannel, scheduler);

        execute(factory);

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
