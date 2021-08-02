package io.deephaven.client.examples;

import io.deephaven.client.DaggerSessionImplComponent;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.qst.table.LabeledTable;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

@Command(name = "hammer-sessions", mixinStandardHelpOptions = true,
    description = "Create a lot of sessions as fast as possible", version = "0.1.0")
public class HammerSessions implements Callable<Void> {
    @Option(names = {"-t", "--target"}, description = "The host target.",
        defaultValue = "localhost:10000")
    String target;

    @Parameters(arity = "1", paramLabel = "OUTSTANDING",
        description = "Maximum number of outstanding sessions.")
    int outstanding;

    @Parameters(arity = "1", paramLabel = "TOTAL",
        description = "The total number of sessions to create.")
    int total;

    public static void main(String[] args) {
        int execute = new CommandLine(new HammerSessions()).execute(args);
        System.exit(execute);
    }

    @Override
    public Void call() throws Exception {

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel =
            ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        SessionFactory factory =
            DaggerSessionImplComponent.factory().create(managedChannel, scheduler);

        final LongAdder failed = new LongAdder();
        Semaphore semaphore = new Semaphore(outstanding, false);

        final long start = System.nanoTime();
        for (long i = 0; i < total; ++i) {
            if (i % (outstanding / 3) == 0) {
                System.out.printf("Started %d, outstanding %d%n", i, semaphore.availablePermits());
            }
            semaphore.acquire();
            factory.sessionFuture()
                .whenComplete((BiConsumer<Session, Throwable>) (session, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                        failed.add(1);
                    }
                    if (session != null) {
                        session.closeFuture().whenComplete((x, y) -> semaphore.release());
                    } else {
                        semaphore.release();
                    }
                });
        }

        semaphore.acquire(outstanding);
        final long end = System.nanoTime();

        long f = failed.longValue();
        System.out.printf("%d succeeded, %d failed, avg duration %s%n", total - f, f,
            Duration.ofNanos(end - start).dividedBy(total));

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
