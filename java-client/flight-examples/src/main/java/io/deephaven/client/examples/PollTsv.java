package io.deephaven.client.examples;

import io.deephaven.client.DaggerSessionImplComponent;
import io.deephaven.client.impl.DaggerFlightComponent;
import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.Flight;
import io.deephaven.client.impl.FlightClientModule;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionAndFlight;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Command(name = "poll-tsv", mixinStandardHelpOptions = true,
    description = "Send a QST, poll the results, and convert to TSV", version = "0.1.0")
class PollTsv implements Callable<Void> {

    @Option(names = {"-t", "--target"}, description = "The host target.",
        defaultValue = "localhost:10000")
    String target;

    @Option(names = {"-i", "--interval"}, description = "The interval.", defaultValue = "PT1s")
    Duration interval;

    @Option(names = {"-c", "--count"}, description = "The number of polls.")
    Long count;

    @Parameters(arity = "1", paramLabel = "QST", description = "QST file to send and get.",
        converter = TableConverter.class)
    TableSpec table;

    @Override
    public Void call() throws Exception {
        BufferAllocator bufferAllocator = new RootAllocator();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ManagedChannel managedChannel =
            ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        Runtime.getRuntime()
            .addShutdownHook(new Thread(() -> onShutdown(scheduler, managedChannel)));

        // todo: fix dual-graph Dagger
        SessionAndFlight sessionAndFlight = DaggerFlightComponent.factory().create(
            new FlightClientModule(
                DaggerSessionImplComponent.factory().create(managedChannel, scheduler).session()),
            managedChannel, scheduler, bufferAllocator).sessionAndFlight();

        long times = count == null ? Long.MAX_VALUE : count;

        try (final Flight flight = sessionAndFlight.flight();
            final Export export = sessionAndFlight.session().export(table)) {
            for (long i = 0; i < times; ++i) {

                long start = System.nanoTime();
                try (final FlightStream stream = flight.get(export)) {
                    stream.next();
                    if (i == 0) {
                        System.out.println(stream.getSchema());
                        System.out.println();
                    }
                    VectorSchemaRoot root = stream.getRoot();
                    long end = System.nanoTime();
                    System.out.println(root.contentToTSVString());
                    System.out.printf("%s duration%n%n", Duration.ofNanos(end - start));

                    if (i + 1 < times) {
                        Thread.sleep(interval.toMillis());
                    }
                }
            }
        } finally {
            sessionAndFlight.session().closeFuture().get(5, TimeUnit.SECONDS);
        }

        scheduler.shutdownNow();
        managedChannel.shutdownNow();
        return null;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new PollTsv()).execute(args);
        System.exit(execute);
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
