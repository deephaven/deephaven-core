package io.deephaven.client.examples;

import io.deephaven.client.DaggerSessionImplComponent;
import io.deephaven.client.impl.DaggerFlightComponent;
import io.deephaven.client.impl.Flight;
import io.deephaven.client.impl.FlightClientModule;
import io.deephaven.client.impl.SessionAndFlight;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Command(name = "list-tables", mixinStandardHelpOptions = true, description = "List the flights",
    version = "0.1.0")
class ListTables implements Callable<Void> {

    @Option(names = {"-s", "--schema"}, description = "Whether to include schema",
        defaultValue = "false")
    boolean showSchema;

    @Option(names = {"-t", "--target"}, description = "The host target.",
        defaultValue = "localhost:10000")
    String target;

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

        try (final Flight flight = sessionAndFlight.flight()) {
            for (FlightInfo flightInfo : flight.list()) {
                if (showSchema) {
                    StringBuilder sb = new StringBuilder(flightInfo.getDescriptor().toString())
                        .append(System.lineSeparator());
                    for (Field field : flightInfo.getSchema().getFields()) {
                        sb.append('\t').append(field).append(System.lineSeparator());
                    }
                    System.out.println(sb);
                } else {
                    System.out.printf("%s%n", flightInfo.getDescriptor());
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
        int execute = new CommandLine(new ListTables()).execute(args);
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
