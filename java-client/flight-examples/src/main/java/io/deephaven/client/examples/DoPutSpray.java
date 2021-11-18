package io.deephaven.client.examples;

import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TicketTable;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Command(name = "do-put-spray", mixinStandardHelpOptions = true,
        description = "Do Put Spray", version = "0.1.0")
class DoPutSpray implements Callable<Void> {

    @ArgGroup(exclusive = false, multiplicity = "2..*")
    List<ConnectOptions> connects;

    @Parameters(arity = "1", paramLabel = "TICKET", description = "The ticket from the first connection.")
    String ticket;

    @Parameters(arity = "1", paramLabel = "VARIABLE", description = "The variable name to set.")
    String variableName;

    @Override
    public Void call() throws Exception {

        BufferAllocator bufferAllocator = new RootAllocator();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        ConnectOptions source = connects.get(0);

        final ManagedChannel sourceChannel = source.open();
        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, sourceChannel)));

        final FlightSession sourceSession = session(bufferAllocator, scheduler, sourceChannel);
        try (final TableHandle sourceHandle = sourceSession.session().execute(TicketTable.of(ticket))) {
            for (ConnectOptions other : connects.subList(1, connects.size())) {
                final FlightSession destSession = session(bufferAllocator, scheduler, other.open());
                try (final FlightStream in = sourceSession.stream(sourceHandle)) {
                    final TableHandle destHandle = destSession.putExport(in);
                    destSession.session().publish(variableName, destHandle).get();
                } finally {
                    close(destSession);
                }
            }
        } finally {
            close(sourceSession);
        }
        return null;
    }

    private static void close(FlightSession session) throws InterruptedException, ExecutionException, TimeoutException {
        session.close();
        session.session().closeFuture().get(5, TimeUnit.SECONDS);
    }

    private FlightSession session(BufferAllocator bufferAllocator, ScheduledExecutorService scheduler,
            ManagedChannel sourceChannel) {
        return DaggerDeephavenFlightRoot.create().factoryBuilder()
                .managedChannel(sourceChannel)
                .scheduler(scheduler)
                .allocator(bufferAllocator)
                .build()
                .newFlightSession();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoPutSpray()).execute(args);
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
