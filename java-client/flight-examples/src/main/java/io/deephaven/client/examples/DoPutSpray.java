//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.FlightSessionFactoryConfig.Factory;
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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.client.examples.FlightExampleBase.CLIENT_CHANNEL_FACTORY;

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

        final Factory sourceFactory = FlightSessionFactoryConfig.builder()
                .clientConfig(source.config())
                .clientChannelFactory(CLIENT_CHANNEL_FACTORY)
                .allocator(bufferAllocator)
                .scheduler(scheduler)
                .build()
                .factory();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> onShutdown(scheduler, sourceFactory.managedChannel())));

        try (final FlightSession sourceSession = sourceFactory.newFlightSession()) {
            try (final TableHandle sourceHandle =
                    sourceSession.session().execute(TicketTable.of(ticket.getBytes(StandardCharsets.UTF_8)))) {
                for (ConnectOptions other : connects.subList(1, connects.size())) {
                    final Factory otherFactory = FlightSessionFactoryConfig.builder()
                            .clientConfig(other.config())
                            .clientChannelFactory(CLIENT_CHANNEL_FACTORY)
                            .allocator(bufferAllocator)
                            .scheduler(scheduler)
                            .build()
                            .factory();
                    try (final FlightSession destSession = otherFactory.newFlightSession()) {
                        try (final FlightStream in = sourceSession.stream(sourceHandle)) {
                            final TableHandle destHandle = destSession.putExport(in);
                            destSession.session().publish(variableName, destHandle).get();
                        } finally {
                            closeFutureWait(destSession);
                        }
                    }
                    otherFactory.managedChannel().shutdown().awaitTermination(5, TimeUnit.SECONDS);
                }
            } finally {
                closeFutureWait(sourceSession);
            }
        }
        return null;
    }

    private static void closeFutureWait(FlightSession session)
            throws InterruptedException, ExecutionException, TimeoutException {
        session.session().closeFuture().get(5, TimeUnit.SECONDS);
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
