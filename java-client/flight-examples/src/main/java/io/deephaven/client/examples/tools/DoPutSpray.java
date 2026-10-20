//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples.tools;

import io.deephaven.client.examples.ConnectOptions;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TicketTable;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Operations tool: copies a table from one server to one or more others. Reads it from the first connection with DoGet
 * and writes it to each of the rest with DoPut, publishing it under the same variable name on each.
 */
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
        final BufferAllocator allocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final FlightSessionFactoryConfig.Factory sourceFactory = factory(connects.get(0), allocator, scheduler);
        try (final FlightSession source = sourceFactory.newFlightSession()) {
            try (final TableHandle sourceHandle =
                    source.session().execute(TicketTable.of(ticket.getBytes(StandardCharsets.UTF_8)))) {
                for (ConnectOptions other : connects.subList(1, connects.size())) {
                    final FlightSessionFactoryConfig.Factory destFactory = factory(other, allocator, scheduler);
                    try (final FlightSession dest = destFactory.newFlightSession()) {
                        try (
                                final FlightStream in = source.stream(sourceHandle);
                                final TableHandle destHandle = dest.putExport(in)) {
                            dest.session().publish(variableName, destHandle).get();
                        } finally {
                            // Wait for the server to acknowledge the close before its channel goes away below
                            dest.session().closeFuture().get(5, TimeUnit.SECONDS);
                        }
                    } finally {
                        destFactory.managedChannel().shutdownNow();
                    }
                }
            } finally {
                // Wait for the server to acknowledge the close; close() only starts it, and the channel goes away below
                source.session().closeFuture().get(5, TimeUnit.SECONDS);
            }
        } finally {
            sourceFactory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private static FlightSessionFactoryConfig.Factory factory(ConnectOptions connect, BufferAllocator allocator,
            ScheduledExecutorService scheduler) {
        return FlightSessionFactoryConfig.builder()
                .clientConfig(connect.config())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DoPutSpray()).execute(args));
    }
}
