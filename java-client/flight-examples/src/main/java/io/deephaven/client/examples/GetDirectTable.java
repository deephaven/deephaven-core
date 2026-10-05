//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Reads a server table by ticket over Arrow Flight, printing its schema and the number of rows in each batch.
 */
@Command(name = "get-table", mixinStandardHelpOptions = true, description = "Get a table", version = "0.1.0")
class GetDirectTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read and written over Flight
        final BufferAllocator allocator = new RootAllocator();
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data).
        // DoGet by ticket streams the current rows of a table the server already holds, as Arrow record batches.
        try (
                final FlightSession flight = factory.newFlightSession();
                final FlightStream stream = flight.stream(ticket)) {
            System.out.println(stream.getSchema());
            long tableRows = 0L;
            while (stream.next()) {
                int batchRows = stream.getRoot().getRowCount();
                System.out.println("    batch received: " + batchRows + " rows");
                tableRows += batchRows;
            }
            System.out.println("Table received: " + tableRows + " rows");
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new GetDirectTable()).execute(args));
    }
}
