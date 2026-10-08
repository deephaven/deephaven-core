//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TypedTicket;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Prints a server object as TSV. A {@code Table} is read directly; any other object type is fetched through the object
 * service and must export exactly one table, which is then read.
 */
@Command(name = "convert-to-table", mixinStandardHelpOptions = true, description = "Convert to table",
        version = "0.1.0")
class ConvertToTable implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"--type"}, required = true, description = "The ticket type.")
    String type;

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
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data)
        try (final FlightSession flight = factory.newFlightSession()) {
            // A table can be read directly; any other plugin object is fetched first to find the table it exports
            if ("Table".equals(type)) {
                showTable(flight, ticket);
            } else {
                try (final TableObject tableExport = fetchTableExport(flight)) {
                    showTable(flight, tableExport);
                }
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private static void showTable(FlightSession flight, HasTicketId ticket) throws Exception {
        try (final FlightStream stream = flight.stream(ticket)) {
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    private TableObject fetchTableExport(FlightSession flight) throws InterruptedException, ExecutionException {
        final ServerData fetchedObject = flight.session().fetch(new TypedTicket(type, ticket)).get();
        if (fetchedObject.exports().size() != 1) {
            throw new IllegalStateException("Expected fetched object to have exactly one export");
        }
        final ServerObject serverObject = fetchedObject.exports().get(0);
        if (!(serverObject instanceof TableObject)) {
            throw new IllegalStateException("Expected fetched object to export a Table");
        }
        if (fetchedObject.data().remaining() != 0) {
            throw new IllegalStateException("Expected fetched object to not have any bytes");
        }
        return (TableObject) serverObject;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ConvertToTable()).execute(args));
    }
}
