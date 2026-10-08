//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Round-trips a table through the client: creates one on the server, reads it with DoGet, writes it back with DoPut,
 * and publishes the copy under a variable name. The three methods differ in how the DoPut destination is managed.
 */
@Command(name = "do-put-new", mixinStandardHelpOptions = true,
        description = "Do Put New", version = "0.1.0")
class DoPutNew implements Callable<Void> {

    enum Method {
        HANDLE, TICKET, DIRECT
    }

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-m", "--method"}, description = "The method to use. [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "HANDLE")
    Method method;

    @Option(names = {"-n", "--num-rows"}, description = "The number of rows to doPut",
            defaultValue = "1000")
    long numRows;

    @Parameters(arity = "1", paramLabel = "VAR", description = "Variable name to publish.")
    String variableName;

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
            switch (method) {
                case HANDLE:
                    handle(flight);
                    break;
                case TICKET:
                    ticket(flight);
                    break;
                case DIRECT:
                    direct(flight);
                    break;
                default:
                    throw new IllegalStateException("Unexpected method " + method);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private TableSpec table() {
        return TableSpec.empty(numRows).view("X=i");
    }

    private void publish(FlightSession flight, HasTicketId ticketId) throws InterruptedException, ExecutionException {
        flight.session().publish(variableName, ticketId).get();
    }

    private void handle(FlightSession flight) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip.
        // Executing the spec gives a TableHandle export; DoGet streams its rows; DoPut uploads that stream as a
        // new server table and returns a handle to it.
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle);
                final TableHandle destHandle = flight.putExport(doGet)) {
            publish(flight, destHandle);
        }
    }

    private void ticket(FlightSession flight) throws Exception {
        // This version is more efficient, but requires manual management of a ticket
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle)) {
            final ExportId exportId = flight.putExportManual(doGet);
            try {
                publish(flight, exportId);
            } finally {
                flight.release(exportId);
            }
        }
    }

    private void direct(FlightSession flight) throws Exception {
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle)) {
            flight.put(new ScopeId(variableName), doGet);
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DoPutNew()).execute(args));
    }
}
