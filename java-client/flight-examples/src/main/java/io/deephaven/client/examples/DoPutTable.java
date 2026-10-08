//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Builds a small table client-side with one column of every primitive type, uploads it with DoPut, and publishes it
 * under the given ticket. The three methods differ in how the DoPut destination is managed.
 */
@Command(name = "do-put-table", mixinStandardHelpOptions = true,
        description = "Do Put Table", version = "0.1.0")
class DoPutTable implements Callable<Void> {

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
            switch (method) {
                case HANDLE:
                    handle(flight, allocator);
                    break;
                case TICKET:
                    ticket(flight, allocator);
                    break;
                case DIRECT:
                    direct(flight, allocator);
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

    /**
     * A NewTable is data built on the client, column headers plus rows, ready to upload with DoPut.
     */
    private static NewTable newTable() {
        return ColumnHeader.of(
                ColumnHeader.ofBoolean("Boolean"),
                ColumnHeader.ofByte("Byte"),
                ColumnHeader.ofChar("Char"),
                ColumnHeader.ofShort("Short"),
                ColumnHeader.ofInt("Int"),
                ColumnHeader.ofLong("Long"),
                ColumnHeader.ofFloat("Float"),
                ColumnHeader.ofDouble("Double"),
                ColumnHeader.ofString("String"),
                ColumnHeader.ofInstant("Instant"))
                .start(3)
                .row(true, (byte) 42, 'a', (short) 32_000, 1234567, 1234567890123L, 3.14f, 3.14d, "Hello, World",
                        Instant.now())
                .row(null, null, null, null, null, null, null, null, null, (Instant) null)
                .row(false, (byte) -42, 'b', (short) -32_000, -1234567, -1234567890123L, -3.14f, -3.14d, "Goodbye.",
                        Instant.ofEpochMilli(0))
                .newTable();
    }

    private void publish(FlightSession flight, HasTicketId ticketId) throws InterruptedException, ExecutionException {
        flight.session().publish(ticket, ticketId).get();
    }

    private void handle(FlightSession flight, BufferAllocator allocator) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip
        try (final TableHandle destHandle = flight.putExport(newTable(), allocator)) {
            publish(flight, destHandle);
        }
    }

    private void ticket(FlightSession flight, BufferAllocator allocator) throws Exception {
        // This version is more efficient, but requires manual management of an export ticket
        final ExportId exportId = flight.putExportManual(newTable(), allocator);
        try {
            publish(flight, exportId);
        } finally {
            flight.release(exportId);
        }
    }

    private void direct(FlightSession flight, BufferAllocator allocator) {
        // This version is most efficient, but the RHS is ephemeral and can't be re-referenced
        flight.put(ticket.asHasPathId(), newTable(), allocator);
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new DoPutTable()).execute(args));
    }
}
