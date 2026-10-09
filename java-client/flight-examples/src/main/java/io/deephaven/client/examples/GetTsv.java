//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends a query, reads the result back over Arrow Flight, and prints it as TSV.
 */
@Command(name = "get-tsv", mixinStandardHelpOptions = true,
        description = "Send a QST, get the results, and convert to a TSV", version = "0.1.0")
class GetTsv implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

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
            try {
                // A TableSpec describes a table; nothing runs until the server executes it
                final TableSpec table = EmptyTable.of(42).view("I=ii");
                // Batch sends a whole query as one request; serial sends one operation per request
                final TableHandleManager manager = BatchOrSerialOptions.manager(mode, flight.session());
                final long start = System.nanoTime();
                // Executing the spec returns a TableHandle, a server-side export released when the handle is closed.
                // DoGet on the handle streams the table's rows back as Arrow record batches.
                try (
                        final TableHandle handle = manager.execute(table);
                        final FlightStream stream = flight.stream(handle)) {
                    System.out.println(stream.getSchema());
                    while (stream.next()) {
                        System.out.println(stream.getRoot().contentToTSVString());
                    }
                }
                System.out.printf("%s duration%n", Duration.ofNanos(System.nanoTime() - start));
            } finally {
                // Wait for the server to acknowledge the close; close() only starts it, and the channel goes away below
                flight.session().closeFuture().get(5, TimeUnit.SECONDS);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new GetTsv()).execute(args));
    }
}
