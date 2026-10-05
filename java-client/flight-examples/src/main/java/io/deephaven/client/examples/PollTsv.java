//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Creates a ticking time table and repeatedly reads its current contents over Arrow Flight, printing each snapshot as
 * TSV. Each poll is a fresh DoGet; for a push-based view of changes see the barrage examples.
 */
@Command(name = "poll-tsv", mixinStandardHelpOptions = true,
        description = "Send a QST, poll the results, and convert to TSV", version = "0.1.0")
class PollTsv implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @Option(names = {"-i", "--interval"}, description = "The interval.", defaultValue = "PT1s")
    Duration interval;

    @Option(names = {"-c", "--count"}, description = "The number of polls.")
    Long count;

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
            // A TableSpec describes a table; a time table adds a row every period once the server executes it
            final TableSpec table = TimeTable.of(Duration.ofSeconds(1));
            // Batch sends a whole query as one request; serial sends one operation per request
            final TableHandleManager manager = BatchOrSerialOptions.manager(mode, flight.session());
            final long times = count == null ? Long.MAX_VALUE : count;
            // Executing the spec returns a TableHandle, a server-side export released when the handle is closed
            try (final TableHandle handle = manager.execute(table)) {
                for (long i = 0; i < times; ++i) {
                    final long start = System.nanoTime();
                    try (final FlightStream stream = flight.stream(handle)) {
                        if (i == 0) {
                            System.out.println(stream.getSchema());
                            System.out.println();
                        }
                        while (stream.next()) {
                            System.out.println(stream.getRoot().contentToTSVString());
                        }
                    }
                    System.out.printf("%s duration%n%n", Duration.ofNanos(System.nanoTime() - start));
                    if (i + 1 < times) {
                        Thread.sleep(interval.toMillis());
                    }
                }
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new PollTsv()).execute(args));
    }
}
