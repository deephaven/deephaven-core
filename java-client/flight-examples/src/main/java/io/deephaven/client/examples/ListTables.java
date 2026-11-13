//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Lists the tables the server exposes as Arrow Flights, optionally with their schemas.
 */
@Command(name = "list-tables", mixinStandardHelpOptions = true, description = "List the flights",
        version = "0.1.0")
class ListTables implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-s", "--schema"}, description = "Whether to include schema",
            defaultValue = "false")
    boolean showSchema;

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
                // Each scope variable and application field holding a table is listed as a Flight
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
        System.exit(new CommandLine(new ListTables()).execute(args));
    }
}
