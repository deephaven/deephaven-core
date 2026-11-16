//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Fetches the Arrow schema of a server table by its flight path, without reading any data.
 */
@Command(name = "get-schema", mixinStandardHelpOptions = true, description = "Get a schema", version = "0.1.0")
class GetDirectSchema implements Callable<Void> {

    enum Format {
        DEFAULT, JSON
    }

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-f", "--format"},
            description = "The output format, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "DEFAULT")
    Format format;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Path path;

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
                // A path names a table by scope variable or application field, the same way Flight lists it
                final Schema schema = flight.schema(path);
                switch (format) {
                    case DEFAULT:
                        System.out.println(schema);
                        break;
                    case JSON:
                        System.out.println(schema.toJson());
                        break;
                    default:
                        throw new IllegalStateException("Unexpected format " + format);
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
        System.exit(new CommandLine(new GetDirectSchema()).execute(args));
    }
}
