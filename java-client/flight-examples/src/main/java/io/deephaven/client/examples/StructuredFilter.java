//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Builds a filter with the structured {@link Filter} API instead of a filter string, applies it to a table, and reads
 * the result back over Arrow Flight as TSV. Three shapes are available: an OR of comparisons, an AND of comparisons,
 * and a mix of comparisons with a raw filter string.
 */
@Command(name = "structured-filter", mixinStandardHelpOptions = true,
        description = "Filter a table with the structured Filter API and print it as TSV", version = "0.1.0")
class StructuredFilter implements Callable<Void> {

    enum Shape {
        OR, AND, MIXED
    }

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true)
    BatchOrSerialOptions mode;

    @Option(names = {"--filter"},
            description = "The filter to build, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "OR")
    Shape shape;

    /**
     * The same conditions a user might type as a filter string, built as objects so they can be composed and checked
     * before anything is sent to the server.
     */
    static Filter filter(Shape shape) {
        final ColumnName i = ColumnName.of("I");
        switch (shape) {
            case OR:
                // I < 42 || I == 93
                return Filter.or(
                        FilterComparison.lt(i, Literal.of(42L)),
                        FilterComparison.eq(i, Literal.of(93L)));
            case AND:
                // I >= 42 && I < 55
                return Filter.and(
                        FilterComparison.geq(i, Literal.of(42L)),
                        FilterComparison.lt(i, Literal.of(55L)));
            case MIXED:
                // I < 42 || I == 93 || I % 2 == 0, with the last term left as a raw string
                return Filter.or(
                        FilterComparison.lt(i, Literal.of(42L)),
                        FilterComparison.eq(i, Literal.of(93L)),
                        RawString.of("I % 2 == 0"));
            default:
                throw new IllegalStateException("Unexpected shape " + shape);
        }
    }

    @Override
    public Void call() throws Exception {
        // Arrow memory for the data read over Flight, and a scheduler for the client's background work such as
        // refreshing the session token
        final BufferAllocator allocator = new RootAllocator();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        // A FlightSession pairs a Session (tables, consoles, publishing) with an Arrow Flight client (bulk data)
        try (final FlightSession flight = factory.newFlightSession()) {
            // A TableSpec describes a table; nothing runs until the server executes it
            final TableSpec table = TableSpec.empty(100).view("I=i").where(filter(shape));
            // Batch sends the whole spec as one request; serial sends one operation per request
            final TableHandleManager manager = BatchOrSerialOptions.manager(mode, flight.session());
            // Executing the spec returns a TableHandle, a server-side export released when the handle is closed
            try (
                    final TableHandle handle = manager.execute(table);
                    final FlightStream stream = flight.stream(handle)) {
                System.out.println(stream.getSchema());
                while (stream.next()) {
                    System.out.println(stream.getRoot().contentToTSVString());
                }
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new StructuredFilter()).execute(args));
    }
}
