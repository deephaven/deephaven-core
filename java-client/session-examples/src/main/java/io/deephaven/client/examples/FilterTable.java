//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.api.filter.Filter;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Filters a server table with raw filter strings and publishes the result as {@code filter_table_results}.
 */
@Command(name = "filter-table", mixinStandardHelpOptions = true,
        description = "Filter table using raw strings", version = "0.1.0")
class FilterTable implements Callable<Void> {

    enum Type {
        AND, OR
    }

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"--filter-type"},
            description = "The filter type, default: ${DEFAULT-VALUE}, candidates: [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "AND")
    Type type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Parameters(arity = "1+", paramLabel = "FILTER", description = "Raw-string filters")
    List<String> filters;

    @Override
    public Void call() throws Exception {
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final SessionFactoryConfig.Factory factory = SessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .scheduler(scheduler)
                .build()
                .factory();
        try (final Session session = factory.newSession()) {
            final Filter filter = type == Type.AND ? Filter.and(Filter.from(filters)) : Filter.or(Filter.from(filters));
            // A ticket names a table the server already holds; table() wraps it as a TableSpec to build on
            final TableSpec filtered = ticket.ticketId().table().where(filter);
            // Executing the spec returns a TableHandle, a server-side export released when the handle is closed
            try (final TableHandle handle = session.executeAsync(filtered).getOrCancel()) {
                // Publishing binds the export to a scope variable, so it outlives this session
                session.publish("filter_table_results", handle).get();
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new FilterTable()).execute(args));
    }
}
