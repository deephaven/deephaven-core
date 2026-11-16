//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.TypedTicket;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Fetches a plugin object by type and ticket. The object's metadata goes to standard error and its bytes to standard
 * output or a file; with {@code --recursive}, the objects it exports are fetched the same way.
 */
@Command(name = "fetch-object", mixinStandardHelpOptions = true,
        description = "Fetch object", version = "0.1.0")
class FetchObject implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"--type"}, required = true, description = "The ticket type.")
    String type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Option(names = {"-f", "--file"}, description = "The output file, otherwise goes to STDOUT.")
    Path file;

    @Option(names = {"-r", "--recursive"}, description = "If the program should recursively fetch.")
    boolean recursive;

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
        // A typed ticket names a server object by plugin type and location; fetching it returns its bytes and
        // the objects it exports
        try (
                final Session session = factory.newSession();
                final Fetchable fetchable = session.fetchable(new TypedTicket(type, ticket)).get();
                final ServerData dataAndExports = fetchable.fetch().get()) {
            show(0, type, dataAndExports);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private void show(int depth, String type, ServerData dataAndExports)
            throws IOException, ExecutionException, InterruptedException {
        final String prefix = " ".repeat(depth);
        System.err.println(prefix + "type: " + type);
        System.err.println(prefix + "size: " + dataAndExports.data().remaining());
        for (ServerObject export : dataAndExports.exports()) {
            System.err.println(prefix + "exportId: " + export);
        }
        final byte[] data = new byte[dataAndExports.data().remaining()];
        dataAndExports.data().slice().get(data);
        if (file != null) {
            Files.write(file, data);
        } else {
            System.out.write(data);
        }
        if (recursive) {
            for (ServerObject serverObject : dataAndExports.exports()) {
                show(depth + 1, serverObject);
            }
        }
    }

    private void show(int depth, ServerObject obj)
            throws IOException, ExecutionException, InterruptedException {
        if (obj instanceof Fetchable) {
            final Fetchable fetchable = (Fetchable) obj;
            try (final ServerData fetched = fetchable.fetch().get()) {
                show(depth, fetchable.type(), fetched);
            }
        } else {
            final String prefix = " ".repeat(depth);
            System.err.println(prefix + obj + " is not fetchable");
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new FetchObject()).execute(args));
    }
}
