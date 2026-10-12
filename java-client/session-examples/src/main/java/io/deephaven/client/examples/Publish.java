//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Publishes the table behind one ticket under another, for example {@code --variable source --variable copy} to bind a
 * second scope variable to the same table.
 */
@Command(name = "publish", mixinStandardHelpOptions = true,
        description = "Publish", version = "0.1.0")
class Publish implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    // Note: this is not perfect, and will need to look into picocli usage to better support this in the future.
    // Right now, the two ticket types need to be the same, even though that's not a technical requirement.
    @ArgGroup(exclusive = false, multiplicity = "2")
    List<Ticket> tickets;

    @Override
    public Void call() throws Exception {
        // The source is given first on the command line; Session.publish takes the destination first
        final Ticket source = tickets.get(0);
        final Ticket destination = tickets.get(1);

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
            // Publishing binds the table behind one ticket to another name; both then refer to the same table
            session.publish(destination, source).get();
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Publish()).execute(args));
    }
}
