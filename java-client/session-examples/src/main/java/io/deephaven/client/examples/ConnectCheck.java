//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Opens a session and prints it, which is enough to check that a server is reachable and accepts the given
 * authentication.
 */
@Command(name = "connect-check", mixinStandardHelpOptions = true,
        description = "Connect check", version = "0.1.0")
class ConnectCheck implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

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
        // Closing the session releases everything it exported on the server
        try (final Session session = factory.newSession()) {
            System.out.println("Connected to session: " + session);
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ConnectCheck()).execute(args));
    }
}
