//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TimeTable;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Creates a time table and publishes it under a shared id that other clients can fetch while this one stays connected.
 * Prints the id, then holds the session open for {@code --duration}, or until Ctrl-C.
 */
@Command(name = "create-shared-id", mixinStandardHelpOptions = true,
        description = "Exports a time table to a random shared id", version = "0.1.0")
class CreateSharedId implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = false)
    SharedField destination;

    @Option(names = {"--duration"},
            description = "How long to keep the shared id published before exiting, for example PT10S; "
                    + "unlimited if unset")
    Duration duration;

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
            final SharedId sharedId = destination != null ? destination.sharedId() : SharedId.newRandom();
            // A TableSpec describes a table; executing it on the server returns a TableHandle export
            final TableHandle timeTable = session.execute(TimeTable.of(Duration.ofSeconds(1)));
            // A shared id is a name other sessions can resolve while this session keeps the export alive
            session.publish(sharedId, timeTable).get();

            System.out.println("shared id: " + sharedId.asHexString());
            System.out.println();

            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
            if (duration == null) {
                System.out.println("ctrl-C to kill");
                latch.await();
            } else {
                System.out.println("holding for " + duration);
                latch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new CreateSharedId()).execute(args));
    }
}
