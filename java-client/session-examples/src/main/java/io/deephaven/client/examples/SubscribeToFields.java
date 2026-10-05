//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ApplicationService.Cancel;
import io.deephaven.client.impl.ApplicationService.Listener;
import io.deephaven.client.impl.FieldChanges;
import io.deephaven.client.impl.FieldInfo;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Subscribes to the server's exported fields and prints each change notification. The first notification lists the
 * fields that already exist.
 */
@Command(name = "subscribe-fields", mixinStandardHelpOptions = true,
        description = "Subscribe to fields", version = "0.1.0")
public final class SubscribeToFields implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-c", "--count"},
            description = "The number of field change notifications to receive before exiting, unlimited if unset")
    Long count;

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
            final CountDownLatch latch = new CountDownLatch(1);
            final long notificationsToReceive = count == null ? Long.MAX_VALUE : count;
            final AtomicLong notificationsReceived = new AtomicLong();
            // Fields are the named objects the server exposes: scope variables and application fields
            final Cancel cancel = session.subscribeToFields(new Listener() {
                @Override
                public void onNext(FieldChanges fields) {
                    final List<FieldInfo> created = fields.created();
                    final List<FieldInfo> updated = fields.updated();
                    final List<FieldInfo> removed = fields.removed();
                    System.out.println("Created: " + created.size());
                    System.out.println("Updated: " + updated.size());
                    System.out.println("Removed: " + removed.size());
                    for (FieldInfo fieldInfo : created) {
                        System.out.println("Created: " + fieldInfo);
                    }
                    for (FieldInfo fieldInfo : updated) {
                        System.out.println("Updated: " + fieldInfo);
                    }
                    for (FieldInfo fieldInfo : removed) {
                        System.out.println("Removed: " + fieldInfo);
                    }
                    if (notificationsReceived.incrementAndGet() >= notificationsToReceive) {
                        latch.countDown();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    if (!isCancelled(t)) {
                        t.printStackTrace(System.err);
                    }
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            });
            Runtime.getRuntime().addShutdownHook(new Thread(cancel::cancel));
            latch.await();
            cancel.cancel();
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private static boolean isCancelled(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            return ((StatusRuntimeException) t).getStatus().getCode() == Code.CANCELLED;
        } else if (t instanceof StatusException) {
            return ((StatusException) t).getStatus().getCode() == Code.CANCELLED;
        }
        return false;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new SubscribeToFields()).execute(args));
    }
}
