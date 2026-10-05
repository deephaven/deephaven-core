//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest.Builder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Streams the server's log messages, starting with its recent history, using the raw gRPC console service rather than
 * the session wrapper.
 */
@Command(name = "subscribe-to-logs", mixinStandardHelpOptions = true,
        description = "Console#SubscribeToLogs", version = "0.1.0")
class SubscribeToLogs implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @Option(names = {"-c", "--count"},
            description = "The number of messages to consume before exiting, unlimited if unset")
    Long count;

    @Option(names = {"-b", "--batch"}, description = "The number of messages to read before sleeping, defaults to 100",
            defaultValue = "100")
    int batch;

    @Option(names = {"-s", "--sleep"}, description = "The duration to sleep every batch size, defaults to pt0s",
            defaultValue = "pt0s")
    Duration sleepDuration;

    @Option(names = {"-q", "--quiet"}, description = "If the log output should be silenced, defaults to false",
            defaultValue = "false")
    boolean quiet;

    @Option(names = {"-l", "--level"},
            description = "Limits the messages to the specified levels, defaults to all levels")
    Set<String> levels;

    @Option(names = {"--timeout"},
            description = "Exit after this duration even if fewer than --count messages arrived, for example PT5S; "
                    + "unlimited if unset")
    Duration timeout;

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
            final Builder builder = LogSubscriptionRequest.newBuilder();
            if (levels != null) {
                for (String level : levels) {
                    builder.addLevels(level);
                }
            }
            // session.channel() exposes the raw gRPC stubs, for calls the Session wrapper does not cover
            ConsoleServiceBlockingStub console = session.channel().consoleBlocking();
            if (timeout != null) {
                console = console.withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            final Iterator<LogSubscriptionData> logs = console.subscribeToLogs(builder.build());
            final long count = this.count == null ? Long.MAX_VALUE : this.count;
            try {
                for (int i = 0; i < count && logs.hasNext();) {
                    for (int j = 0; i < count && j < batch && logs.hasNext(); ++j, ++i) {
                        final LogSubscriptionData record = logs.next();
                        if (!quiet) {
                            System.out.println(format(record));
                        }
                    }
                    // this is useful for simulating different types of client behavior
                    Thread.sleep(sleepDuration.toMillis());
                }
            } catch (StatusRuntimeException e) {
                if (timeout == null || e.getStatus().getCode() != Code.DEADLINE_EXCEEDED) {
                    throw e;
                }
                // The --timeout deadline passed; that is a normal exit.
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    private static String format(LogSubscriptionData record) {
        final Instant timestamp = Instant.ofEpochMilli(record.getMicros() / 1_000);
        String message = record.getMessage();
        if (message.endsWith("\n")) {
            message = message.substring(0, message.length() - 1);
        }
        return String.format("[%s][%s] %s", timestamp, record.getLogLevel(), message);
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new SubscribeToLogs()).execute(args));
    }
}
