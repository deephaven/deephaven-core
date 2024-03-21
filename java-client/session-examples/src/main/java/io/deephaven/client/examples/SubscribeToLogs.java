//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest.Builder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Set;

@Command(name = "subscribe-to-logs", mixinStandardHelpOptions = true,
        description = "Console#SubscribeToLogs", version = "0.1.0")
class SubscribeToLogs extends SessionExampleBase {

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

    @Override
    protected void execute(SessionFactory sessionFactory) throws Exception {
        try (final Session session = sessionFactory.newSession()) {
            final Builder builder = LogSubscriptionRequest.newBuilder();
            if (levels != null) {
                for (String level : levels) {
                    builder.addLevels(level);
                }
            }
            final Iterator<LogSubscriptionData> logs = session
                    .channel()
                    .consoleBlocking()
                    .subscribeToLogs(builder.build());
            final long count = this.count == null ? Long.MAX_VALUE : this.count;
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
        }
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
        int execute = new CommandLine(new SubscribeToLogs()).execute(args);
        System.exit(execute);
    }
}
