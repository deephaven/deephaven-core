//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ApplicationService.Cancel;
import io.deephaven.client.impl.ApplicationService.Listener;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.FieldChanges;
import io.deephaven.client.impl.FieldInfo;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.SharedId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The session layer against a real server: configuration, the console, executing and publishing tables, resolving them
 * from another session, and the field and log subscriptions.
 */
class SessionApiTest {

    private static ScheduledExecutorService scheduler;
    private static SessionFactoryConfig.Factory factory;

    @BeforeAll
    static void connect() {
        scheduler = Executors.newScheduledThreadPool(4);
        factory = SessionFactoryConfig.builder()
                .clientConfig(TestServer.clientConfig())
                .scheduler(scheduler)
                .build()
                .factory();
    }

    @AfterAll
    static void disconnect() throws InterruptedException {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
        NettyLeakRecorder.assertNoLeaks();
    }

    @Test
    void configurationConstantsArePublished() throws Exception {
        try (final Session session = factory.newSession()) {
            final Map<String, ConfigValue> constants = session.getConfigurationConstants().get(10, TimeUnit.SECONDS);
            assertThat(constants).isNotEmpty();
            assertThat(constants.keySet()).allSatisfy(key -> assertThat(key).isNotBlank());
        }
    }

    @Test
    void consoleReportsCreatedTables() throws Exception {
        try (
                final Session session = factory.newSession();
                final ConsoleSession console = session.console("python").get(10, TimeUnit.SECONDS)) {
            final Changes changes = console.executeCode(
                    "from deephaven import empty_table\napi_console_table = empty_table(3)");
            assertThat(changes.errorMessage()).isEmpty();
            assertThat(changes.changes().created())
                    .extracting(FieldInfo::name)
                    .contains("api_console_table");
            assertThat(changes.changes().created())
                    .filteredOn(f -> f.name().equals("api_console_table"))
                    .extracting(f -> f.type().orElse("?"))
                    .containsExactly("Table");
        }
    }

    @Test
    void consoleReportsErrors() throws Exception {
        try (
                final Session session = factory.newSession();
                final ConsoleSession console = session.console("python").get(10, TimeUnit.SECONDS)) {
            final Changes changes = console.executeCode("def (");
            assertThat(changes.errorMessage()).isPresent();
            assertThat(changes.errorMessage().get()).contains("SyntaxError");
        }
    }

    @Test
    void executedTableHasTheExpectedSize() throws Exception {
        try (
                final Session session = factory.newSession();
                final TableHandle handle = session.execute(TableSpec.empty(10).view("I=ii").where("I > 6"))) {
            assertThat(handle.response().getSize()).isEqualTo(3);
            assertThat(handle.response().getIsStatic()).isTrue();
        }
    }

    @Test
    void publishedTableResolvesFromAnotherSession() throws Exception {
        try (
                final Session publisher = factory.newSession();
                final TableHandle handle = publisher.execute(TableSpec.empty(5).view("I=ii"))) {
            publisher.publish("api_published", handle).get(10, TimeUnit.SECONDS);
            try (
                    final Session reader = factory.newSession();
                    final TableHandle resolved = reader.execute(TicketTable.fromQueryScopeField("api_published"))) {
                assertThat(resolved.response().getSize()).isEqualTo(5);
            }
        }
    }

    @Test
    void sharedIdResolvesFromAnotherSessionWhileThePublisherIsOpen() throws Exception {
        final SharedId sharedId = SharedId.newRandom();
        try (
                final Session publisher = factory.newSession();
                final TableHandle handle = publisher.execute(TableSpec.empty(7).view("I=ii"))) {
            publisher.publish(sharedId, handle).get(10, TimeUnit.SECONDS);
            try (
                    final Session reader = factory.newSession();
                    final TableHandle resolved = reader.execute(sharedId.ticketId().table())) {
                assertThat(resolved.response().getSize()).isEqualTo(7);
            }
        }
    }

    @Test
    void fieldSubscriptionListsPublishedTables() throws Exception {
        try (
                final Session session = factory.newSession();
                final TableHandle handle = session.execute(TableSpec.empty(1))) {
            session.publish("api_field", handle).get(10, TimeUnit.SECONDS);

            final CountDownLatch first = new CountDownLatch(1);
            final AtomicReference<FieldChanges> initial = new AtomicReference<>();
            final Cancel cancel = session.subscribeToFields(new Listener() {
                @Override
                public void onNext(FieldChanges fields) {
                    if (initial.compareAndSet(null, fields)) {
                        first.countDown();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    first.countDown();
                }

                @Override
                public void onCompleted() {
                    first.countDown();
                }
            });
            try {
                assertThat(first.await(10, TimeUnit.SECONDS)).as("first field notification").isTrue();
                assertThat(initial.get()).isNotNull();
                // The first notification carries every field that already exists
                assertThat(initial.get().created()).extracting(FieldInfo::name).contains("api_field");
            } finally {
                cancel.cancel();
            }
        }
    }

    @Test
    void logSubscriptionReceivesConsoleOutput() throws Exception {
        final String marker = "api-log-marker-" + System.nanoTime();
        try (
                final Session session = factory.newSession();
                final ConsoleSession console = session.console("python").get(10, TimeUnit.SECONDS)) {
            // The subscription replays recent history first, then streams; a deadline bounds the wait
            final ConsoleServiceBlockingStub stub = session.channel().consoleBlocking()
                    .withDeadlineAfter(30, TimeUnit.SECONDS);
            final Iterator<LogSubscriptionData> logs =
                    stub.subscribeToLogs(LogSubscriptionRequest.newBuilder().build());

            console.executeCode("print('" + marker + "')");

            boolean seen = false;
            while (!seen && logs.hasNext()) {
                seen = logs.next().getMessage().contains(marker);
            }
            assertThat(seen).as("log message containing " + marker).isTrue();
        }
    }
}
