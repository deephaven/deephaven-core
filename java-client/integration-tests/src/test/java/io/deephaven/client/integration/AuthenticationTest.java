//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The authentication handlers the test server is started with: anonymous, and pre-shared key accepted or rejected.
 */
class AuthenticationTest {

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
    static void disconnect() {
        factory.managedChannel().shutdownNow();
        scheduler.shutdownNow();
    }

    @Test
    void anonymousIsAccepted() throws Exception {
        try (final Session session = factory.newSession()) {
            assertThat(session.getConfigurationConstants().get()).isNotEmpty();
        }
    }

    @Test
    void correctPreSharedKeyIsAccepted() throws Exception {
        try (final Session session = factory.newSession(TestServer.pskSessionConfig(TestServer.PSK))) {
            assertThat(session.getConfigurationConstants().get()).isNotEmpty();
        }
    }

    @Test
    void wrongPreSharedKeyIsRejected() {
        assertThatThrownBy(() -> factory.newSession(TestServer.pskSessionConfig("not-the-key")).close())
                .hasStackTraceContaining("UNAUTHENTICATED");
    }
}
