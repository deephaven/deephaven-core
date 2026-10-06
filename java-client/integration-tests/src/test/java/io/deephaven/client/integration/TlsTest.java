//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.ssl.config.IdentityPrivateKey;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustCertificates;
import io.deephaven.uri.DeephavenTarget;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TLS against a second server started with the development certificates under {@code server/dev-certs}, with client
 * certificates wanted but not required. Covers the handshake and ALPN through whichever SSL provider the client
 * configures (see {@code TransportTest} for which one that is), trust configuration, client identity, and the failure
 * modes a misconfigured client hits. Runs under the {@code testTls} task, which passes {@code dh.tls.port} and
 * {@code dh.devCerts}.
 */
class TlsTest {

    private static Path devCerts;
    private static ScheduledExecutorService scheduler;
    private static BufferAllocator allocator;

    @BeforeAll
    static void setUp() {
        devCerts = Paths.get(ExampleRunner.requireProperty("dh.devCerts"));
        scheduler = Executors.newScheduledThreadPool(4);
        allocator = new RootAllocator();
    }

    @AfterAll
    static void tearDown() {
        scheduler.shutdownNow();
        allocator.close();
    }

    private static DeephavenTarget target(boolean secure) {
        return DeephavenTarget.builder()
                .host("localhost")
                .port(Integer.parseInt(ExampleRunner.requireProperty("dh.tls.port")))
                .isSecure(secure)
                .build();
    }

    /** Trust the development CA that signed the server's certificate. */
    private static SSLConfig trustDevCa() {
        return SSLConfig.builder()
                .trust(TrustCertificates.of(devCerts.resolve("ca.crt").toString()))
                .build();
    }

    /** Trust the development CA and present the development client certificate. */
    private static SSLConfig mutualTls() {
        return SSLConfig.builder()
                .trust(TrustCertificates.of(devCerts.resolve("ca.crt").toString()))
                .identity(IdentityPrivateKey.builder()
                        .certChainPath(devCerts.resolve("client.chain.crt").toString())
                        .privateKeyPath(devCerts.resolve("client.key").toString())
                        .build())
                .build();
    }

    private static SessionFactoryConfig.Factory sessionFactory(ClientConfig clientConfig) {
        return SessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .scheduler(scheduler)
                .build()
                .factory();
    }

    @Test
    void tlsWithTrustedCaConnects() throws Exception {
        final SessionFactoryConfig.Factory factory = sessionFactory(
                ClientConfig.builder().target(target(true)).ssl(trustDevCa()).build());
        try (final Session session = factory.newSession()) {
            assertThat(session.getConfigurationConstants().get(10, TimeUnit.SECONDS)).isNotEmpty();
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }

    @Test
    void mutualTlsConnects() throws Exception {
        final SessionFactoryConfig.Factory factory = sessionFactory(
                ClientConfig.builder().target(target(true)).ssl(mutualTls()).build());
        try (final Session session = factory.newSession()) {
            assertThat(session.getConfigurationConstants().get(10, TimeUnit.SECONDS)).isNotEmpty();
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }

    @Test
    void flightDataFlowsOverTls() throws Exception {
        final FlightSessionFactoryConfig.Factory factory = FlightSessionFactoryConfig.builder()
                .clientConfig(ClientConfig.builder().target(target(true)).ssl(trustDevCa()).build())
                .allocator(allocator)
                .scheduler(scheduler)
                .build()
                .factory();
        try (
                final FlightSession flight = factory.newFlightSession();
                final TableHandle handle = flight.session().execute(TableSpec.empty(100_000).view("I=ii"));
                final FlightStream stream = flight.stream(handle)) {
            long rows = 0;
            while (stream.next()) {
                rows += stream.getRoot().getRowCount();
            }
            assertThat(rows).isEqualTo(100_000);
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }

    @Test
    void defaultTrustRejectsTheSelfSignedServer() {
        // No ssl config means the JDK's trust store, which does not know the development CA
        final SessionFactoryConfig.Factory factory = sessionFactory(
                ClientConfig.builder().target(target(true)).build());
        try {
            assertThatThrownBy(() -> factory.newSession().close())
                    .hasStackTraceContaining("UNAVAILABLE");
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }

    @Test
    void plaintextAgainstTheTlsPortFails() {
        final SessionFactoryConfig.Factory factory = sessionFactory(
                ClientConfig.builder().target(target(false)).build());
        try {
            // The server answers the cleartext HTTP/2 preface with a TLS alert record, which the client's HTTP/2
            // codec rejects as not being a SETTINGS frame
            assertThatThrownBy(() -> factory.newSession().close())
                    .hasStackTraceContaining("First received frame was not SETTINGS");
        } finally {
            factory.managedChannel().shutdownNow();
        }
    }
}
