//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.uri.DeephavenTarget;

/**
 * How the API-level tests reach the Docker server: its host port comes from the {@code dh.port} system property the
 * Gradle test task sets once the container is up, and it accepts anonymous sessions and the pre-shared key
 * {@link #PSK}.
 */
final class TestServer {

    /** The pre-shared key the test server is started with; see {@code build.gradle}. */
    static final String PSK = "deephaven";

    static DeephavenTarget target() {
        return DeephavenTarget.builder()
                .host("localhost")
                .port(Integer.parseInt(ExampleRunner.requireProperty("dh.port")))
                .isSecure(false)
                .build();
    }

    static ClientConfig clientConfig() {
        return ClientConfig.builder().target(target()).build();
    }

    /** A session config that authenticates with the given pre-shared key. */
    static SessionConfig pskSessionConfig(String key) {
        return SessionConfig.builder()
                .authenticationTypeAndValue("io.deephaven.authentication.psk.PskAuthenticationHandler " + key)
                .build();
    }

    private TestServer() {}
}
