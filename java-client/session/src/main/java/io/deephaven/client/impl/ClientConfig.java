/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * The client configuration.
 */
@Immutable
@BuildableStyle
public abstract class ClientConfig {

    public static final int DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 100 * 1024 * 1024;

    public static Builder builder() {
        return ImmutableClientConfig.builder();
    }

    /**
     * The target.
     */
    public abstract DeephavenTarget target();

    /**
     * The SSL configuration.
     */
    public abstract Optional<SSLConfig> ssl();

    /**
     * The user agent.
     */
    public abstract Optional<String> userAgent();

    /**
     * The maximum inbound message size. Defaults to 100MiB.
     */
    @Default
    public int maxInboundMessageSize() {
        return DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    }

    @Check
    final void checkSslStatus() {
        if (!target().isSecure() && ssl().isPresent()) {
            throw new IllegalArgumentException("target() is trying to connect via plaintext, but ssl() is present");
        }
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder ssl(SSLConfig ssl);

        Builder userAgent(String userAgent);

        Builder maxInboundMessageSize(int maxInboundMessageSize);

        ClientConfig build();
    }
}
