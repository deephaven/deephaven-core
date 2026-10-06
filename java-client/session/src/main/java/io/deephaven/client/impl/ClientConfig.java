//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.CopyableStyle;
import io.deephaven.grpc.compression.CompressionCodecs;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.uri.DeephavenTarget;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The client configuration encapsulates the configuration to created a {@link io.grpc.ManagedChannel}.
 */
@Immutable
@CopyableStyle
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
     * The SSL configuration. Only relevant if {@link #target()} is secure.
     */
    public abstract Optional<SSLConfig> ssl();

    /**
     * The user-agent.
     *
     * @see <a href="https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#user-agents">grpc user-agents</a>
     */
    public abstract Optional<String> userAgent();

    /**
     * The overridden authority.
     */
    public abstract Optional<String> overrideAuthority();

    /**
     * The extra headers.
     */
    public abstract Map<String, String> extraHeaders();

    /**
     * The maximum inbound message size. Defaults to 100MiB.
     */
    @Default
    public int maxInboundMessageSize() {
        return DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    }

    /**
     * The gRPC message encodings this client advertises in {@code grpc-accept-encoding}, and so allows the server to
     * compress responses with. Any of {@code gzip}, {@code zstd} and {@code snappy}; defaults to all three. The server
     * only compresses tables that allow it, and picks the encoding from the table's own list. An empty set requests
     * uncompressed responses.
     */
    @Default
    @SuppressWarnings("immutables:untype") // a defaulted set is set as a whole, without add methods
    public Set<String> acceptCompression() {
        return Set.copyOf(CompressionCodecs.SUPPORTED);
    }

    /**
     * Returns or creates a client config with {@link #ssl()} as {@code ssl}.
     */
    public abstract ClientConfig withSsl(SSLConfig ssl);

    /**
     * Returns or creates a client config with {@link #userAgent()} as {@code userAgent}.
     */
    public abstract ClientConfig withUserAgent(String userAgent);

    @Check
    final void checkAcceptCompression() {
        acceptCompression().forEach(CompressionCodecs::checkSupported);
    }

    public interface Builder {

        Builder target(DeephavenTarget target);

        Builder ssl(SSLConfig ssl);

        Builder userAgent(String userAgent);

        Builder overrideAuthority(String overrideAuthority);

        Builder putExtraHeaders(String key, String value);

        Builder putAllExtraHeaders(Map<String, ? extends String> entries);

        Builder maxInboundMessageSize(int maxInboundMessageSize);

        Builder acceptCompression(Set<String> acceptCompression);

        ClientConfig build();
    }
}
