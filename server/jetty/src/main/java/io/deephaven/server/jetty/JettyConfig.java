package io.deephaven.server.jetty;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

/**
 * The jetty server configuration.
 */
@Immutable
@BuildableStyle
public abstract class JettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 10000;
    public static final boolean DEFAULT_WITH_WEBSOCKETS = true;

    public static Builder builder() {
        return ImmutableJettyConfig.builder();
    }

    /**
     * The default configuration is suitable for local development purposes. It inherits all of the defaults, which are
     * documented on each individual method. In brief, the default server starts up on all interfaces with plaintext
     * port {@value DEFAULT_PLAINTEXT_PORT}, a token expiration duration of {@value DEFAULT_TOKEN_EXPIRE_MIN} minutes, a
     * scheduler pool size of {@value DEFAULT_SCHEDULER_POOL_SIZE}, and a max inbound message size of
     * {@value DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB} MiB.
     */
    public static JettyConfig defaultConfig() {
        return builder().build();
    }

    /**
     * The port. Defaults to {@value DEFAULT_SSL_PORT} if {@link #ssl()} is present, otherwise defaults to
     * {@value DEFAULT_PLAINTEXT_PORT}.
     */
    @Default
    public int port() {
        return ssl().isPresent() ? DEFAULT_SSL_PORT : DEFAULT_PLAINTEXT_PORT;
    }

    /**
     * Include websockets. Defaults to {@value DEFAULT_WITH_WEBSOCKETS}.
     */
    @Default
    public boolean websockets() {
        return DEFAULT_WITH_WEBSOCKETS;
    }

    public interface Builder extends ServerConfig.Builder<JettyConfig, Builder> {

        Builder websockets(boolean websockets);
    }
}
