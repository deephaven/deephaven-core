package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.ssl.config.Parser;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

/**
 * The jetty server configuration. Typically sourced from a JSON file via {@link #parseJson(Path)}.
 */
@Immutable
@BuildableStyle
@JsonDeserialize
public abstract class JettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 10000;
    public static final boolean DEFAULT_WITH_WEBSOCKETS = true;

    /**
     * The default configuration is suitable for local development purposes. It inherits all of the defaults, which are
     * documented on each individual method. In brief, the default server starts up on all interfaces with plaintext
     * port {@value DEFAULT_PLAINTEXT_PORT}, a token expiration duration of {@value DEFAULT_TOKEN_EXPIRE_MIN} minutes, a
     * scheduler pool size of {@value DEFAULT_SCHEDULER_POOL_SIZE}, and a max inbound message size of
     * {@value DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB} MiB.
     *
     * <p>
     * The default configuration can be represented as an empty JSON object "{}", or as:
     *
     * <pre>
     * {
     *   "host": "0.0.0.0",
     *   "port": 8080,
     *   "tokenExpire": "PT5m",
     *   "schedulerPoolSize": 4,
     *   "maxInboundMessageSize": 104857600,
     *   "websockets": true
     * }
     * </pre>
     */
    public static JettyConfig defaultConfig() {
        return ImmutableJettyConfig.builder().build();
    }

    /**
     * Parse the JSON at {@code path} into a jetty server configuration.
     *
     * @param path the path
     * @return the configuration
     * @throws IOException if an IO exception occurs
     */
    public static JettyConfig parseJson(Path path) throws IOException {
        return Parser.parseJson(path.toFile(), ImmutableJettyConfig.class);
    }

    /**
     * Parse the JSON at {@code url} into a jetty server configuration.
     *
     * @param url the url
     * @return the configuration
     * @throws IOException if an IO exception occurs
     */
    public static JettyConfig parseJson(URL url) throws IOException {
        return Parser.parseJson(url, ImmutableJettyConfig.class);
    }

    public static JettyConfig parseJsonUnchecked(Path path) {
        try {
            return parseJson(path);
        } catch (IOException e) {
            throw new UncheckedDeephavenException(e);
        }
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
}
