package io.deephaven.server.netty;

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
 * The netty server configuration. Typically sourced from a JSON file via {@link #parseJson(Path)}.
 */
@Immutable
@BuildableStyle
@JsonDeserialize
public abstract class NettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 8080;

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
     *   "maxInboundMessageSize": 104857600
     * }
     * </pre>
     */
    public static NettyConfig defaultConfig() {
        return ImmutableNettyConfig.builder().build();
    }

    /**
     * Parse the JSON at {@code path} into a netty server configuration.
     *
     * @param path the path
     * @return the configuration
     * @throws IOException if an IO exception occurs
     */
    public static NettyConfig parseJson(Path path) throws IOException {
        return Parser.parseJson(path.toFile(), ImmutableNettyConfig.class);
    }

    /**
     * Parse the JSON at {@code url} into a netty server configuration.
     *
     * @param url the url
     * @return the configuration
     * @throws IOException if an IO exception occurs
     */
    public static NettyConfig parseJson(URL url) throws IOException {
        return Parser.parseJson(url, ImmutableNettyConfig.class);
    }

    public static NettyConfig parseJsonUnchecked(Path path) {
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
}
