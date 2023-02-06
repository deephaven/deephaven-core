/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;

import javax.annotation.Nullable;

/**
 * The jetty server configuration.
 */
@Immutable
@BuildableStyle
// Need to let EmbeddedServer overwrite builder from python
@Style(strictBuilder = false)
public abstract class JettyConfig implements ServerConfig {

    public static final int DEFAULT_SSL_PORT = 443;
    public static final int DEFAULT_PLAINTEXT_PORT = 10000;
    public static final String HTTP_WEBSOCKETS = "http.websockets";
    public static final String HTTP_HTTP1 = "http.http1";

    /**
     * Values to indicate what kind of websocket support should be offered.
     */
    public enum WebsocketsSupport {

        /**
         * Disable all websockets. Recommended for use with https, including behind a proxy that will offer its own
         * https.
         */
        NONE,
        /**
         * Establish one websocket per grpc stream (including unary calls). Compatible with the websocket client
         * provided by https://github.com/improbable-eng/grpc-web/, but not recommended.
         */
        GRPC_WEBSOCKET,
        /**
         * Allows reuse of a single websocket for many grpc streams, even between services. This reduces latency by
         * avoiding a fresh websocket handshake per rpc.
         */
        GRPC_WEBSOCKET_MULTIPLEXED,

        /**
         * Enables both {@link #GRPC_WEBSOCKET} and {@link #GRPC_WEBSOCKET_MULTIPLEXED}, letting the client specify
         * which to use via websocket subprotocols.
         */
        BOTH;
    }

    public static Builder builder() {
        return ImmutableJettyConfig.builder();
    }

    /**
     * The default configuration is suitable for local development purposes. It inherits all of the defaults, which are
     * documented on each individual method. In brief, the default server starts up on all interfaces with plaintext
     * port {@value DEFAULT_PLAINTEXT_PORT}, a scheduler pool size of {@value DEFAULT_SCHEDULER_POOL_SIZE}, and a max
     * inbound message size of {@value DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB} MiB.
     */
    public static JettyConfig defaultConfig() {
        return builder().build();
    }

    /**
     * Parses the configuration values into the appropriate builder methods via
     * {@link ServerConfig#buildFromConfig(ServerConfig.Builder, Configuration)}.
     *
     * <p>
     * Additionally, parses the property {@value HTTP_WEBSOCKETS} into {@link Builder#websockets(WebsocketsSupport)} and
     * {@value HTTP_HTTP1} into {@link Builder#http1(Boolean)}.
     *
     * @param config the config
     * @return the builder
     */
    public static Builder buildFromConfig(Configuration config) {
        final Builder builder = ServerConfig.buildFromConfig(builder(), config);
        String httpWebsockets = config.getStringWithDefault(HTTP_WEBSOCKETS, null);
        String httpHttp1 = config.getStringWithDefault(HTTP_HTTP1, null);
        if (httpWebsockets != null) {
            switch (httpWebsockets.toLowerCase()) {
                case "true":// backwards compatible
                case "both":
                    builder.websockets(WebsocketsSupport.BOTH);
                    break;
                case "grpc-websockets":
                    builder.websockets(WebsocketsSupport.GRPC_WEBSOCKET);
                    break;
                case "grpc-websockets-multiplex":
                    builder.websockets(WebsocketsSupport.GRPC_WEBSOCKET_MULTIPLEXED);
                    break;
                default:
                    // backwards compatible, either "false" or "none" or anything else
                    builder.websockets(WebsocketsSupport.NONE);
            }
        }
        if (httpHttp1 != null) {
            builder.http1(Boolean.parseBoolean(httpHttp1));
        }
        return builder;
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
     * Include websockets.
     */
    @Nullable
    public abstract WebsocketsSupport websockets();

    /**
     * Include HTTP/1.1.
     */
    @Nullable
    public abstract Boolean http1();

    /**
     * Returns {@link #websockets()} if explicitly set. If {@link #proxyHint()} is {@code true}, returns {@code false}.
     * Otherwise, defaults to {@code true} when {@link #ssl()} is empty, and {@code false} when {@link #ssl()} is
     * present.
     */
    public final WebsocketsSupport websocketsOrDefault() {
        final WebsocketsSupport websockets = websockets();
        if (websockets != null) {
            return websockets;
        }
        if (Boolean.TRUE.equals(proxyHint())) {
            return WebsocketsSupport.NONE;
        }
        return ssl().isEmpty() ? WebsocketsSupport.BOTH : WebsocketsSupport.NONE;
    }

    /**
     * Returns {@link #http1()} if explicitly set. If {@link #proxyHint()} is {@code true}, returns {@code false}.
     * Otherwise, defaults to {@code true}. This may become more strict in the future, see
     * <a href="https://github.com/deephaven/deephaven-core/issues/2787">#2787</a>).
     */
    public final boolean http1OrDefault() {
        final Boolean http1 = http1();
        if (http1 != null) {
            return http1;
        }
        if (Boolean.TRUE.equals(proxyHint())) {
            return false;
        }
        // TODO(deephaven-core#2787): OS / browser testing to determine minimum viable HTTP version
        // return ssl().isEmpty();
        return true;
    }

    public interface Builder extends ServerConfig.Builder<JettyConfig, Builder> {

        Builder websockets(WebsocketsSupport websockets);

        Builder http1(Boolean http1);
    }
}
