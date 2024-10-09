//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

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
    public static final String HTTP_STREAM_TIMEOUT = "http2.stream.idleTimeoutMs";
    public static final String HTTP_COMPRESSION = "http.compression";
    public static final String SNI_HOST_CHECK = "https.sniHostCheck";
    public static final String MAX_CONCURRENT_STREAMS = "http2.maxConcurrentStreams";
    public static final String MAX_HEADER_REQUEST_SIZE = "http.maxHeaderRequestSize";

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
         * provided by <a href="https://github.com/improbable-eng/grpc-web/">improbable-eng/grpc-web</a>, but not
         * recommended.
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
     * Additionally, parses the property {@value HTTP_WEBSOCKETS} into {@link Builder#websockets(WebsocketsSupport)},
     * {@value HTTP_HTTP1} into {@link Builder#http1(Boolean)}, {@value HTTP_STREAM_TIMEOUT} into
     * {@link Builder#http2StreamIdleTimeout(long)}, and {@value HTTP_COMPRESSION} into
     * {@link Builder#httpCompression(Boolean)}
     *
     * @param config the config
     * @return the builder
     */
    public static Builder buildFromConfig(Configuration config) {
        final Builder builder = ServerConfig.buildFromConfig(builder(), config);
        String httpWebsockets = config.getStringWithDefault(HTTP_WEBSOCKETS, null);
        String httpHttp1 = config.getStringWithDefault(HTTP_HTTP1, null);
        String httpCompression = config.getStringWithDefault(HTTP_COMPRESSION, null);
        String sniHostCheck = config.getStringWithDefault(SNI_HOST_CHECK, null);
        String h2StreamIdleTimeout = config.getStringWithDefault(HTTP_STREAM_TIMEOUT, null);
        String h2MaxConcurrentStreams = config.getStringWithDefault(MAX_CONCURRENT_STREAMS, null);
        String maxHeaderRequestSize = config.getStringWithDefault(MAX_HEADER_REQUEST_SIZE, null);
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
        if (h2StreamIdleTimeout != null) {
            builder.http2StreamIdleTimeout(Long.parseLong(h2StreamIdleTimeout));
        }
        if (httpCompression != null) {
            builder.httpCompression(Boolean.parseBoolean(httpCompression));
        }
        if (sniHostCheck != null) {
            builder.sniHostCheck(Boolean.parseBoolean(sniHostCheck));
        }
        if (h2MaxConcurrentStreams != null) {
            builder.maxConcurrentStreams(Integer.parseInt(h2MaxConcurrentStreams));
        }
        if (maxHeaderRequestSize != null) {
            builder.maxHeaderRequestSize(Integer.parseInt(maxHeaderRequestSize));
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
     * Include sniHostCheck.
     */
    @Default
    public boolean sniHostCheck() {
        return true;
    }

    public abstract OptionalLong http2StreamIdleTimeout();

    /**
     * Include HTTP compression.
     */
    @Nullable
    public abstract Boolean httpCompression();

    /**
     * How long can a stream be idle in milliseconds before it should be shut down. Non-positive values disable this
     * feature. Default is zero.
     */
    public long http2StreamIdleTimeoutOrDefault() {
        return http2StreamIdleTimeout().orElse(0);
    }

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

    /**
     * Returns {@link #httpCompression()} if explicitly set, otherwise returns {@code true}.
     */
    public final boolean httpCompressionOrDefault() {
        final Boolean httpCompression = httpCompression();
        return httpCompression == null || httpCompression;
    }

    /**
     * Value is in bytes. If unset, uses Jetty's default (presently 8192).
     */
    public abstract OptionalInt maxHeaderRequestSize();

    /**
     * If unset, uses Jetty's default (presently 128). Only applies to http2 connections.
     */
    public abstract OptionalInt maxConcurrentStreams();

    public interface Builder extends ServerConfig.Builder<JettyConfig, Builder> {

        Builder websockets(WebsocketsSupport websockets);

        Builder http1(Boolean http1);

        Builder httpCompression(Boolean httpCompression);

        Builder http2StreamIdleTimeout(long timeoutInMillis);

        Builder sniHostCheck(boolean sniHostCheck);

        Builder maxHeaderRequestSize(int maxHeaderRequestSize);

        Builder maxConcurrentStreams(int maxConcurrentStreams);
    }
}
