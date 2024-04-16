//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.config;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.ssl.config.SSLConfig;
import org.immutables.value.Value.Default;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

/**
 * The server configuration.
 */
public interface ServerConfig {

    int DEFAULT_SCHEDULER_POOL_SIZE = 4;

    int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB = 100;

    int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    String HTTP_SESSION_DURATION_MS = "http.session.durationMs";

    String HTTP_HOST = "http.host";

    String HTTP_PORT = "http.port";

    String HTTP_TARGET_URL = "http.targetUrl";

    String SCHEDULER_POOL_SIZE = "scheduler.poolSize";

    String GRPC_MAX_INBOUND_MESSAGE_SIZE = "grpc.maxInboundMessageSize";

    String PROXY_HINT = "proxy.hint";
    String SHUTDOWN_TIMEOUT_MILLIS = "shutdown.timeoutMs";

    /**
     * Parses the configuration values into the appropriate builder methods.
     *
     * <table>
     * <tr>
     * <th>Property</th>
     * <th>Method</th>
     * </tr>
     * <tr>
     * <td>{@value HTTP_SESSION_DURATION_MS}</td>
     * <td>{@link Builder#tokenExpire(Duration)}</td>
     * </tr>
     * <tr>
     * <td>{@value HTTP_HOST}</td>
     * <td>{@link Builder#host(String)}</td>
     * </tr>
     * <tr>
     * <td>{@value HTTP_PORT}</td>
     * <td>{@link Builder#port(int)}</td>
     * </tr>
     * <tr>
     * <td>{@value HTTP_TARGET_URL}</td>
     * <td>{@link Builder#targetUrl(String)}</td>
     * </tr>
     * <tr>
     * <td>{@value SCHEDULER_POOL_SIZE}</td>
     * <td>{@link Builder#schedulerPoolSize(int)}</td>
     * </tr>
     * <tr>
     * <td>{@value GRPC_MAX_INBOUND_MESSAGE_SIZE}</td>
     * <td>{@link Builder#maxInboundMessageSize(int)}</td>
     * </tr>
     * <tr>
     * <td>{@value PROXY_HINT}</td>
     * <td>{@link Builder#proxyHint(Boolean)}</td>
     * </tr>
     * </table>
     *
     * Also parses {@link MainHelper#parseSSLConfig(Configuration)} into {@link Builder#ssl(SSLConfig)} and
     * {@link MainHelper#parseOutboundSSLConfig(Configuration)} into {@link Builder#outboundSsl(SSLConfig)}.
     * <p>
     * See {@link MainHelper#parseSSLConfig(Configuration)} for {@link Builder#ssl(SSLConfig)}.
     * </p>
     * 
     * @param builder the builder
     * @param config the configuration
     * @return the builder
     * @param <B> the builder type
     */
    static <B extends Builder<?, B>> B buildFromConfig(B builder, Configuration config) {
        int httpSessionExpireMs = config.getIntegerWithDefault(HTTP_SESSION_DURATION_MS, -1);
        String httpHost = config.getStringWithDefault(HTTP_HOST, null);
        int httpPort = config.getIntegerWithDefault(HTTP_PORT, -1);
        String httpTargetUrl = config.getStringWithDefault(HTTP_TARGET_URL, null);
        int schedulerPoolSize = config.getIntegerWithDefault(SCHEDULER_POOL_SIZE, -1);
        int maxInboundMessageSize = config.getIntegerWithDefault(GRPC_MAX_INBOUND_MESSAGE_SIZE, -1);
        String proxyHint = config.getStringWithDefault(PROXY_HINT, null);
        int shutdownTimeoutMillis = config.getIntegerWithDefault(SHUTDOWN_TIMEOUT_MILLIS, -1);
        if (httpSessionExpireMs > -1) {
            builder.tokenExpire(Duration.ofMillis(httpSessionExpireMs));
        }
        if (httpHost != null) {
            builder.host(httpHost);
        }
        if (httpPort != -1) {
            builder.port(httpPort);
        }
        if (httpTargetUrl != null) {
            builder.targetUrl(httpTargetUrl);
        }
        if (schedulerPoolSize != -1) {
            builder.schedulerPoolSize(schedulerPoolSize);
        }
        if (maxInboundMessageSize != -1) {
            builder.maxInboundMessageSize(maxInboundMessageSize);
        }
        if (proxyHint != null) {
            builder.proxyHint(Boolean.parseBoolean(proxyHint));
        }
        if (shutdownTimeoutMillis != -1) {
            builder.shutdownTimeout(Duration.ofMillis(shutdownTimeoutMillis));
        }
        MainHelper.parseSSLConfig(config).ifPresent(builder::ssl);
        MainHelper.parseOutboundSSLConfig(config).ifPresent(builder::outboundSsl);
        return builder;
    }

    /**
     * The network interface this server binds to as an IP address or a hostname. If not set, then bind to all
     * interfaces.
     */
    Optional<String> host();

    /**
     * The port.
     */
    int port();

    /**
     * The user-accessible target URL.
     */
    Optional<String> targetUrl();

    /**
     * The optional SSL configuration.
     */
    Optional<SSLConfig> ssl();

    /**
     * The optional outbound SSL configuration.
     */
    Optional<SSLConfig> outboundSsl();

    /**
     * The token expiration.
     */
    Duration tokenExpire();

    /**
     * The scheduler pool size. Defaults to {@value DEFAULT_SCHEDULER_POOL_SIZE}.
     */
    @Default
    default int schedulerPoolSize() {
        return DEFAULT_SCHEDULER_POOL_SIZE;
    }

    /**
     * The maximum inbound message size. Defaults to {@value DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB} MiB.
     */
    @Default
    default int maxInboundMessageSize() {
        return DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB * 1024 * 1024;
    }

    /**
     * How many do we wait to shut down the server. Defaults to {@value DEFAULT_SHUTDOWN_TIMEOUT_MILLIS}.
     */
    @Default
    default Duration shutdownTimeout() {
        return Duration.ofMillis(DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    /**
     * A hint that the server is running behind a proxy. This may allow consumers of the configuration to make more
     * appropriate default choices.
     */
    @Nullable
    Boolean proxyHint();

    /**
     * Returns {@link #targetUrl()} if set, otherwise computes a reasonable default based on {@link #host()},
     * {@link #port()}, and {@link #ssl()}.
     *
     * @return the target URL or default
     */
    default String targetUrlOrDefault() {
        final Optional<String> targetUrl = targetUrl();
        if (targetUrl.isPresent()) {
            return targetUrl.get();
        }
        final String host = host().orElse("localhost");
        final int port = port();
        if (ssl().isPresent()) {
            if (port == 443) {
                return String.format("https://%s", host);
            } else {
                return String.format("https://%s:%d", host, port);
            }
        } else {
            if (port == 80) {
                return String.format("http://%s", host);
            } else {
                return String.format("http://%s:%d", host, port);
            }
        }
    }

    interface Builder<T, B extends Builder<T, B>> {
        B host(String host);

        B port(int port);

        B targetUrl(String targetUrl);

        B ssl(SSLConfig ssl);

        B outboundSsl(SSLConfig outboundSsl);

        B tokenExpire(Duration duration);

        B schedulerPoolSize(int schedulerPoolSize);

        B maxInboundMessageSize(int maxInboundMessageSize);

        B proxyHint(Boolean proxyHint);

        B shutdownTimeout(Duration shutdownTimeout);

        T build();
    }
}
