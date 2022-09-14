/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.config;

import io.deephaven.configuration.Configuration;
import io.deephaven.server.runner.Main;
import io.deephaven.ssl.config.SSLConfig;
import org.immutables.value.Value.Default;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;

/**
 * The server configuration.
 */
public interface ServerConfig {

    int DEFAULT_TOKEN_EXPIRE_MIN = 5;

    int DEFAULT_SCHEDULER_POOL_SIZE = 4;

    int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB = 100;

    String HTTP_SESSION_DURATION_MS = "http.session.durationMs";

    String HTTP_HOST = "http.host";

    String HTTP_PORT = "http.port";

    String SCHEDULER_POOL_SIZE = "scheduler.poolSize";

    String GRPC_MAX_INBOUND_MESSAGE_SIZE = "grpc.maxInboundMessageSize";

    String PROXY_HINT = "proxy.hint";

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
     * Also parses {@link Main#parseSSLConfig(Configuration)} into {@link Builder#ssl(SSLConfig)} and
     * {@link Main#parseOutboundSSLConfig(Configuration)} into {@link Builder#outboundSsl(SSLConfig)}.
     *
     * @param builder the builder
     * @param config the configuration
     * @return the builder
     * @param <B> the builder type
     * @see Main#parseSSLConfig(Configuration) for {@link Builder#ssl(SSLConfig)}
     */
    static <B extends Builder<?, B>> B buildFromConfig(B builder, Configuration config) {
        int httpSessionExpireMs = config.getIntegerWithDefault(HTTP_SESSION_DURATION_MS, -1);
        String httpHost = config.getStringWithDefault(HTTP_HOST, null);
        int httpPort = config.getIntegerWithDefault(HTTP_PORT, -1);
        int schedulerPoolSize = config.getIntegerWithDefault(SCHEDULER_POOL_SIZE, -1);
        int maxInboundMessageSize = config.getIntegerWithDefault(GRPC_MAX_INBOUND_MESSAGE_SIZE, -1);
        String proxyHint = config.getStringWithDefault(PROXY_HINT, null);
        if (httpSessionExpireMs > -1) {
            builder.tokenExpire(Duration.ofMillis(httpSessionExpireMs));
        }
        if (httpHost != null) {
            builder.host(httpHost);
        }
        if (httpPort != -1) {
            builder.port(httpPort);
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
        Main.parseSSLConfig(config).ifPresent(builder::ssl);
        Main.parseOutboundSSLConfig(config).ifPresent(builder::outboundSsl);
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
     * The optional SSL configuration.
     */
    Optional<SSLConfig> ssl();

    /**
     * The optional outbound SSL configuration.
     */
    Optional<SSLConfig> outboundSsl();

    /**
     * The token expiration. Defaults to {@value DEFAULT_TOKEN_EXPIRE_MIN} minutes.
     */
    @Default
    default Duration tokenExpire() {
        return Duration.ofMinutes(DEFAULT_TOKEN_EXPIRE_MIN);
    }

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
     * A hint that the server is running behind a proxy. This may allow consumers of the configuration to make more
     * appropriate default choices.
     */
    @Nullable
    Boolean proxyHint();

    interface Builder<T, B extends Builder<T, B>> {
        B host(String host);

        B port(int port);

        B ssl(SSLConfig ssl);

        B outboundSsl(SSLConfig outboundSsl);

        B tokenExpire(Duration duration);

        B schedulerPoolSize(int schedulerPoolSize);

        B maxInboundMessageSize(int maxInboundMessageSize);

        B proxyHint(Boolean proxyHint);

        T build();
    }
}
