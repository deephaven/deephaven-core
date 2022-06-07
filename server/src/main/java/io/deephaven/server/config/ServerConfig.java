package io.deephaven.server.config;

import io.deephaven.ssl.config.SSLConfig;
import org.immutables.value.Value.Default;

import java.time.Duration;
import java.util.Optional;

/**
 * The server configuration.
 */
public interface ServerConfig {

    String DEFAULT_HOST = "0.0.0.0";

    int DEFAULT_TOKEN_EXPIRE_MIN = 5;

    int DEFAULT_SCHEDULER_POOL_SIZE = 4;

    int DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB = 100;

    /**
     * The host. Defaults to {@value DEFAULT_HOST}.
     */
    @Default
    default String host() {
        return DEFAULT_HOST;
    }

    /**
     * The port.
     */
    int port();

    /**
     * The optional SSL configuration.
     */
    Optional<SSLConfig> ssl();

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

    interface Builder<T, B extends Builder<T, B>> {
        B host(String host);

        B port(int port);

        B ssl(SSLConfig ssl);

        B tokenExpire(Duration duration);

        B schedulerPoolSize(int schedulerPoolSize);

        B maxInboundMessageSize(int maxInboundMessageSize);

        T build();
    }
}
