/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.Trust;
import io.deephaven.ssl.config.TrustJdk;

import javax.inject.Named;

@Module
public class DeephavenApiConfigModule {

    @Provides
    @Named("http.port")
    public static int providesPort(ServerConfig config) {
        return config.port();
    }

    @Provides
    @Named("scheduler.poolSize")
    public static int providesSchedulerPoolSize(ServerConfig config) {
        return config.schedulerPoolSize();
    }

    @Provides
    @Named("session.tokenExpireMs")
    public static long providesSessionTokenExpireTmMs(ServerConfig config) {
        return config.tokenExpire().toMillis();
    }

    @Provides
    @Named("grpc.maxInboundMessageSize")
    public static int providesMaxInboundMessageSize(ServerConfig config) {
        return config.maxInboundMessageSize();
    }

    /**
     * The client SSL configuration is the first of {@link ServerConfig#outboundSsl()}, {@link ServerConfig#ssl()}, or
     * {@link SSLConfig#empty()}. In addition, {@link TrustJdk} is mixed-in.
     *
     * @param config the server configuration
     * @return the client SSL configuration
     */
    @Provides
    @Named("client.sslConfig")
    public static SSLConfig providesSSLConfigForClient(ServerConfig config) {
        final SSLConfig clientSslConfig = config.outboundSsl().or(config::ssl).orElseGet(SSLConfig::empty);
        final Trust trustPlusJdk = clientSslConfig.trust().orElse(TrustJdk.of()).or(TrustJdk.of());
        return clientSslConfig.withTrust(trustPlusJdk);
    }
}
