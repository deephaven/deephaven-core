/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.ssl.config.CiphersModern;
import io.deephaven.ssl.config.ProtocolsModern;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.Trust;
import io.deephaven.ssl.config.TrustJdk;

import javax.annotation.Nullable;
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

    @Provides
    @Named("client.sslConfig")
    public static SSLConfig providesSSLConfigForClient(ServerConfig config) {
        // The client configuration is based off of the server configuration.
        // TrustJdk gets mixed in.
        final Trust mixinTrustJdk = config.ssl().flatMap(SSLConfig::trust).orElse(TrustJdk.of()).or(TrustJdk.of());
        return config.ssl()
                .orElseGet(SSLConfig::empty)
                .withTrust(mixinTrustJdk);
    }
}
