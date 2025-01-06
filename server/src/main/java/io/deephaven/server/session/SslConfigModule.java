//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.session.ClientChannelFactoryModule.SslConfig;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.Trust;
import io.deephaven.ssl.config.TrustJdk;

@Module(includes = {ClientChannelFactoryModule.class})
public interface SslConfigModule {
    /**
     * The client SSL configuration is the first of {@link ServerConfig#outboundSsl()}, {@link ServerConfig#ssl()}, or
     * {@link SSLConfig#empty()}. In addition, {@link TrustJdk} is mixed-in.
     *
     * @param config the server configuration
     * @return the client SSL configuration
     */
    @Provides
    @SslConfig
    static SSLConfig providesSSLConfigForClient(ServerConfig config) {
        final SSLConfig clientSslConfig = config.outboundSsl().or(config::ssl).orElseGet(SSLConfig::empty);
        final Trust trustPlusJdk = clientSslConfig.trust().orElse(TrustJdk.of()).or(TrustJdk.of());
        return clientSslConfig.withTrust(trustPlusJdk);
    }
}
