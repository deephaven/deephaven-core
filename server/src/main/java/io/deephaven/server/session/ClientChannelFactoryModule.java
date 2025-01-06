//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import dagger.Module;
import dagger.Provides;
import io.deephaven.client.impl.ClientChannelFactory;
import io.deephaven.client.impl.ClientChannelFactoryDefaulter;
import io.deephaven.ssl.config.SSLConfig;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Module
public interface ClientChannelFactoryModule {
    @Documented
    @Retention(RetentionPolicy.SOURCE)
    @interface SslConfig {

    }

    @Documented
    @Retention(RetentionPolicy.SOURCE)
    @interface UserAgent {

    }

    @Provides
    static ClientChannelFactory providesClientChannelFactory(
            @SslConfig SSLConfig sslConfig,
            @UserAgent String userAgent) {
        return ClientChannelFactoryDefaulter.builder()
                .ssl(sslConfig)
                .userAgent(userAgent)
                .build();
    }
}
