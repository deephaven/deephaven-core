//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.config.ServerConfig;

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
}
