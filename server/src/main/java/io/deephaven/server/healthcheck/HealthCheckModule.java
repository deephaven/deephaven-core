/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.healthcheck;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.BindableService;
import io.grpc.protobuf.services.HealthStatusManager;

import javax.inject.Singleton;

@Module
public class HealthCheckModule {
    @Provides
    @Singleton
    public HealthStatusManager bindHealthStatusManager() {
        HealthStatusManager healthStatusManager = new HealthStatusManager();
        return healthStatusManager;
    }

    @Provides
    @IntoSet
    BindableService bindHealthServiceImpl(HealthStatusManager healthStatusManager) {
        return healthStatusManager.getHealthService();
    }
}
