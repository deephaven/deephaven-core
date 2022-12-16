/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.healthcheck;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.util.process.ShutdownManager;
import io.grpc.BindableService;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.protobuf.services.HealthStatusManager;

import javax.inject.Singleton;

@Module
public class HealthCheckModule {
    @Provides
    @Singleton
    public HealthStatusManager bindHealthStatusManager() {
        HealthStatusManager healthStatusManager = new HealthStatusManager();

        // As we start to shut down, first notify all watchers of the health service
        ProcessEnvironment.getGlobalShutdownManager().registerTask(
                ShutdownManager.OrderingCategory.FIRST,
                healthStatusManager::enterTerminalState);

        return healthStatusManager;
    }

    @Provides
    @IntoSet
    BindableService bindHealthServiceImpl(
            AuthorizationProvider authorizationProvider, HealthStatusManager healthStatusManager) {
        return new AuthorizationWrappedGrpcBinding<>(
                authorizationProvider.getHealthAuthWiring(),
                (HealthGrpc.HealthImplBase) healthStatusManager.getHealthService());
    }
}
