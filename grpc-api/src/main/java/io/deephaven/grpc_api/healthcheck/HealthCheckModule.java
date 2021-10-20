package io.deephaven.grpc_api.healthcheck;

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
        ProcessEnvironment.getGlobalShutdownManager().registerTask(
                ShutdownManager.OrderingCategory.FIRST,
                healthStatusManager::enterTerminalState);

        return healthStatusManager;
    }

    @Provides
    @IntoSet
    BindableService bindHealthServiceImpl(HealthStatusManager healthStatusManager) {
        return healthStatusManager.getHealthService();
    }
}
