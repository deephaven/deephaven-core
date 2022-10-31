package io.deephaven.server.config;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface ConfigServiceModule {
    @Binds
    @IntoSet
    BindableService bindConfigServiceGrpcImpl(ConfigServiceGrpcImpl instance);
}
