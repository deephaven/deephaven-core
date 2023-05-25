package io.deephaven.server.config;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

@Module
public interface ConfigServiceModule {
    @Provides
    @IntoSet
    static BindableService bindConfigServiceGrpcImpl(
            AuthorizationProvider authProvider, ConfigServiceGrpcImpl instance) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getConfigServiceAuthWiring(), instance);
    }
}
