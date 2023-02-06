package io.deephaven.server.notebook;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

/**
 * gRPC Storage Service implementation based on the filesystem.
 */
@Module
public interface FilesystemStorageServiceModule {
    @Provides
    @IntoSet
    static BindableService bindStorageServiceGrpcImpl(
            AuthorizationProvider authProvider, FilesystemStorageServiceGrpcImpl instance) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getStorageServiceAuthWiring(), instance);
    }
}
