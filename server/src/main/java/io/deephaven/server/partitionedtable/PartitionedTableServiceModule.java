package io.deephaven.server.partitionedtable;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.codegen.impl.PartitionedTableServiceContextualAuthWiring;
import io.deephaven.server.auth.AuthorizationProvider;
import io.grpc.BindableService;

@Module
public interface PartitionedTableServiceModule {
    @Provides
    static PartitionedTableServiceContextualAuthWiring provideAuthWiring(AuthorizationProvider authProvider) {
        return authProvider.getPartitionedTableServiceContextualAuthWiring();
    }

    @Binds
    @IntoSet
    BindableService bindPartitionedTableServiceGrpcImpl(PartitionedTableServiceGrpcImpl instance);
}
