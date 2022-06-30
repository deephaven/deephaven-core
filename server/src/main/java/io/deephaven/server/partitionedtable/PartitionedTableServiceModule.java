package io.deephaven.server.partitionedtable;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface PartitionedTableServiceModule {
    @Binds
    @IntoSet
    BindableService bindPartitionedTableServiceGrpcImpl(PartitionedTableServiceGrpcImpl instance);
}
