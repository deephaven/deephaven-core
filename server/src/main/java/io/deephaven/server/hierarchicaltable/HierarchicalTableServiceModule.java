package io.deephaven.server.hierarchicaltable;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface HierarchicalTableServiceModule {
    @Binds
    @IntoSet
    BindableService bindHierarchicalTableServiceGrpcImpl(HierarchicalTableServiceGrpcImpl instance);
}
