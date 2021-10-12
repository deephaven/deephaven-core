package io.deephaven.grpc_api.table.inputtables;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface InputTableModule {
    @Binds
    @IntoSet
    BindableService bindInputTableServiceGrpcImpl(InputTableServiceGrpcImpl inputTableService);
}
