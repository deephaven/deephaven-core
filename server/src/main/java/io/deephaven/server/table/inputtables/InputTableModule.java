/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table.inputtables;

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
