//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.inputtables;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.codegen.impl.InputTableServiceContextualAuthWiring;
import io.deephaven.server.auth.AuthorizationProvider;
import io.grpc.BindableService;

@Module
public interface InputTableModule {
    @Provides
    static InputTableServiceContextualAuthWiring provideAuthWiring(AuthorizationProvider authProvider) {
        return authProvider.getInputTableServiceContextualAuthWiring();
    }

    @Binds
    @IntoSet
    BindableService bindInputTableServiceGrpcImpl(InputTableServiceGrpcImpl inputTableService);
}
