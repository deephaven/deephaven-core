//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

@Module
public interface ObjectServiceModule {
    @Binds
    @IntoSet
    BindableService bindObjectServiceGrpcImpl(ObjectServiceGrpcBinding objectService);
}
