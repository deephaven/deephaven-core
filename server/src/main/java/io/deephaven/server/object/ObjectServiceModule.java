/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.object;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

@Module
public interface ObjectServiceModule {
    @Provides
    @IntoSet
    static BindableService bindObjectServiceGrpcImpl(
            AuthorizationProvider authProvider, ObjectServiceGrpcImpl objectService) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getObjectServiceAuthWiring(), objectService);
    }
}
