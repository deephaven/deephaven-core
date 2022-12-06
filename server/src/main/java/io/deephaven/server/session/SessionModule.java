/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

@Module
public interface SessionModule {
    @Provides
    @IntoSet
    static BindableService bindSessionServiceGrpcImpl(
            AuthorizationProvider authProvider, SessionServiceGrpcImpl sessionServiceGrpc) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getSessionServiceAuthWiring(), sessionServiceGrpc);
    }

    @Binds
    @IntoSet
    ServerInterceptor bindSessionServiceInterceptor(
            SessionServiceGrpcImpl.AuthServerInterceptor sessionServiceInterceptor);

    @Binds
    @IntoSet
    TicketResolver bindSessionTicketResolverServerSideExports(ExportTicketResolver resolver);
}
