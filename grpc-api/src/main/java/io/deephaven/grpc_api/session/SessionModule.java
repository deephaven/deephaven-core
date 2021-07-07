package io.deephaven.grpc_api.session;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

@Module
public interface SessionModule {
    @Binds @IntoSet
    BindableService bindSessionServiceGrpcImpl(SessionServiceGrpcImpl sessionServiceGrpc);
    @Binds @IntoSet
    ServerInterceptor bindSessionServiceInterceptor(SessionServiceGrpcImpl.AuthServerInterceptor sessionServiceInterceptor);

    @Binds @IntoSet
    TicketResolver bindSessionTicketResolverServerSideExports(ExportTicketResolver resolver);
}
