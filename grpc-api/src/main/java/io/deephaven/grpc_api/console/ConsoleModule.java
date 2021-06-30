package io.deephaven.grpc_api.console;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.grpc_api.session.TicketResolver;
import io.grpc.BindableService;

@Module
public interface ConsoleModule {
    @Binds
    @IntoSet
    BindableService bindConsoleServiceImpl(ConsoleServiceGrpcImpl consoleService);

    @Binds @IntoSet
    TicketResolver bindConsoleTicketResolver(ScopeTicketResolver resolver);
}
