package io.deephaven.server.console;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

@Module
public interface ConsoleModule {
    @Binds
    @IntoSet
    BindableService bindConsoleServiceImpl(ConsoleServiceGrpcBinding consoleService);

    @Binds
    @IntoSet
    TicketResolver bindConsoleTicketResolver(ScopeTicketResolver resolver);
}
