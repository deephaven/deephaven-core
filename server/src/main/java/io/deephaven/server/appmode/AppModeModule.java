package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

@Module
public interface AppModeModule {
    @Binds
    @IntoSet
    BindableService bindApplicationServiceImpl(ApplicationServiceGrpcImpl applicationService);

    @Binds
    @IntoSet
    TicketResolver bindApplicationTicketResolver(ApplicationTicketResolver resolver);

    @Binds
    ScriptSession.Listener bindScriptSessionListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationState.Listener bindApplicationStateListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationStates bindApplicationStates(ApplicationTicketResolver resolver);
}
