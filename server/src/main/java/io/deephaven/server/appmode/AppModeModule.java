//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.appmode;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Factory;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.TicketResolver;
import io.deephaven.server.util.AuthorizationWrappedGrpcBinding;
import io.grpc.BindableService;

import java.util.LinkedHashSet;
import java.util.Set;

@Module
public interface AppModeModule {
    @Provides
    @IntoSet
    static BindableService bindApplicationServiceImpl(
            AuthorizationProvider authProvider, ApplicationServiceGrpcImpl applicationService) {
        return new AuthorizationWrappedGrpcBinding<>(
                authProvider.getApplicationServiceAuthWiring(), applicationService);
    }

    @Binds
    @IntoSet
    TicketResolver bindApplicationTicketResolver(ApplicationTicketResolver resolver);

    @Binds
    ScriptSession.Listener bindScriptSessionListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationState.Listener bindApplicationStateListener(ApplicationServiceGrpcImpl applicationService);

    @Binds
    ApplicationStates bindApplicationStates(ApplicationTicketResolver resolver);

    @Provides
    @ElementsIntoSet
    static Set<Factory> providesFactoriesFromServiceLoader() {
        final Set<Factory> set = new LinkedHashSet<>();
        ApplicationState.Factory.loadFromServiceFactory().forEach(set::add);
        return set;
    }
}
