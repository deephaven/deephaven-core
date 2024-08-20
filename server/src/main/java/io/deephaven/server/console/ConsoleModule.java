//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import dagger.multibindings.IntoSet;
import io.deephaven.lang.completion.CustomCompletion;
import io.deephaven.server.session.TicketResolver;
import io.grpc.BindableService;

import javax.inject.Singleton;
import java.util.Collections;
import java.util.Set;

@Module
public interface ConsoleModule {
    @Binds
    @IntoSet
    BindableService bindConsoleServiceImpl(ConsoleServiceGrpcBinding consoleService);

    @Binds
    @IntoSet
    TicketResolver bindConsoleTicketResolver(ScopeTicketResolver resolver);

    @Provides
    @Singleton
    static ScriptSessionCacheInit bindScriptSessionCacheInit() {
        return new ScriptSessionCacheInit();
    }

    @Provides
    @ElementsIntoSet
    static Set<CustomCompletion.Factory> primeCustomCompletions() {
        return Collections.emptySet();
    }
}
