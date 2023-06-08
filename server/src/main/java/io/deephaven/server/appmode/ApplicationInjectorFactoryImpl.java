/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.appmode;

import dagger.Binds;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Factory;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Objects;
import java.util.Set;

/**
 * An implementation that sources from {@link Factory}.
 */
public class ApplicationInjectorFactoryImpl implements ApplicationInjector {

    /**
     * Binds {@link ApplicationInjectorFactoryImpl} as {@link ApplicationInjector}.
     */
    @dagger.Module
    public interface Module {

        @Binds
        ApplicationInjector bindsImpl(ApplicationInjectorFactoryImpl impl);
    }

    private static final Logger log = LoggerFactory.getLogger(ApplicationInjectorFactoryImpl.class);

    private final Provider<ScriptSession> scriptSessionProvider;
    private final ApplicationTicketResolver ticketResolver;
    private final ApplicationState.Listener applicationListener;
    private final Set<Factory> factories;

    @Inject
    public ApplicationInjectorFactoryImpl(
            final Provider<ScriptSession> scriptSessionProvider,
            final ApplicationTicketResolver ticketResolver,
            final ApplicationState.Listener applicationListener,
            final Set<Factory> factories) {
        this.scriptSessionProvider = Objects.requireNonNull(scriptSessionProvider);
        this.ticketResolver = Objects.requireNonNull(ticketResolver);
        this.applicationListener = Objects.requireNonNull(applicationListener);
        this.factories = Objects.requireNonNull(factories);
    }

    @Override
    public void run() {
        for (Factory factory : factories) {
            loadApplicationFactory(factory);
        }
    }

    private void loadApplicationFactory(Factory factory) {
        log.info().append("Starting ApplicationState.Factory '").append(factory.toString()).append('\'').endl();
        final ApplicationState app;
        try (final SafeCloseable ignored1 = LivenessScopeStack.open();
                final SafeCloseable ignored2 = scriptSessionProvider.get().getExecutionContext().open()) {
            app = factory.create(applicationListener);
        }
        int numExports = app.listFields().size();
        log.info().append("\tfound ").append(numExports).append(" exports").endl();
        ticketResolver.onApplicationLoad(app);
    }
}
