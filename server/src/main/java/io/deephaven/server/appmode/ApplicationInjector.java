//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.ApplicationState.Factory;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ApplicationInjector {

    private static final Logger log = LoggerFactory.getLogger(ApplicationInjector.class);

    private final Provider<ScriptSession> scriptSessionProvider;
    private final ApplicationTicketResolver ticketResolver;
    private final ApplicationState.Listener applicationListener;
    private final Set<Factory> factories;

    @Inject
    public ApplicationInjector(
            final Provider<ScriptSession> scriptSessionProvider,
            final ApplicationTicketResolver ticketResolver,
            final ApplicationState.Listener applicationListener,
            final Set<Factory> factories) {
        this.scriptSessionProvider = Objects.requireNonNull(scriptSessionProvider);
        this.ticketResolver = ticketResolver;
        this.applicationListener = applicationListener;
        this.factories = factories;
    }

    public void run() throws IOException, ClassNotFoundException {
        for (ApplicationState.Factory factory : factories) {
            loadApplicationFactory(factory);
        }

        if (!ApplicationConfig.isCustomApplicationModeEnabled()) {
            return;
        }

        final Path applicationDir = ApplicationConfig.customApplicationDir();
        log.info().append("Finding custom application(s) in '").append(applicationDir.toString()).append("'...").endl();

        List<ApplicationConfig> configs;
        try {
            configs = ApplicationConfig.find();
        } catch (final NoSuchFileException ignored) {
            configs = Collections.emptyList();
        } catch (final RuntimeException error) {
            log.error().append("Failed to read custom application config(s): ").append(error).endl();
            throw error;
        }

        if (configs.isEmpty()) {
            log.warn().append("No custom application(s) found...").endl();
            return;
        }

        for (ApplicationConfig config : configs) {
            if (!config.isEnabled()) {
                log.info().append("Skipping disabled custom application: ").append(config.toString()).endl();
                continue;
            }
            loadApplication(applicationDir, config);
        }
    }

    private void loadApplication(final Path applicationDir, final ApplicationConfig config) {
        // Note: if we need to be more specific about which application we are starting, we can print out the path of
        // the application.
        log.info().append("Starting application '").append(config.toString()).append('\'').endl();

        final ApplicationState app;
        try (final SafeCloseable ignored = LivenessScopeStack.open();
                final SafeCloseable ignored2 = scriptSessionProvider.get().getExecutionContext().open()) {
            app = ApplicationFactory.create(applicationDir, config, scriptSessionProvider.get(), applicationListener);
        }
        int numExports = app.listFields().size();
        log.info().append("\tfound ").append(numExports).append(" exports").endl();
        ticketResolver.onApplicationLoad(app);
    }

    private void loadApplicationFactory(ApplicationState.Factory factory) {
        log.info().append("Starting ApplicationState.Factory '").append(factory.toString()).append('\'').endl();
        final ApplicationState app;
        try (final SafeCloseable ignored1 = LivenessScopeStack.open();
                final SafeCloseable ignored2 = scriptSessionProvider.get().getExecutionContext().open()) {
            app = factory.create(applicationListener);
        }
        int numExports = app.listFields().size();
        log.info().append("\tapp.id=").append(app.id()).append(", found ").append(numExports).append(" exports")
                .endl();
        ticketResolver.onApplicationLoad(app);
    }
}
