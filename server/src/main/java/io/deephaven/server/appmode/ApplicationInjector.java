/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationState;
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

public class ApplicationInjector {

    private static final Logger log = LoggerFactory.getLogger(ApplicationInjector.class);

    private final AppMode appMode;
    private final Provider<ScriptSession> scriptSessionProvider;
    private final ApplicationTicketResolver ticketResolver;
    private final ApplicationState.Listener applicationListener;

    @Inject
    public ApplicationInjector(final AppMode appMode,
            final Provider<ScriptSession> scriptSessionProvider,
            final ApplicationTicketResolver ticketResolver,
            final ApplicationState.Listener applicationListener) {
        this.appMode = appMode;
        this.scriptSessionProvider = Objects.requireNonNull(scriptSessionProvider);
        this.ticketResolver = ticketResolver;
        this.applicationListener = applicationListener;
    }

    public void run() throws IOException, ClassNotFoundException {
        if (!ApplicationConfig.isApplicationModeEnabled()) {
            return;
        }

        final Path applicationDir = ApplicationConfig.applicationDir();
        log.info().append("Finding application(s) in '").append(applicationDir.toString()).append("'...").endl();

        List<ApplicationConfig> configs;
        try {
            configs = ApplicationConfig.find();
        } catch (final NoSuchFileException ignored) {
            configs = Collections.emptyList();
        } catch (final RuntimeException error) {
            log.error().append("Failed to read application config(s): ").append(error).endl();
            throw error;
        }

        if (configs.isEmpty()) {
            log.warn().append("No application(s) found...").endl();
            if (appMode != AppMode.HYBRID) {
                log.warn().append("No console sessions allowed...").endl();
            }
            return;
        }

        for (ApplicationConfig config : configs) {
            if (!config.isEnabled()) {
                log.info().append("Skipping disabled application: ").append(config.toString()).endl();
                continue;
            }
            loadApplication(applicationDir, config);
        }
    }

    private void loadApplication(final Path applicationDir, final ApplicationConfig config) {
        // Note: if we need to be more specific about which application we are starting, we can print out the path of
        // the application.
        log.info().append("Starting application '").append(config.toString()).append('\'').endl();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final ApplicationState app = ApplicationFactory.create(applicationDir, config,
                    scriptSessionProvider.get(), applicationListener);

            int numExports = app.listFields().size();
            log.info().append("\tfound ").append(numExports).append(" exports").endl();

            ticketResolver.onApplicationLoad(app);
        }
    }
}
