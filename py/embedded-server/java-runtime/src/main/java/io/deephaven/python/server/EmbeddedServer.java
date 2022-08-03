/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.python.server;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.console.python.PythonGlobalScopeModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.jetty.JettyConfig.Builder;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiConfigModule;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.DeephavenApiServerModule;
import io.deephaven.server.runner.Main;
import io.deephaven.server.util.Scheduler;
import org.jpy.PyObject;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;

public class EmbeddedServer {
    @Singleton
    @Component(modules = {
            DeephavenApiServerModule.class,
            DeephavenApiConfigModule.class,
            PythonGlobalScopeModule.class,
            HealthCheckModule.class,
            PythonPluginsRegistration.Module.class,
            JettyServerModule.class
    })
    public interface PythonServerComponent extends DeephavenApiServerComponent {
        @Component.Builder
        interface Builder extends DeephavenApiServerComponent.Builder<PythonServerComponent.Builder> {
            @BindsInstance
            Builder withJettyConfig(JettyConfig config);

            PythonServerComponent build();
        }

        void injectFields(EmbeddedServer instance);
    }

    @Inject
    DeephavenApiServer server;
    @Inject
    Scheduler scheduler;
    @Inject
    Provider<ScriptSession> scriptSession;

    public EmbeddedServer(String host, Integer port, PyObject dict) throws IOException {
        final Configuration config = Main.init(new String[0], EmbeddedServer.class);
        final Builder builder = JettyConfig.buildFromConfig(config);
        if (host != null) {
            builder.host(host);
        }
        if (port != null) {
            builder.port(port);
        }
        DaggerEmbeddedServer_PythonServerComponent
                .builder()
                .withJettyConfig(builder.build())
                .withOut(null)
                .withErr(null)
                .build()
                .injectFields(this);
    }

    public void start() throws Exception {
        server.run();
        checkGlobals(scriptSession.get(), null);
        System.out.println("Server started on port " + server.server().getPort());
    }

    private void checkGlobals(ScriptSession scriptSession, @Nullable ScriptSession.SnapshotScope lastSnapshot) {
        // TODO deephaven-core#2453 make this more generic, ideally by pushing this in whole or part into script session
        ScriptSession.SnapshotScope nextSnapshot;
        try {
            nextSnapshot = scriptSession.snapshot(lastSnapshot);
        } catch (IllegalStateException e) {
            if (e.getMessage().startsWith("Expected transition from=")) {
                // We are limited in how we can track external changes, and the web IDE has made this change and
                // already applied it.
                // Take a fresh snapshot right away to continue polling
                nextSnapshot = scriptSession.snapshot();
            } else {
                throw e;
            }
        }
        ScriptSession.SnapshotScope s = nextSnapshot;
        scheduler.runAfterDelay(100, () -> {
            checkGlobals(scriptSession, s);
        });
    }

    public int getPort() {
        return server.server().getPort();
    }
}
