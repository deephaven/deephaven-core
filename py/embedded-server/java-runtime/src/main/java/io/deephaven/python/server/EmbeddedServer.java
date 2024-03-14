//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.python.server;

import dagger.Component;
import io.deephaven.client.ClientDefaultsModule;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.integrations.python.PyLogOutputStream;
import io.deephaven.internal.log.Bootstrap;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferOutputStream;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.time.calendar.CalendarsFromConfigurationModule;
import io.deephaven.server.console.ExecutionContextModule;
import io.deephaven.server.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.console.python.PythonGlobalScopeModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.jetty.JettyConfig;
import io.deephaven.server.jetty.JettyConfig.Builder;
import io.deephaven.server.jetty.JettyServerComponent;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiConfigModule;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.runner.DeephavenApiServerModule;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class EmbeddedServer {

    @Singleton
    @Component(modules = {
            DeephavenApiServerModule.class,
            EmbeddedPyLogModule.class,
            DeephavenApiConfigModule.class,
            PythonGlobalScopeModule.class,
            HealthCheckModule.class,
            PythonPluginsRegistration.Module.class,
            JettyServerModule.class,
            HealthCheckModule.class,
            PythonConsoleSessionModule.class,
            GroovyConsoleSessionModule.class,
            ExecutionContextModule.class,
            CommunityAuthorizationModule.class,
            ClientDefaultsModule.class,
            ObfuscatingErrorTransformerModule.class,
            CalendarsFromConfigurationModule.class,
    })
    public interface PythonServerComponent extends JettyServerComponent {
        @Component.Builder
        interface Builder extends JettyServerComponent.Builder<Builder, PythonServerComponent> {
        }

        void injectFields(EmbeddedServer instance);
    }

    @Inject
    DeephavenApiServer server;
    @Inject
    Provider<ScriptSession> scriptSession;

    // // this is a nice idea, but won't work, since this is the same instance that we had to disable via sysprop
    // @Inject
    // StreamToLogBuffer logBuffer;

    @Inject
    LogBuffer logBuffer;

    public EmbeddedServer(String host, Integer port, PyObject dict) throws IOException {
        // Redirect System.out and err to the python equivelents, in case python has (or will) redirected them.
        PyModule sys = PyModule.importModule("sys");
        System.setOut(new PrintStream(new PyLogOutputStream(() -> sys.getAttribute("stdout"))));
        System.setErr(new PrintStream(new PyLogOutputStream(() -> sys.getAttribute("stderr"))));

        final Configuration config = MainHelper.init(new String[0], EmbeddedServer.class);
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

        // We need to open the systemic execution context to permanently install the contexts for this thread.
        scriptSession.get().getExecutionContext().open();
    }

    public void start() throws Exception {
        server.run();

        Bootstrap.printf("Server started on port %d%n", getPort());
    }

    public int getPort() {
        return server.server().getPort();
    }

    /**
     * Provide a way for Python to "tee" log output into the Deephaven log buffers.
     */
    public OutputStream getStdout() {
        return new LogBufferOutputStream(logBuffer, LogLevel.STDOUT, 256, 1 << 19);
    }

    /**
     * Provide a way for Python to "tee" log output into the Deephaven log buffers.
     */
    public OutputStream getStderr() {
        return new LogBufferOutputStream(logBuffer, LogLevel.STDERR, 256, 1 << 19);
    }
}
