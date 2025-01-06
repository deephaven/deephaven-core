//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.python.server;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.client.impl.BarrageSessionFactoryConfig;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.integrations.python.PyLogOutputStream;
import io.deephaven.internal.log.Bootstrap;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferOutputStream;
import io.deephaven.server.auth.CommunityAuthorizationModule;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.console.ExecutionContextModule;
import io.deephaven.server.console.groovy.GroovyConsoleSessionModule;
import io.deephaven.server.console.python.PythonConsoleSessionModule;
import io.deephaven.server.console.python.PythonGlobalScopeModule;
import io.deephaven.server.flightsql.FlightSqlModule;
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
import io.deephaven.server.session.ClientChannelFactoryModule;
import io.deephaven.server.session.ClientChannelFactoryModule.UserAgent;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;
import io.deephaven.server.session.SslConfigModule;
import io.deephaven.time.calendar.CalendarsFromConfigurationModule;
import io.deephaven.util.process.ProcessEnvironment;
import org.jpy.PyModule;
import org.jpy.PyObject;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EmbeddedServer {

    @Module(includes = {
            ClientChannelFactoryModule.class,
            SslConfigModule.class
    })
    public interface EmbeddedPythonClientChannelFactoryModule {

        @Provides
        @UserAgent
        static String providesUserAgent() {
            return BarrageSessionFactoryConfig.userAgent(List.of("deephaven-server-embedded"));
        }
    }

    @Module(includes = {
            DeephavenApiServerModule.class,
            EmbeddedPyLogModule.class,
            DeephavenApiConfigModule.class,
            PythonGlobalScopeModule.class,
            HealthCheckModule.class,
            PythonPluginsRegistration.Module.class,
            JettyServerModule.class,
            FlightSqlModule.class,
            HealthCheckModule.class,
            PythonConsoleSessionModule.class,
            GroovyConsoleSessionModule.class,
            ExecutionContextModule.class,
            CommunityAuthorizationModule.class,
            ObfuscatingErrorTransformerModule.class,
            CalendarsFromConfigurationModule.class,
            EmbeddedPythonClientChannelFactoryModule.class,
    })
    public interface PythonServerModule {
    }

    @Singleton
    @Component(modules = PythonServerModule.class)
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

    @Inject
    ServerConfig serverConfig;

    @Inject
    Map<String, AuthenticationRequestHandler> authenticationHandlers;

    /**
     * Create an embedded server with the given host and port.
     * 
     * @param host the host to bind to
     * @param port the port to bind to
     * @param dict Deprecated, no longer used.
     * @throws IOException if the server cannot be started
     *
     * @deprecated use {@link #EmbeddedServer(String, Integer)} instead. dict is not used.
     */
    @Deprecated
    public EmbeddedServer(String host, Integer port, PyObject dict) throws IOException {
        this(host, port);
    }


    /**
     * Create an embedded server with the given host and port.
     * 
     * @param host the host to bind to
     * @param port the port to bind to
     * @throws IOException if the server cannot be started
     */
    public EmbeddedServer(String host, Integer port) throws IOException {
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

    public void prepareForShutdown() {
        // Stop any DH-started threads and work
        ProcessEnvironment.getGlobalShutdownManager().maybeInvokeTasks();

        // Shut down our wrapped stdout/stderr instances
        System.out.close();
        System.err.close();
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

    /**
     * Provide access to the server configuration.
     * 
     * @return the server configuration
     */
    public ServerConfig serverConfig() {
        return serverConfig;
    }

    /**
     * Provide access to the authentication handlers.
     * 
     * @return the authentication handlers
     */
    public Collection<AuthenticationRequestHandler> authenticationHandlers() {
        return authenticationHandlers.values();
    }
}
