package io.deephaven.python.server;

import dagger.Component;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.jetty.JettyServerModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiServer;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.DeephavenApiServerModule;
import io.deephaven.server.runner.Main;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EmbeddedServer {
    @Singleton
        @Component(modules = {
            DeephavenApiServerModule.class,
            HealthCheckModule.class,
            PythonPluginsRegistration.Module.class,
            JettyServerModule.class
    })
    public interface PythonServerComponent extends DeephavenApiServerComponent {
        @Component.Builder
        interface Builder extends DeephavenApiServerComponent.Builder<PythonServerComponent.Builder> {
            PythonServerComponent build();
        }
        void injectFields(EmbeddedServer instance);
    }

    @Inject
    DeephavenApiServer server;
    @Inject
    Map<String, Provider<ScriptSession>> scriptSessions;

    public EmbeddedServer(int port) throws IOException {
        final Configuration config = Main.init(new String[0], EmbeddedServer.class);
//        dict.asDict();

        int httpSessionExpireMs = config.getIntegerWithDefault("http.session.durationMs", 300000);
        int httpPort = port;
        int schedulerPoolSize = config.getIntegerWithDefault("scheduler.poolSize", 4);
        int maxInboundMessageSize = config.getIntegerWithDefault("grpc.maxInboundMessageSize", 100 * 1024 * 1024);

        DaggerEmbeddedServer_PythonServerComponent
                .builder()
                .withPort(httpPort)
                .withSchedulerPoolSize(schedulerPoolSize)
                .withSessionTokenExpireTmMs(httpSessionExpireMs)
                .withMaxInboundMessageSize(maxInboundMessageSize)
                .withOut(null)
                .withErr(null)
                .build()
                .injectFields(this);
    }

    public void start() throws Exception {
        new Thread(() -> {
            try {
                server.run();
                System.out.println("Server running on port " + server.server().getPort());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void stop() {
        server.server().stopWithTimeout(10, TimeUnit.SECONDS);
    }

    public void bind(String name, Object value) {
        scriptSessions.get("python").get().setVariable(name, value);
    }

}
