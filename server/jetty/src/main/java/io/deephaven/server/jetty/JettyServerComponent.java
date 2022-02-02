package io.deephaven.server.jetty;

import dagger.Component;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.DeephavenApiServerModule;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        HealthCheckModule.class,
        PythonPluginsRegistration.Module.class,
        JettyServerModule.class
})
public interface JettyServerComponent extends DeephavenApiServerComponent {
    @Component.Builder
    interface Builder extends DeephavenApiServerComponent.Builder<Builder> {
        JettyServerComponent build();
    }
}
