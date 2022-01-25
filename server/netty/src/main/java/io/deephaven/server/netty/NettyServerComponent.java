package io.deephaven.server.netty;

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
        NettyServerModule.class
})
public interface NettyServerComponent extends DeephavenApiServerComponent {
    @Component.Builder
    interface Builder extends DeephavenApiServerComponent.Builder<Builder> {
        NettyServerComponent build();
    }
}
