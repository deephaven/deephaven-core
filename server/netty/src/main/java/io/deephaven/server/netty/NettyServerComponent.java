/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.server.console.python.PythonGlobalScopeCopyModule;
import io.deephaven.server.healthcheck.HealthCheckModule;
import io.deephaven.server.plugin.python.PythonPluginsRegistration;
import io.deephaven.server.runner.DeephavenApiConfigModule;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.DeephavenApiServerModule;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        DeephavenApiServerModule.class,
        DeephavenApiConfigModule.class,
        PythonGlobalScopeCopyModule.class,
        HealthCheckModule.class,
        PythonPluginsRegistration.Module.class,
        NettyServerModule.class
})
public interface NettyServerComponent extends DeephavenApiServerComponent {
    @Component.Builder
    interface Builder extends DeephavenApiServerComponent.Builder<Builder> {
        @BindsInstance
        Builder withNettyConfig(NettyConfig config);

        NettyServerComponent build();
    }
}
