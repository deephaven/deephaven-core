package io.deephaven.server.jetty;

import dagger.Component;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.MainModule;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        MainModule.class,
        JettyServerModule.class
})
public interface JettyServerComponent extends DeephavenApiServerComponent {
    @Component.Builder
    interface Builder extends MainModule.Builder<Builder> {
        JettyServerComponent build();
    }
}
