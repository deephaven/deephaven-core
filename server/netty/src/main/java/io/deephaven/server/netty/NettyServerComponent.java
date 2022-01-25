package io.deephaven.server.netty;

import dagger.Component;
import io.deephaven.server.runner.DeephavenApiServerComponent;
import io.deephaven.server.runner.MainModule;

import javax.inject.Singleton;

@Singleton
@Component(modules = {
        MainModule.class,
        NettyServerModule.class
})
public interface NettyServerComponent extends DeephavenApiServerComponent {
    @Component.Builder
    interface Builder extends MainModule.Builder<Builder> {
        NettyServerComponent build();
    }
}
