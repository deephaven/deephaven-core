package io.deephaven.server.runner;

import dagger.Module;

@Module(includes = {
        DeephavenApiServerModule.class,
        ServerBuilderInProcessModule.class
})
public interface InProcessModule {
}
