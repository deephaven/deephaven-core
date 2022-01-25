package io.deephaven.server.runner;

import dagger.Component;
import io.grpc.ManagedChannelBuilder;

import javax.inject.Singleton;

@Singleton
@Component(modules = {InProcessModule.class})
public interface InProcessServerComponent extends DeephavenApiServerComponent {

    ManagedChannelBuilder<?> channelBuilder();

    @Component.Builder
    interface Builder extends DeephavenApiServerComponent.Builder<Builder> {
        InProcessServerComponent build();
    }
}
