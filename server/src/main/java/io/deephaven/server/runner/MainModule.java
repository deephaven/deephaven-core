package io.deephaven.server.runner;

import dagger.BindsInstance;
import dagger.Module;
import io.deephaven.server.healthcheck.HealthCheckModule;

import javax.inject.Named;

@Module(includes = {
        DeephavenApiServerModule.class,
        HealthCheckModule.class
})
public interface MainModule {

    interface Builder<B extends Builder<B>> extends DeephavenApiServerComponent.Builder<B> {
        @BindsInstance
        B withPort(@Named("http.port") int port);

        @BindsInstance
        B withMaxInboundMessageSize(@Named("grpc.maxInboundMessageSize") int maxInboundMessageSize);
    }
}
