package io.deephaven.grpc_api.runner;

import dagger.Module;
import dagger.Provides;
import io.grpc.ServerBuilder;

import javax.inject.Named;

@Module
public class ServerBuilderModule {

    @Provides
    static ServerBuilder<?> serverBuilder(final @Named("grpc.port") int port) {
        return ServerBuilder.forPort(port);
    }
}
