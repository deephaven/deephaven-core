package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.grpc.ServerBuilder;

import javax.inject.Named;

@Module
public class NettyServerBuilderModule {

    @Provides
    static ServerBuilder<?> serverBuilder(final @Named("http.port") int port) {
        return ServerBuilder.forPort(port);
    }
}
