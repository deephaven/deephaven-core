package io.deephaven.server.netty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;

import javax.inject.Named;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Module
public class NettyServerModule {
    @Provides
    static GrpcServer serverBuilder(
            final @Named("http.port") int port,
            final @Named("grpc.maxInboundMessageSize") int maxMessageSize,
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port);
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        Server server = serverBuilder.directExecutor().build();

        return GrpcServer.of(server);
    }
}
