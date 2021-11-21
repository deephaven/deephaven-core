package io.deephaven.server.netty;

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

public class NettyServerModule {
    @Provides
    static GrpcServer serverBuilder(
            final @Named("grpc.port") int port,
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors
    ) {
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port);
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        Server server = serverBuilder.directExecutor().build();

        return new GrpcServer() {
            @Override
            public void start() throws IOException {
                server.start();
            }

            @Override
            public void shutdown() {
                server.shutdown();
            }

            @Override
            public void shutdownNow() {
                server.shutdownNow();
            }

            @Override
            public void awaitTermination() throws InterruptedException {
                server.awaitTermination();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return server.awaitTermination(timeout, unit);
            }
        };
    }
}
