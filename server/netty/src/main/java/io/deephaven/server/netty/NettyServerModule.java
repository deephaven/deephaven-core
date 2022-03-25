package io.deephaven.server.netty;

import dagger.Module;
import dagger.Provides;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;

import javax.inject.Named;
import java.io.File;
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

        String chainLoc = System.getenv("DH_TLS_CHAIN");
        if (chainLoc != null) {
            String chainKey = System.getenv("DH_TLS_KEY");
            final File chain = new File(chainLoc);
            final File key = new File(chainKey);
            if (!chain.exists()) {
                throw new IllegalArgumentException("TLS chain " + chain + " does not exist!");
            }
            if (!key.exists()) {
                throw new IllegalArgumentException("TLS key " + key + " does not exist!");
            }
            serverBuilder.useTransportSecurity(chain, key);
        }
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);

        serverBuilder.maxInboundMessageSize(maxMessageSize);

        Server server = serverBuilder.directExecutor().build();

        return GrpcServer.of(server);
    }
}
