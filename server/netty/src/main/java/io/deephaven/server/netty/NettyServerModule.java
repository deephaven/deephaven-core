package io.deephaven.server.netty;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.ssl.config.NettySSLConfig;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.GrpcServer;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.util.Set;

@Module
public interface NettyServerModule {

    @Binds
    ServerConfig bindsServerConfig(NettyConfig serverConfig);

    @Provides
    static GrpcServer serverBuilder(
            NettyConfig serverConfig,
            Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        final NettyServerBuilder serverBuilder =
                NettyServerBuilder.forAddress(new InetSocketAddress(serverConfig.host(), serverConfig.port()));
        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);
        serverBuilder.maxInboundMessageSize(serverConfig.maxInboundMessageSize());
        if (serverConfig.ssl().isPresent()) {
            final SSLConfig ssl = serverConfig.ssl().get();
            final SslContextBuilder netty = NettySSLConfig.forServer(ssl);
            final SslContextBuilder grpc = GrpcSslContexts.configure(netty);
            try {
                serverBuilder.sslContext(grpc.build());
            } catch (SSLException e) {
                throw new UncheckedDeephavenException(e);
            }
        }
        Server server = serverBuilder.directExecutor().build();
        return GrpcServer.of(server);
    }
}
