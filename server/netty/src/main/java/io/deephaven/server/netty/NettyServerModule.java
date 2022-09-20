/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.netty;

import dagger.Binds;
import dagger.Module;
import dagger.Provides;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.grpc.MTlsCertificate;
import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.NettySslUtils;

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
        final NettyServerBuilder serverBuilder;
        if (serverConfig.host().isPresent()) {
            serverBuilder = NettyServerBuilder
                    .forAddress(new InetSocketAddress(serverConfig.host().get(), serverConfig.port()));
        } else {
            serverBuilder = NettyServerBuilder.forPort(serverConfig.port());
        }
        serverBuilder.withOption(ChannelOption.SO_REUSEADDR, true);

        services.forEach(serverBuilder::addService);
        interceptors.forEach(serverBuilder::intercept);
        serverBuilder.intercept(MTlsCertificate.DEFAULT_INTERCEPTOR);
        serverBuilder.maxInboundMessageSize(serverConfig.maxInboundMessageSize());
        if (serverConfig.ssl().isPresent()) {
            final SSLConfig ssl = serverConfig.ssl().get().orTrust(TrustJdk.of());
            final SSLFactory kickstart = KickstartUtils.create(ssl);
            SslContextBuilder sslBuilder = NettySslUtils.forServer(kickstart);
            GrpcSslContexts.configure(sslBuilder);
            if (ssl.protocols().isPresent() || ssl.ciphers().isPresent()) {
                sslBuilder
                        .protocols(kickstart.getProtocols())
                        .ciphers(kickstart.getCiphers(), SupportedCipherSuiteFilter.INSTANCE);
            }
            try {
                serverBuilder.sslContext(sslBuilder.build());
            } catch (SSLException e) {
                throw new UncheckedDeephavenException(e);
            }
        }
        Server server = serverBuilder.directExecutor().build();
        return GrpcServer.of(server);
    }
}
