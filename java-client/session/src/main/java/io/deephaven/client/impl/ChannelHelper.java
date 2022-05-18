package io.deephaven.client.impl;

import io.deephaven.ssl.config.NettySSLConfig;
import io.deephaven.ssl.config.SSLConfig;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;

public final class ChannelHelper {

    public static final int DEFAULT_TLS_PORT = 443;

    public static final int DEFAULT_PLAINTEXT_PORT = 10000;

    /**
     * Creates a {@link ManagedChannel} according to the {@code clientConfig}.
     *
     * @param clientConfig the Deephaven client configuration
     * @return the channel
     */
    public static ManagedChannel channel(ClientConfig clientConfig) {
        final NettyChannelBuilder builder = NettyChannelBuilder
                .forTarget(clientConfig.target().toString())
                .maxInboundMessageSize(clientConfig.maxInboundMessageSize());
        if (clientConfig.target().isSecure()) {
            final SSLConfig ssl = clientConfig.sslOrDefault();
            final SslContextBuilder netty = NettySSLConfig.forClient(ssl);
            final SslContext grpc;
            try {
                grpc = GrpcSslContexts.configure(netty).build();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
            builder.sslContext(grpc);
        } else {
            builder.usePlaintext();
        }
        clientConfig.userAgent().ifPresent(builder::userAgent);
        return builder.build();
    }
}
