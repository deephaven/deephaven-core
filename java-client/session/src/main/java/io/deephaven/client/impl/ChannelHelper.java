/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.NettySslUtils;

import javax.net.ssl.SSLException;

public final class ChannelHelper {

    public static final int DEFAULT_TLS_PORT = 443;

    public static final int DEFAULT_PLAINTEXT_PORT = 10000;

    /**
     * Creates a {@link ManagedChannel} according to the {@code clientConfig}.
     *
     * <p>
     * If the target is secure, the channel will be configured with SSL. By default, the SSL configuration inherits
     * configuration logic from {@link TrustJdk} and {@link GrpcSslContexts#configure(SslContextBuilder)}. As mentioned
     * there: "precisely what is set is permitted to change, so if an application requires particular settings it should
     * override the options set".
     *
     * @param clientConfig the Deephaven client configuration
     * @return the channel
     */
    public static ManagedChannel channel(ClientConfig clientConfig) {
        final NettyChannelBuilder channelBuilder = NettyChannelBuilder
                .forTarget(clientConfig.target().toString())
                .maxInboundMessageSize(clientConfig.maxInboundMessageSize());
        if (clientConfig.target().isSecure()) {
            final SSLConfig ssl = clientConfig.ssl().orElseGet(SSLConfig::empty).orTrust(TrustJdk.of());
            final SSLFactory sslFactory = KickstartUtils.create(ssl);
            final SslContextBuilder sslBuilder = NettySslUtils.forClient(sslFactory);

            // GrpcSslContext potentially configures the following: protocols, ciphers, sslProvider,
            // applicationProtocolConfig, and sslContextProvider.
            GrpcSslContexts.configure(sslBuilder);

            if (ssl.protocols().isPresent() || ssl.ciphers().isPresent()) {
                // If the user was explicit, we'll re-set protocols and ciphers
                sslBuilder
                        .protocols(sslFactory.getProtocols())
                        .ciphers(sslFactory.getCiphers(), SupportedCipherSuiteFilter.INSTANCE);
            }
            try {
                channelBuilder.sslContext(sslBuilder.build());
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
        } else {
            channelBuilder.usePlaintext();
        }
        clientConfig.userAgent().ifPresent(channelBuilder::userAgent);
        return channelBuilder.build();
    }
}
