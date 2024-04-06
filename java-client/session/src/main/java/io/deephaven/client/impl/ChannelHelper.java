//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.netty.util.NettySslUtils;

import javax.net.ssl.SSLException;
import java.util.Map;

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
        return channelBuilder(clientConfig).build();
    }

    /**
     * Initializes a {@link ManagedChannelBuilder} in the same fashion as {@link #channel(ClientConfig)}, but does not
     * {@link ManagedChannelBuilder#build()} it.
     *
     * @param clientConfig the Deephaven client configuration
     * @return the channel builder
     */
    public static ManagedChannelBuilder<?> channelBuilder(ClientConfig clientConfig) {
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
        clientConfig.overrideAuthority().ifPresent(channelBuilder::overrideAuthority);
        if (!clientConfig.extraHeaders().isEmpty()) {
            channelBuilder.intercept(MetadataUtils.newAttachHeadersInterceptor(of(clientConfig.extraHeaders())));
        }
        return channelBuilder;
    }

    private static Metadata of(Map<String, String> map) {
        final Metadata metadata = new Metadata();
        map.forEach((k, v) -> metadata.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v));
        return metadata;
    }
}
