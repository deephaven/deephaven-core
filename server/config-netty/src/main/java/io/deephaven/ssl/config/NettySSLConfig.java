package io.deephaven.ssl.config;

import io.netty.handler.ssl.SslContextBuilder;
import nl.altindag.ssl.util.NettySslUtils;

public class NettySSLConfig {

    /**
     * Creates a Netty server SSL context builder from {@code config}.
     *
     * @param config the SSL config
     * @return the Netty server SSL context builder
     */
    public static SslContextBuilder forServer(SSLConfig config) {
        return NettySslUtils.forServer(DeephavenSslUtils.create(config));
    }

    /**
     * Creates a Netty client SSL context builder from {@code config}.
     *
     * @param config the SSL config
     * @return the Netty client SSL context builder
     */
    public static SslContextBuilder forClient(SSLConfig config) {
        return NettySslUtils.forClient(DeephavenSslUtils.create(config));
    }
}
