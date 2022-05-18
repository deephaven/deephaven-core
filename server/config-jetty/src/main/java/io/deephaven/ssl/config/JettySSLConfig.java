package io.deephaven.ssl.config;

import nl.altindag.ssl.util.JettySslUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class JettySSLConfig {

    /**
     * Creates a Jetty server SSL context factory from {@code config}.
     *
     * @param config the SSL config
     * @return the Jetty server SSL context factory
     */
    public static SslContextFactory forServer(SSLConfig config) {
        // Want to use explicit type SslContextFactory.Server, but can't
        // TODO(deephaven-core#2404): combined-javadoc fundamentally flawed
        return JettySslUtils.forServer(DeephavenSslUtils.create(config));
    }

    /**
     * Creates a Jetty client SSL context factory from {@code config}.
     *
     * @param config the SSL config
     * @return the Jetty client SSL context factory
     */
    public static SslContextFactory forClient(SSLConfig config) {
        // Want to use explicit type SslContextFactory.Client, but can't
        // TODO(deephaven-core#2404): combined-javadoc fundamentally flawed
        return JettySslUtils.forClient(DeephavenSslUtils.create(config));
    }
}
