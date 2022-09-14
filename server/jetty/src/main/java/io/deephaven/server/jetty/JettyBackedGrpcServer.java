/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import io.deephaven.server.config.ServerConfig;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.ssl.config.CiphersIntermediate;
import io.deephaven.ssl.config.ProtocolsIntermediate;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.servlet.web.websocket.WebSocketServerStream;
import io.grpc.servlet.jakarta.web.GrpcWebFilter;
import jakarta.servlet.DispatcherType;
import jakarta.websocket.server.ServerEndpointConfig;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.JettySslUtils;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.parser.RateControl;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS;

public class JettyBackedGrpcServer implements GrpcServer {

    private final Server jetty;

    @Inject
    public JettyBackedGrpcServer(
            final JettyConfig config,
            final GrpcFilter filter) {
        jetty = new Server();
        jetty.addConnector(createConnector(jetty, config));

        final WebAppContext context =
                new WebAppContext(null, "/", null, null, null, new ErrorPageErrorHandler(), SESSIONS);
        try {
            String knownFile = "/ide/index.html";
            URL ide = JettyBackedGrpcServer.class.getResource(knownFile);
            context.setBaseResource(Resource.newResource(ide.toExternalForm().replace("!" + knownFile, "!/")));
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }

        // For the Web UI, cache everything in the static folder
        // https://create-react-app.dev/docs/production-build/#static-file-caching
        context.addFilter(NoCacheFilter.class, "/iriside/*", EnumSet.noneOf(DispatcherType.class));
        context.addFilter(CacheFilter.class, "/iriside/static/*", EnumSet.noneOf(DispatcherType.class));

        // Always add eTags
        context.setInitParameter("org.eclipse.jetty.servlet.Default.etags", "true");
        context.setSecurityHandler(new ConstraintSecurityHandler());

        // Add an extra filter to redirect from / to /ide/
        context.addFilter(HomeFilter.class, "/", EnumSet.noneOf(DispatcherType.class));

        // Direct jetty all use this configuration as the root application
        context.setContextPath("/");

        // Handle grpc-web connections, translate to vanilla grpc
        context.addFilter(new FilterHolder(new GrpcWebFilter()), "/*", EnumSet.noneOf(DispatcherType.class));

        // Wire up the provided grpc filter
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.noneOf(DispatcherType.class));

        // Set up websocket for grpc-web
        if (config.websocketsOrDefault()) {
            JakartaWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
                container.addEndpoint(
                        ServerEndpointConfig.Builder.create(WebSocketServerStream.class, "/{service}/{method}")
                                .configurator(new ServerEndpointConfig.Configurator() {
                                    @Override
                                    public <T> T getEndpointInstance(Class<T> endpointClass)
                                            throws InstantiationException {
                                        return (T) filter.create(WebSocketServerStream::new);
                                    }
                                })
                                .build());
            });
        }
        jetty.setHandler(context);
    }

    @Override
    public void start() throws IOException {
        try {
            jetty.start();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public void join() throws InterruptedException {
        jetty.join();
    }

    @Override
    public void stopWithTimeout(long timeout, TimeUnit unit) {
        jetty.setStopTimeout(unit.toMillis(timeout));
        Thread shutdownThread = new Thread(() -> {
            try {
                jetty.stop();
            } catch (Exception exception) {
                throw new IllegalStateException("Failure while stopping", exception);
            }
        });
        shutdownThread.start();
    }

    @Override
    public int getPort() {
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    private static ServerConnector createConnector(Server server, JettyConfig config) {
        // https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#pg-server-http-connector-protocol-http2-tls
        final HttpConfiguration httpConfig = new HttpConfiguration();
        final HttpConnectionFactory http11 = config.http1OrDefault() ? new HttpConnectionFactory(httpConfig) : null;
        final ServerConnector serverConnector;
        if (config.ssl().isPresent()) {
            // Consider allowing configuration of sniHostCheck
            final boolean sniHostCheck = true;
            httpConfig.addCustomizer(new SecureRequestCustomizer(sniHostCheck));
            final HTTP2ServerConnectionFactory h2 = new HTTP2ServerConnectionFactory(httpConfig);
            h2.setRateControlFactory(new RateControl.Factory() {});
            final ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
            alpn.setDefaultProtocol(http11 != null ? http11.getProtocol() : h2.getProtocol());
            // The Jetty server is getting intermediate setup by default if none are configured. This is most similar to
            // how the Netty servers gets setup by default via GrpcSslContexts.
            final SSLConfig sslConfig = config.ssl().get()
                    .orTrust(TrustJdk.of())
                    .orProtocols(ProtocolsIntermediate.of())
                    .orCiphers(CiphersIntermediate.of());
            final SSLFactory kickstart = KickstartUtils.create(sslConfig);
            final SslContextFactory.Server jetty = JettySslUtils.forServer(kickstart);
            final SslConnectionFactory tls = new SslConnectionFactory(jetty, alpn.getProtocol());
            if (http11 != null) {
                serverConnector = new ServerConnector(server, tls, alpn, h2, http11);
            } else {
                serverConnector = new ServerConnector(server, tls, alpn, h2);
            }
        } else {
            final HTTP2CServerConnectionFactory h2c = new HTTP2CServerConnectionFactory(httpConfig);
            h2c.setRateControlFactory(new RateControl.Factory() {});
            if (http11 != null) {
                serverConnector = new ServerConnector(server, http11, h2c);
            } else {
                serverConnector = new ServerConnector(server, h2c);
            }
        }
        config.host().ifPresent(serverConnector::setHost);
        serverConnector.setPort(config.port());
        return serverConnector;
    }
}
