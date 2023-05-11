/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import io.deephaven.server.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.ssl.config.CiphersIntermediate;
import io.deephaven.ssl.config.ProtocolsIntermediate;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.InternalStatus;
import io.grpc.internal.GrpcUtil;
import io.grpc.servlet.web.websocket.GrpcWebsocket;
import io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream;
import io.grpc.servlet.web.websocket.WebSocketServerStream;
import io.grpc.servlet.jakarta.web.GrpcWebFilter;
import jakarta.servlet.DispatcherType;
import jakarta.websocket.Endpoint;
import jakarta.websocket.server.ServerEndpointConfig;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.JettySslUtils;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http2.HTTP2Connection;
import org.eclipse.jetty.http2.HTTP2Session;
import org.eclipse.jetty.http2.parser.RateControl;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnection;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.MultiException;
import org.eclipse.jetty.util.component.Graceful;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.websocket.jakarta.common.SessionTracker;
import org.eclipse.jetty.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer;
import org.eclipse.jetty.websocket.jakarta.server.internal.JakartaWebSocketServerContainer;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream.GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL;
import static io.grpc.servlet.web.websocket.WebSocketServerStream.GRPC_WEBSOCKETS_PROTOCOL;
import static org.eclipse.jetty.servlet.ServletContextHandler.NO_SESSIONS;

public class JettyBackedGrpcServer implements GrpcServer {

    private final Server jetty;
    private final boolean websocketsEnabled;

    @Inject
    public JettyBackedGrpcServer(
            final JettyConfig config,
            final GrpcFilter filter) {
        jetty = new Server();
        jetty.addConnector(createConnector(jetty, config));

        final WebAppContext context =
                new WebAppContext(null, "/", null, null, null, new ErrorPageErrorHandler(), NO_SESSIONS);
        try {
            String knownFile = "/ide/index.html";
            URL ide = JettyBackedGrpcServer.class.getResource(knownFile);
            Resource jarContents = Resource.newResource(ide.toExternalForm().replace("!" + knownFile, "!/"));
            context.setBaseResource(ControlledCacheResource.wrap(jarContents));
        } catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
        context.setInitParameter(DefaultServlet.CONTEXT_INIT + "dirAllowed", "false");

        // Cache all of the appropriate assets folders
        for (String appRoot : List.of("/ide/", "/iframe/table/", "/iframe/chart/")) {
            context.addFilter(NoCacheFilter.class, appRoot + "*", EnumSet.noneOf(DispatcherType.class));
            context.addFilter(CacheFilter.class, appRoot + "assets/*", EnumSet.noneOf(DispatcherType.class));
        }
        context.addFilter(NoCacheFilter.class, "/jsapi/*", EnumSet.noneOf(DispatcherType.class));
        context.addFilter(DropIfModifiedSinceHeader.class, "/*", EnumSet.noneOf(DispatcherType.class));

        context.setSecurityHandler(new ConstraintSecurityHandler());

        // Add an extra filter to redirect from / to /ide/
        context.addFilter(HomeFilter.class, "/", EnumSet.noneOf(DispatcherType.class));

        // If requested, permit CORS requests
        FilterHolder holder = new FilterHolder(CrossOriginFilter.class);

        // Permit all origins
        holder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");

        // Only support POST - technically gRPC can use GET, but we don't use any of those methods
        holder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "POST");

        // Required request headers for gRPC, gRPC-web, flight, and deephaven
        holder.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, String.join(",",
                // Required for CORS itself to work
                HttpHeader.ORIGIN.asString(),
                CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER,

                // Required for gRPC
                GrpcUtil.CONTENT_TYPE_KEY.name(),
                GrpcUtil.TIMEOUT_KEY.name(),

                // Optional for gRPC
                GrpcUtil.MESSAGE_ENCODING_KEY.name(),
                GrpcUtil.MESSAGE_ACCEPT_ENCODING_KEY.name(),
                GrpcUtil.CONTENT_ENCODING_KEY.name(),
                GrpcUtil.CONTENT_ACCEPT_ENCODING_KEY.name(),

                // Required for gRPC-web
                "x-grpc-web",
                // Optional for gRPC-web
                "x-user-agent",

                // Required for Flight auth 1/2
                AuthConstants.TOKEN_NAME,
                Auth2Constants.AUTHORIZATION_HEADER,

                // Required for DH gRPC browser bidi stream support
                BrowserStreamInterceptor.TICKET_HEADER_NAME,
                BrowserStreamInterceptor.SEQUENCE_HEADER_NAME,
                BrowserStreamInterceptor.HALF_CLOSE_HEADER_NAME));

        // Response headers that the browser will need to be able to decode
        holder.setInitParameter(CrossOriginFilter.EXPOSED_HEADERS_PARAM, String.join(",",
                Auth2Constants.AUTHORIZATION_HEADER,
                GrpcUtil.CONTENT_TYPE_KEY.name(),
                InternalStatus.CODE_KEY.name(),
                InternalStatus.MESSAGE_KEY.name(),
                // Not used (yet?), see io.grpc.protobuf.StatusProto
                "grpc-status-details-bin"));

        // Add the filter on all requests
        context.addFilter(holder, "/*", EnumSet.noneOf(DispatcherType.class));

        // Handle grpc-web connections, translate to vanilla grpc
        context.addFilter(new FilterHolder(new GrpcWebFilter()), "/*", EnumSet.noneOf(DispatcherType.class));

        // Wire up the provided grpc filter
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.noneOf(DispatcherType.class));

        // Set up websockets for grpc-web - depending on configuration, we can register both in case we encounter a
        // client using "vanilla"
        // grpc-websocket, that can't multiplex all streams on a single socket
        if (config.websocketsOrDefault() != JettyConfig.WebsocketsSupport.NONE) {
            JakartaWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
                final Map<String, Supplier<Endpoint>> endpoints = new HashMap<>();
                if (config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.BOTH
                        || config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.GRPC_WEBSOCKET) {
                    endpoints.put(GRPC_WEBSOCKETS_PROTOCOL, () -> filter.create(WebSocketServerStream::new));
                }
                if (config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.BOTH
                        || config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.GRPC_WEBSOCKET_MULTIPLEXED) {
                    endpoints.put(GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL,
                            () -> filter.create(MultiplexedWebSocketServerStream::new));
                }
                JakartaWebSocketServerContainer jettyWebsocketContainer = (JakartaWebSocketServerContainer) container;
                WebsocketFactory websocketFactory =
                        new WebsocketFactory(() -> new GrpcWebsocket(endpoints), jettyWebsocketContainer);
                jettyWebsocketContainer.addBean(websocketFactory);
                container.addEndpoint(ServerEndpointConfig.Builder.create(GrpcWebsocket.class, "/{service}/{method}")
                        .configurator(new ServerEndpointConfig.Configurator() {
                            @Override
                            public <T> T getEndpointInstance(Class<T> endpointClass) {
                                // noinspection unchecked
                                return (T) websocketFactory.create();
                            }
                        })
                        .subprotocols(new ArrayList<>(endpoints.keySet()))
                        .build());
            });
            this.websocketsEnabled = true;
        } else {
            this.websocketsEnabled = false;
        }

        // Note: handler order matters due to pathSpec order
        HandlerCollection handlers = new HandlerCollection();
        // Set up /js-plugins/*
        JsPlugins.maybeAdd(handlers::addHandler);
        // Set up /*
        handlers.addHandler(context);
        jetty.setHandler(handlers);
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
    public void beginShutdown() {
        // "start to stop" the jetty container, skipping over websockets, since their Graceful implementation isn't
        // very nice. This is roughly the implementation of Graceful.shutdown(Component), except avoiding anything that
        // would directly stop a websocket, which instead will be handled later, as part of the actual stop() call tell
        // the graceful handlers that we are shutting down.

        // For websockets, since the SessionTracker will instantly stop the socket rather than allow it to finish
        // nicely. Instead, when websockets were created, we registered extra graceful beans to shutdown like h2.
        // See Server.doStop(), this is roughly the implementation of the first phase of that method, only asking
        // Graceful instances to stop, but not stopping connectors or non-graceful components.

        // Note that this would not apply correctly if we used WebSockets for some purpose other than gRPC transport.
        Collection<Graceful> gracefuls = jetty.getContainedBeans(Graceful.class);
        gracefuls.stream().filter(g -> !(g instanceof SessionTracker)).forEach(Graceful::shutdown);
    }

    @Override
    public void stopWithTimeout(long timeout, TimeUnit unit) {
        Thread shutdownThread = new Thread(() -> {
            MultiException exceptions = new MultiException();
            long millis = unit.toMillis(timeout);

            // If websockets are enabled, try to spend part of our shutdown timeout budget on waiting for websockets, as
            // in beginShutdown.
            if (websocketsEnabled && millis > 250) {
                // shut down everything except the websockets themselves with half our timeout
                millis /= 2;

                // Collect the same beans we gracefully stopped before (or, if we didn't already start a graceful
                // shutdown, this is the first attempt)
                Collection<Graceful> gracefuls = jetty.getContainedBeans(Graceful.class);
                try {
                    CompletableFuture.allOf(gracefuls.stream().filter(g -> !(g instanceof SessionTracker))
                            .map(Graceful::shutdown).toArray(CompletableFuture[]::new))
                            .get(millis, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }

            // regardless of failures so far, continue shutdown with remaining budget. This will end all websockets
            // right away.
            try {
                jetty.setStopTimeout(millis);
                jetty.stop();
                exceptions.ifExceptionThrow();
            } catch (Exception exception) {
                exceptions.add(exception);
            }
            exceptions.ifExceptionThrowRuntime();
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
        httpConfig.addCustomizer(new ForwardedRequestCustomizer());
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

        // Give connections extra time to shutdown, since we have an explicit server shutdown
        serverConnector.setShutdownIdleTimeout(serverConnector.getIdleTimeout());

        // Override the h2 stream timeout with a specified value
        serverConnector.addEventListener(new Connection.Listener() {
            @Override
            public void onOpened(Connection connection) {
                if (connection instanceof HTTP2ServerConnection) {
                    HTTP2Session session = (HTTP2Session) ((HTTP2Connection) connection).getSession();
                    session.setStreamIdleTimeout(config.http2StreamIdleTimeoutOrDefault());
                }
            }

            @Override
            public void onClosed(Connection connection) {

            }
        });

        return serverConnector;
    }
}
