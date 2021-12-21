package io.deephaven.server.jetty;

import io.deephaven.server.runner.GrpcServer;
import io.grpc.servlet.web.websocket.WebSocketServerStream;
import jakarta.servlet.DispatcherType;
import jakarta.websocket.server.ServerEndpointConfig;
import org.eclipse.jetty.http2.parser.RateControl;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.eclipse.jetty.servlet.ServletContextHandler.SESSIONS;

public class JettyBackedGrpcServer implements GrpcServer {

    private final Server jetty;

    @Inject
    public JettyBackedGrpcServer(
            final @Named("http.port") int port,
            final GrpcFilter filter) {
        jetty = new Server(port);
        ServerConnector sc = (ServerConnector) jetty.getConnectors()[0];
        HTTP2CServerConnectionFactory factory =
                new HTTP2CServerConnectionFactory(new HttpConfiguration());
        factory.setRateControlFactory(new RateControl.Factory() {});
        sc.addConnectionFactory(factory);

        final WebAppContext context =
                new WebAppContext(null, "/", null, null, null, new ErrorPageErrorHandler(), SESSIONS);
        try {
            String knownFile = "/ide/index.html";
            URL ide = JettyBackedGrpcServer.class.getResource(knownFile);
            context.setBaseResource(Resource.newResource(ide.toExternalForm().replace("!" + knownFile, "!/")));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        // context.addFilter(NoCacheFilter.class, "*.nocache.js", EnumSet.noneOf(DispatcherType.class));
        // context.addFilter(CacheFilter.class, "*.cache.js", EnumSet.noneOf(DispatcherType.class));
        //
        // // For the Web UI, cache everything in the static folder
        // // https://create-react-app.dev/docs/production-build/#static-file-caching
        // context.addFilter(NoCacheFilter.class, "/iriside/*", EnumSet.noneOf(DispatcherType.class));
        // context.addFilter(CacheFilter.class, "/iriside/static/*", EnumSet.noneOf(DispatcherType.class));

        // Always add eTags
        context.setInitParameter("org.eclipse.jetty.servlet.Default.etags", "true");
        context.setSecurityHandler(new ConstraintSecurityHandler());

        context.setContextPath("/");
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.noneOf(DispatcherType.class));


        // FilterHolder filter = WebSocketUpgradeFilter.ensureFilter(context.getServletContext());
        // context.addFilter()
        jetty.setHandler(context);

        // set up websocket for grpc-web
        JakartaWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
            container.addEndpoint(
                    ServerEndpointConfig.Builder.create(WebSocketServerStream.class, "/{service}/{method}")
                            .configurator(new ServerEndpointConfig.Configurator() {
                                @Override
                                public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                                    return (T) filter.create(WebSocketServerStream::new);
                                }
                            })
                            .build());
        });

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
    public void shutdown() throws Exception {
        jetty.stop();
    }

    @Override
    public void shutdownNow() {
        // jetty.stop();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        jetty.join();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        jetty.join();
        return true;
    }
}
