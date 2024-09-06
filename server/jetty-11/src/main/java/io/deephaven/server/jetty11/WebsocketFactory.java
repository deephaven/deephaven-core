//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import io.grpc.servlet.web.websocket.GrpcWebsocket;
import io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream;
import jakarta.websocket.Endpoint;
import org.eclipse.jetty.util.component.Graceful;
import org.eclipse.jetty.websocket.jakarta.server.internal.JakartaWebSocketServerContainer;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream.GRACEFUL_CLOSE;

/**
 * Helper class to bridge the gap between Jetty's Graceful interface and the jakarta implementation of the supported
 * grpc websocket transports.
 * <p>
 * </p>
 * This type is not an Endpoint, so only one instance of this can create and shutdown many endpoints.
 */
public class WebsocketFactory implements Graceful {
    private final AtomicReference<CompletableFuture<Void>> shutdown = new AtomicReference<>();

    private final Supplier<GrpcWebsocket> factory;
    private final JakartaWebSocketServerContainer jettyWebsocketContainer;

    public WebsocketFactory(Supplier<GrpcWebsocket> factory, JakartaWebSocketServerContainer jettyWebsocketContainer) {
        this.factory = factory;
        this.jettyWebsocketContainer = jettyWebsocketContainer;
    }

    public Endpoint create() {
        return factory.get();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        // Modeled after AbstractHTTP2ServerConnectionFactory.HTTP2SessionContainer.shutdown()
        CompletableFuture<Void> result = new CompletableFuture<>();
        // Simply by setting the shutdown, we don't allow new endpoint instances to be created
        if (shutdown.compareAndSet(null, result)) {
            // iterate created transports, and if we can, prevent new streams
            CompletableFuture.allOf(
                    jettyWebsocketContainer.getOpenSessions().stream()
                            .map(s -> (MultiplexedWebSocketServerStream.GracefulClose) s.getUserProperties()
                                    .get(GRACEFUL_CLOSE))
                            .map(MultiplexedWebSocketServerStream.GracefulClose::get)
                            .filter(Objects::nonNull)
                            .toArray(CompletableFuture[]::new))
                    .whenComplete((success, throwable) -> {
                        if (throwable == null) {
                            // When all clients have acknowledged, complete
                            result.complete(success);
                        } else {
                            result.completeExceptionally(throwable);
                        }
                    });
            return result;
        } else {
            return shutdown.get();
        }
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get() != null;
    }
}
