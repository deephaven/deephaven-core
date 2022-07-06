package io.grpc.servlet.web.websocket;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Supports both grpc-websockets and grpc-websockets-multiplex subprotocols, and delegates to the correct implementation
 * after protocol negotiation.
 */
public class GrpcWebsocket extends Endpoint {
    private final Map<String, Supplier<Endpoint>> endpointFactories = new HashMap<>();
    private Endpoint endpoint;

    public GrpcWebsocket(Map<String, Supplier<Endpoint>> endpoints) {
        endpointFactories.putAll(endpoints);
    }

    public void onOpen(Session session, EndpointConfig endpointConfig) {
        Supplier<Endpoint> supplier = endpointFactories.get(session.getNegotiatedSubprotocol());
        if (supplier == null) {
            try {
                session.close(new CloseReason(CloseReason.CloseCodes.PROTOCOL_ERROR, "Unsupported subprotocol"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }

        endpoint = supplier.get();
        endpoint.onOpen(session, endpointConfig);
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        endpoint.onClose(session, closeReason);
    }

    @Override
    public void onError(Session session, Throwable thr) {
        endpoint.onError(session, thr);
    }
}
