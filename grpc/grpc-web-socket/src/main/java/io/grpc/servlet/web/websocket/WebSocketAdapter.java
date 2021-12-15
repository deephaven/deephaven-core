package io.grpc.servlet.web.websocket;

import io.grpc.Attributes;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.ServerTransportListener;

import java.util.List;

public class WebSocketAdapter {
    private final ServerTransportListener transportListener;
    private final List<? extends ServerStreamTracer.Factory> streamTracerFactories;
    private final int maxInboundMessageSize;
    private final Attributes attributes;

    public WebSocketAdapter(ServerTransportListener transportListener,
            List<? extends ServerStreamTracer.Factory> streamTracerFactories, int maxInboundMessageSize,
            Attributes attributes) {
        this.transportListener = transportListener;
        this.streamTracerFactories = streamTracerFactories;
        this.maxInboundMessageSize = maxInboundMessageSize;
        this.attributes = attributes;
    }

    public ServerTransportListener getTransportListener() {
        return transportListener;
    }

    public List<? extends ServerStreamTracer.Factory> getStreamTracerFactories() {
        return streamTracerFactories;
    }

    public int getMaxInboundMessageSize() {
        return maxInboundMessageSize;
    }

    public Attributes getAttributes() {
        return attributes;
    }
}
