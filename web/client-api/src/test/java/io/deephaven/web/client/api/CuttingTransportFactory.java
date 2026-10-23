//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsError;
import elemental2.core.Uint8Array;
import io.deephaven.web.client.api.grpc.GrpcTransport;
import io.deephaven.web.client.api.grpc.GrpcTransportFactory;
import io.deephaven.web.client.api.grpc.GrpcTransportOptions;
import io.deephaven.web.client.api.grpc.HeaderValueUnion;
import io.deephaven.web.client.api.grpc.MultiplexedWebsocketTransport;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;

/**
 * A transport that can drop every open stream on command, so a test can simulate losing the connection. Each stream is
 * delegated to a real transport; {@link #cutAll()} cancels the delegates and reports a transport error upward, which is
 * what the client sees when the network goes away.
 */
public class CuttingTransportFactory implements GrpcTransportFactory {
    private final GrpcTransportFactory delegate = new MultiplexedWebsocketTransport.Factory();
    private final List<Open> open = new ArrayList<>();
    private int cuts;

    private final class Open implements GrpcTransport {
        private final GrpcTransportOptions options;
        private final GrpcTransport transport;
        private boolean ended;

        Open(GrpcTransportOptions options) {
            this.options = options;
            GrpcTransportOptions wrapped = new GrpcTransportOptions();
            wrapped.url = options.url;
            wrapped.debug = options.debug;
            wrapped.onHeaders = options.onHeaders;
            wrapped.onChunk = options.onChunk;
            wrapped.onEnd = error -> end(error);
            transport = delegate.create(wrapped);
        }

        private void end(JsError error) {
            if (ended) {
                return;
            }
            ended = true;
            open.remove(this);
            options.onEnd.onEnd(error);
        }

        @Override
        public void start(JsPropertyMap<HeaderValueUnion> metadata) {
            transport.start(metadata);
        }

        @Override
        public void sendMessage(Uint8Array msgBytes) {
            transport.sendMessage(msgBytes);
        }

        @Override
        public void finishSend() {
            transport.finishSend();
        }

        @Override
        public void cancel() {
            ended = true;
            open.remove(this);
            transport.cancel();
        }
    }

    @Override
    public GrpcTransport create(GrpcTransportOptions options) {
        Open stream = new Open(options);
        open.add(stream);
        return stream;
    }

    @Override
    public boolean getSupportsClientStreaming() {
        return delegate.getSupportsClientStreaming();
    }

    /** Drops every stream that is open right now, as a lost connection would. */
    public void cutAll() {
        cuts++;
        for (Open stream : new ArrayList<>(open)) {
            stream.transport.cancel();
            stream.end(new JsError("transport cut by test (" + cuts + ")"));
        }
    }

    public int openStreams() {
        return open.size();
    }
}
