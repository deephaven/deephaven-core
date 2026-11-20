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
    private String holdUrlPart;

    private final class Open implements GrpcTransport {
        private final GrpcTransportOptions options;
        private final GrpcTransport transport;
        private boolean ended;

        Open(GrpcTransportOptions options) {
            this.options = options;
            GrpcTransportOptions wrapped = new GrpcTransportOptions();
            wrapped.url = options.url;
            wrapped.debug = options.debug;
            wrapped.onHeaders = (headers, status) -> {
                if (!held()) {
                    options.onHeaders.onHeaders(headers, status);
                }
            };
            wrapped.onChunk = chunk -> {
                if (!held()) {
                    options.onChunk.onChunk(chunk);
                }
            };
            wrapped.onEnd = error -> {
                if (!held()) {
                    end(error);
                }
            };
            transport = delegate.create(wrapped);
        }

        private boolean held() {
            return holdUrlPart != null && options.url.href.contains(holdUrlPart);
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

    /**
     * Ends every open stream whose URL contains {@code urlPart} as if the server had closed it with the given gRPC
     * status. Use a code that is not a transport error, so the client treats it as a failure of that stream alone.
     */
    public void failStreams(String urlPart, int grpcStatus, String message) {
        for (Open stream : new ArrayList<>(open)) {
            if (!stream.options.url.href.contains(urlPart)) {
                continue;
            }
            JsPropertyMap<HeaderValueUnion> trailers = JsPropertyMap.of();
            trailers.set("grpc-status", HeaderValueUnion.of(String.valueOf(grpcStatus)));
            trailers.set("grpc-message", HeaderValueUnion.of(message));
            stream.options.onHeaders.onHeaders(trailers, 200);
            stream.end(null);
            stream.transport.cancel();
        }
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

    /**
     * While set, responses on streams whose URL contains {@code urlPart} are not delivered, so such a request stays
     * pending until the streams are cut. {@code null} clears it.
     */
    public void holdStreams(String urlPart) {
        holdUrlPart = urlPart;
    }

    /** True while a stream whose URL contains {@code urlPart} is open. */
    public boolean hasOpenStream(String urlPart) {
        for (Open stream : open) {
            if (stream.options.url.href.contains(urlPart)) {
                return true;
            }
        }
        return false;
    }
}
