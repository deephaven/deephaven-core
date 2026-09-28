//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.grpc.fetch.AbstractGrpcWebChannel;
import com.vertispan.grpc.fetch.Transport;
import com.vertispan.grpc.fetch.TransportCallbacks;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.Uint8Array;
import elemental2.dom.URL;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Custom Channel that delegates to the user-provided http transport factory. The factory is reused, allowing for a
 * consistent, shared transport, but each gRPC call made on it can be from a unique Channel.
 */
public class CustomTransportChannel extends AbstractGrpcWebChannel {
    private final GrpcTransportFactory transportFactory;

    public CustomTransportChannel(final URL server, final GrpcTransportFactory transportFactory) {
        super(server);
        this.transportFactory = transportFactory;
    }

    @Override
    protected boolean transportSupportsBidiStreaming() {
        return transportFactory.getSupportsClientStreaming();
    }

    @Override
    protected Transport createTransport(final String method, final URL url, final TransportCallbacks callbacks) {
        final GrpcTransportOptions options = new GrpcTransportOptions();
        options.url = url;
        options.onChunk = callbacks::onChunk;
        options.onEnd = callbacks::onEnd;
        options.onHeaders = (headers, status) -> {
            // normalize the headers union values to a string
            final JsPropertyMap<String> h = JsPropertyMap.of();
            final JsArray<String> keys = JsObject.keys(headers);
            for (int i = 0; i < keys.length; i++) {
                final String key = keys.getAt(i);
                final HeaderValueUnion valueUnion = Js.cast(headers.get(key));
                final String value;
                if (valueUnion.isArray()) {
                    final JsArray<String> array = valueUnion.asArray();
                    value = String.join(",", array.asList());
                } else {
                    value = valueUnion.asString();
                }
                h.set(key, value);
            }

            callbacks.onHeaders(status, h);
        };
        final GrpcTransport transport = transportFactory.create(options);
        return new Transport() {
            @Override
            public void start(final JsPropertyMap<String> headers) {
                transport.start(Js.uncheckedCast(headers));
            }

            @Override
            public void sendMessage(final Uint8Array uint8Array) {
                transport.sendMessage(uint8Array);
            }

            @Override
            public void finishSend() {
                transport.finishSend();
            }

            @Override
            public void cancel() {
                transport.cancel();
            }
        };
    }
}
