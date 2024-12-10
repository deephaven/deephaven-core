//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.JsError;
import elemental2.core.Uint8Array;
import elemental2.dom.URL;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 * Options for creating a gRPC stream transport instance.
 */
@TsInterface
@JsType(namespace = "dh.grpc")
public class GrpcTransportOptions {
    @JsFunction
    @FunctionalInterface
    public interface OnHeadersCallback {
        void onHeaders(JsPropertyMap<HeaderValueUnion> headers, int status);
    }

    @JsFunction
    @FunctionalInterface
    public interface OnChunkCallback {
        void onChunk(Uint8Array chunk);
    }

    @JsFunction
    @FunctionalInterface
    public interface OnEndCallback {
        void onEnd(@JsOptional @JsNullable JsError error);
    }

    /**
     * The gRPC method URL.
     */
    public URL url;

    /**
     * True to enable debug logging for this stream.
     */
    public boolean debug;

    /**
     * Callback for when headers and status are received. The headers are a map of header names to values, and the
     * status is the HTTP status code. If the connection could not be made, the status should be 0.
     */
    public OnHeadersCallback onHeaders;

    /**
     * Callback for when a chunk of data is received.
     */
    public OnChunkCallback onChunk;

    /**
     * Callback for when the stream ends, with an error instance if it can be provided. Note that the present
     * implementation does not consume errors, even if provided.
     */
    public OnEndCallback onEnd;

    /**
     * Internal copy of options, to be used for fallback.
     */
    @JsIgnore
    public TransportOptions originalOptions;

    /**
     * Convert a {@link TransportOptions} instance to a {@link GrpcTransportOptions} instance.
     */
    @JsIgnore
    public static GrpcTransportOptions from(TransportOptions options) {
        GrpcTransportOptions impl = new GrpcTransportOptions();
        impl.url = new URL(options.getUrl());
        impl.debug = options.isDebug();
        impl.onHeaders = (headers, status) -> options.getOnHeaders().onInvoke(new BrowserHeaders(headers), status);
        impl.onChunk = p0 -> {
            // "false" because the underlying implementation doesn't rely on this anyway.
            options.getOnChunk().onInvoke(p0, false);
        };
        impl.onEnd = options.getOnEnd()::onInvoke;
        return impl;
    }
}
