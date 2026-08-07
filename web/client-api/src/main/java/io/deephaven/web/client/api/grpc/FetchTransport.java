//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import elemental2.core.JsArray;
import elemental2.core.JsError;
import elemental2.core.JsIIterableResult;
import elemental2.core.JsIteratorIterable;
import elemental2.core.Uint8Array;
import elemental2.dom.AbortController;
import elemental2.dom.Blob;
import elemental2.dom.DomGlobal;
import elemental2.dom.Headers;
import elemental2.dom.ReadableStreamDefaultReader;
import elemental2.dom.RequestInit;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

public class FetchTransport implements GrpcTransport {
    public static class Factory implements GrpcTransportFactory {
        @Override
        public boolean getSupportsClientStreaming() {
            return false;
        }

        @Override
        public GrpcTransport create(GrpcTransportOptions options) {
            return new FetchTransport(options);
        }
    }

    private final GrpcTransportOptions options;
    private final RequestInit init = RequestInit.create();
    private final AbortController abortController = new AbortController();

    private final JsArray<Blob.ConstructorBlobPartsArrayUnionType> messages = JsArray.of();


    public FetchTransport(GrpcTransportOptions options) {
        this.options = options;
        init.setMethod("POST");
        init.setSignal(abortController.signal);
    }

    @Override
    public void start(JsPropertyMap<HeaderValueUnion> metadata) {
        init.setHeaders(Js.<JsPropertyMap<String>>uncheckedCast(metadata));
    }

    @Override
    public void sendMessage(Uint8Array msgBytes) {
        messages.push(Blob.ConstructorBlobPartsArrayUnionType.of(msgBytes));
    }

    @Override
    public void finishSend() {
        init.setBody(new Blob(messages));
        DomGlobal.fetch(options.url, init).then(response -> {
            if (!abortController.signal.aborted) {
                options.onHeaders.onHeaders(readHeaders(response.headers), response.status);
                if (response.status != 200) {
                    // Not a gRPC response
                    options.onEnd.onEnd(new JsError("HTTP error " + response.status + ": " + response.statusText));
                } else {
                    // 200 means the call was handled by a gRPC server. In theory a browser can omit
                    // the body, but at this time they never do
                    if (response.body == null) {
                        // Emit an error if we weren't expecting this (otherwise we just end)
                        options.onEnd.onEnd(new JsError("Response body is null"));
                    } else {
                        this.readFromStream(response.body.getReader().asReadableStreamDefaultReader());
                    }
                }
            }
            return null;
        }).catch_(error -> {
            if (!abortController.signal.aborted) {
                if (error instanceof JsError e) {
                    options.onEnd.onEnd(e);
                } else {
                    options.onEnd.onEnd(new JsError(error));
                }
                abortController.abort();
            }
            return null;
        });
    }

    @Override
    public void cancel() {
        abortController.abort();
    }

    private void readFromStream(final ReadableStreamDefaultReader<Uint8Array> reader) {
        if (abortController.signal.aborted) {
            return;
        }

        reader.read().then(result -> {
            if (result.isDone()) {
                if (!abortController.signal.aborted) {
                    options.onEnd.onEnd(new JsError("Stream closed without trailers"));
                    abortController.abort();
                }
                return null;
            }

            options.onChunk.onChunk(result.getValue());

            // read the next chunk
            readFromStream(reader);
            return null;
        }, fail -> {
            if (!abortController.signal.aborted) {
                options.onEnd.onEnd(new JsError("Stream closed without trailers"));
                abortController.abort();
            }
            return null;
        });
    }

    private static JsPropertyMap<HeaderValueUnion> readHeaders(final Headers headers) {
        JsPropertyMap<HeaderValueUnion> headersMap = JsPropertyMap.of();
        final JsIteratorIterable<String, Object, Object> keys = headers.keys();
        JsIIterableResult<String> next = keys.next();
        while (!next.isDone()) {
            final String key = next.getValue();
            final String value = headers.get(key);
            headersMap.set(key, HeaderValueUnion.of(value));
            next = keys.next();
        }
        return headersMap;
    }
}
