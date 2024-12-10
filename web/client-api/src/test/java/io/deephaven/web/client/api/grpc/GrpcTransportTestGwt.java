//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.CoreClient;
import jsinterop.base.JsPropertyMap;

/**
 * Simple test to verify we can produce custom transports in JS. Only works with https, which means it can only be run
 * manually at this time, or it will trivially succeed.
 */
public class GrpcTransportTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    /**
     * Simple fetch impl, with no cancelation handling.
     */
    public native GrpcTransportFactory makeFetchTransportFactory() /*-{
        return {
            create: function(options) {
                function pump(reader, res) {
                    reader.read().then(function(result) {
                        if (result.done) {
                            options.onEnd();
                        } else {
                            options.onChunk(result.value);
                            pump(reader, res);
                        }
                    })['catch'](function(e) {
                        options.onEnd(e);
                    });
                }
                return {
                    start: function(metadata) {
                        this.metadata = metadata;
                    },
                    sendMessage: function(msgBytes) {
                        var fetchInit = {
                            headers: new Headers(this.metadata),
                            method: "POST",
                            body: msgBytes,
                        };
                        $wnd.fetch(options.url.href, fetchInit).then(function(response) {
                            var m = {};
                            response.headers.forEach(function(value, key) {
                                m[key] = value;
                            });
                            options.onHeaders(m, response.status);
                            if (response.body) {
                                pump(response.body.getReader(), response);
                            }
                            return response;
                        })['catch'](function(e) {
                            options.onEnd(e);
                        });
                    },
                    finishSend: function() {
                        // no-op
                    },
                    cancel: function() {
                        // no-op
                    }
                };
            },
            supportsClientStreaming: false
        };
    }-*/;

    public void testFetchGrpcTransport() {
        if (!localServer.startsWith("https:")) {
            // We're using h2, so we need to be on https for our current implementation
            return;
        }
        setupDhInternal().then(ignore -> {
            delayTestFinish(7101);
            ConnectOptions connectOptions = new ConnectOptions();
            connectOptions.transportFactory = makeFetchTransportFactory();
            CoreClient coreClient = new CoreClient(localServer, connectOptions);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
                    .then(ignore2 -> Promise.resolve(coreClient));
        }).then(this::finish).catch_(this::report);
    }

    /**
     * Dummy transport that just sends a single message and receives a single message. Doesn't actually talk to the
     * server, headers are empty, and the message is always 5 byte proto payload "no data", followed by trailers
     * signifying success.
     */
    private native GrpcTransportFactory makeDummyTransportFactory() /*-{
        return {
            create: function(options) {
                return {
                    start: function(metadata) {
                        // empty headers
                        $wnd.setTimeout(function() {options.onHeaders({}, 200);}, 0);
                    },
                    sendMessage: function(msgBytes) {
                        // empty payload
                        var empty = new $wnd.Uint8Array(5);
                        // successful trailer payload
                        var trailersString = 'grpc-status:0';
                        var successTrailers = new $wnd.Uint8Array(5 + trailersString.length);
                        successTrailers[0] = 128;
                        successTrailers[4] = trailersString.length;
                        new $wnd.TextEncoder('utf-8').encodeInto(trailersString, successTrailers.subarray(5));
                        $wnd.setTimeout(function() {
                            // delay a bit, then send the empty messages and end the stream
                            options.onChunk(empty);
                            options.onChunk(successTrailers);
                            options.onEnd();
                        }, 0);
                    },
                    finishSend: function() {
                        // no-op
                    },
                    cancel: function() {
                        // no-op
                    }
                };
            },
            supportsClientStreaming: true
        };
    }-*/;

    public void testDummyGrpcTransport() {
        setupDhInternal().then(ignore -> {
            delayTestFinish(7102);
            ConnectOptions connectOptions = new ConnectOptions();
            connectOptions.transportFactory = makeDummyTransportFactory();
            connectOptions.debug = true;
            CoreClient coreClient = new CoreClient(localServer, connectOptions);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
                    .then(ignore2 -> Promise.resolve(coreClient));
        }).then(this::finish).catch_(this::report);
    }
}
