package io.deephaven.web.client.api.grpc;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.CoreClient;
import jsinterop.base.JsPropertyMap;

/**
 * Simple test to verify we can produce custom transports in JS. Only works with https, which means it can only
 * be run manually at this time or it will triviall succeed.
 */
public class GrpcTransportGwtTest extends AbstractAsyncGwtTestCase {
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
                    debugger;
                        if (result.done) {
                            options.onEnd();
                        } else {
                            options.onChunk(result.value);
                            pump(reader, res);
                        }
                    })['catch'](function(e) {
                    debugger;
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
                        debugger;
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
            ConnectOptions connectOptions = new ConnectOptions();
            connectOptions.transportFactory = makeFetchTransportFactory();
                    CoreClient coreClient = new CoreClient(localServer, connectOptions);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
                    .then(ignore2 -> Promise.resolve(coreClient));
        }).then(this::finish).catch_(this::report);
    }

    /**
     * Dummy transport that just sends a single message and receives a single message. Doesn't actually talk to
     * the server, headers are empty, and the message is always 5 byte proto payload "no data".
     */
    private native GrpcTransportFactory makeDummyTransportFactory() /*-{
        return {
            create: function(options) {
                return {
                    start: function(metadata) {
                        // no-op
                        $wnd.setTimeout(function() {options.onHeaders({}, 200);}, 0);
                    },
                    sendMessage: function(msgBytes) {
                        // no-op
                        $wnd.setTimeout(function() {options.onChunk(new Uint8Array(5));}, 0);
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

//    public void testDummyGrpcTransport() {
//        setupDhInternal().then(ignore -> {
//            ConnectOptions connectOptions = new ConnectOptions();
//            connectOptions.transportFactory = makeDummyTransportFactory();
//            CoreClient coreClient = new CoreClient(localServer, connectOptions);
//            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
//                    .then(ignore2 -> Promise.resolve(coreClient));
//        }).then(this::finish).catch_(this::report);
//    }
}
