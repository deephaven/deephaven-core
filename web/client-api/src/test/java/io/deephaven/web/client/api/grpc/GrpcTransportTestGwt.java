//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.grpc;

import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.CoreClient;
import io.deephaven.web.client.api.Sort;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;

/**
 * Simple test to verify we can produce custom transports in JS. Only works with https, which means it can only be run
 * manually at this time, or it will trivially succeed.
 */
public class GrpcTransportTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        // This test runs in its own module to avoid risking poisoning other tests with its broken custom transports
        return "io.deephaven.web.CustomTransportTest";
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

    /**
     * Delegates to the default websocket transport, but once armed, ends every TableService/Batch call with a
     * {@code grpc-status} header and no {@code grpc-message}, as a proxy or server might when the connection drops. The
     * resulting {@link io.grpc.Status} has a {@code null} description.
     */
    private static class FailingBatchTransportFactory implements GrpcTransportFactory {
        private final GrpcTransportFactory delegate = new MultiplexedWebsocketTransport.Factory();
        private boolean failBatches;
        private int failedBatchCount;

        @Override
        public GrpcTransport create(GrpcTransportOptions options) {
            if (!failBatches || !options.url.pathname.endsWith("TableService/Batch")) {
                return delegate.create(options);
            }
            failedBatchCount++;
            return new GrpcTransport() {
                @Override
                public void start(JsPropertyMap<HeaderValueUnion> metadata) {
                    DomGlobal.setTimeout(ignore -> {
                        options.onHeaders.onHeaders(Js.cast(JsPropertyMap.of("grpc-status", "14")), 200);
                        options.onEnd.onEnd(null);
                    }, 0);
                }

                @Override
                public void sendMessage(Uint8Array msgBytes) {}

                @Override
                public void finishSend() {}

                @Override
                public void cancel() {}
            };
        }

        @Override
        public boolean getSupportsClientStreaming() {
            return delegate.getSupportsClientStreaming();
        }
    }

    /**
     * A batch stream that ends with a non-OK status but no description must still fail the batch with a usable message,
     * rather than tripping the "Failures must provide a reason!" assertion in ClientTableState.
     */
    public void testBatchFailureWithoutStatusDescription() {
        FailingBatchTransportFactory transportFactory = new FailingBatchTransportFactory();
        ConnectOptions connectOptions = new ConnectOptions();
        connectOptions.transportFactory = transportFactory;
        connect(new TableSourceBuilder()
                .script("from deephaven import empty_table")
                .script("batch_failure_table", "empty_table(10).update([\"I=i\"])"), connectOptions)
                .then(table("batch_failure_table"))
                .then(table -> {
                    delayTestFinish(5003);
                    // JsTable logs the failed sort as an error, so check the logged errors explicitly instead
                    errorLogsDontFail();
                    transportFactory.failBatches = true;
                    table.applySort(new Sort[] {table.findColumn("I").sort().desc()});
                    return waitFor(() -> !errorLogsSeen.isEmpty(), 10, 2004, table);
                })
                .then(table -> {
                    assertEquals(1, transportFactory.failedBatchCount);
                    assertEquals("Unexpected errors logged: " + errorLogsSeen, 1, errorLogsSeen.size());
                    Object[] errorLog = errorLogsSeen.get(0);
                    String logged = Arrays.toString(errorLog);
                    assertTrue("Unexpected error logged: " + logged,
                            String.valueOf(errorLog[0]).startsWith("Failed to apply sorts: "));
                    assertEquals("Expected a failure reason: " + logged, 2, errorLog.length);
                    assertTrue("Expected a failure reason: " + logged, "string".equals(Js.typeof(errorLog[1])));
                    assertFalse("Expected a failure reason: " + logged, Js.asString(errorLog[1]).isEmpty());

                    // the table rolls back to its last good state
                    assertEquals(0, table.getSort().length);
                    return Promise.resolve(table);
                })
                .then(this::finish).catch_(this::report);
    }

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

    /**
     * Dummy transport that never delivers headers and simply ends the stream, as happens when the server is unreachable
     * during a (re)connect attempt - the socket closes before any response is received.
     */
    private native GrpcTransportFactory makeEndsBeforeHeadersTransportFactory() /*-{
        return {
            create: function(options) {
                return {
                    start: function(metadata) {
                        // never call onHeaders; end the stream as if the server was never reached
                        $wnd.setTimeout(function() { options.onEnd(); }, 0);
                    },
                    sendMessage: function(msgBytes) {
                        // no-op
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

    /**
     * Regression test: when a transport's stream ends before any headers arrive, the channel must report a clean
     * failure rather than throwing while computing the closing status. Previously the status was derived from the
     * (never received) headers, throwing a TypeError before the failure was delivered - so callers such as the
     * reconnect logic never learned the attempt failed and hung indefinitely.
     */
    public void testTransportEndsBeforeHeaders() {
        // The failure path logs the resulting error; judge the test on whether login rejects, not on the logs.
        errorLogsDontFail();
        setupDhInternal().then(ignore -> {
            delayTestFinish(7103);
            ConnectOptions connectOptions = new ConnectOptions();
            connectOptions.transportFactory = makeEndsBeforeHeadersTransportFactory();
            CoreClient coreClient = new CoreClient(localServer, connectOptions);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS));
        }).then(success -> {
            // The stream ended before headers, so login must not succeed.
            reportUncaughtException(new AssertionError(
                    "Expected login to fail when the transport ends before headers, but it succeeded"));
            return Promise.resolve((Object) success);
        }, failure -> {
            // Expected: a clean rejection instead of an uncaught error or an indefinite hang.
            finishTest();
            return Promise.resolve((Object) failure);
        });
    }
}
