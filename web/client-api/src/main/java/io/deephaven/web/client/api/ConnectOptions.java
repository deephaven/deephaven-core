//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.web.client.api.grpc.GrpcTransportFactory;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Addition optional configuration that can be passed to the {@link CoreClient} constructor.
 */
@JsType(namespace = "dh")
public class ConnectOptions {
    /**
     * Optional map of http header names and values to send to the server with each request.
     */
    @JsNullable
    public JsPropertyMap<String> headers = Js.uncheckedCast(JsPropertyMap.of());

    /**
     * True to enable debug logging. At this time, only enables logging for gRPC calls.
     */
    @JsNullable
    public boolean debug = false;

    /**
     * Set this to true to force the use of websockets when connecting to the deephaven instance, false to force the use
     * of {@code fetch}. Ignored if {@link #transportFactory} is set.
     * <p>
     * Defaults to null, indicating that the server URL should be checked to see if we connect with fetch or websockets.
     */
    @JsNullable
    public Boolean useWebsockets;

    /**
     * The transport factory to use for creating gRPC streams. If specified, the JS API will ignore
     * {@link #useWebsockets} and its own internal logic for determining the appropriate transport to use.
     * <p>
     * Defaults to null, indicating that the JS API should determine the appropriate transport to use. If
     * {@code useWebsockets} is set to true, the JS API will use websockets, otherwise if the server url begins with
     * https, it will use fetch, otherwise it will use websockets.
     */
    @JsNullable
    public GrpcTransportFactory transportFactory;

    public ConnectOptions() {

    }

    @JsIgnore
    public ConnectOptions(Object connectOptions) {
        this();
        JsPropertyMap<Object> map = Js.asPropertyMap(connectOptions);
        if (map.has("headers")) {
            headers = Js.uncheckedCast(map.getAsAny("headers").asPropertyMap());
        }
        if (map.has("debug")) {
            debug = map.getAsAny("debug").asBoolean();
        }
        if (map.has("useWebsockets")) {
            useWebsockets = map.getAsAny("useWebsockets").asBoolean();
        }
        // TODO (deephaven-core#6214) provide our own grpc-web library that can replace fetch
        // if (map.has("fetch")) {
        // fetch = map.getAsAny("fetch").uncheckedCast();
        // }
        if (map.has("transportFactory")) {
            transportFactory = map.getAsAny("transportFactory").uncheckedCast();
        }
    }
}
