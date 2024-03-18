//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import elemental2.dom.DomGlobal;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.web.client.api.grpc.MultiplexedWebsocketTransport;
import jsinterop.base.JsPropertyMap;

/**
 * Holds some static details, must be initialized before any calls can be made. When our gRPC-web client is redone to
 * support interceptors/etc, this will likely no longer need to exist
 */
public class WebGrpcUtils {
    /**
     * One place to change all debug flags in the app.
     */
    public static final Object CLIENT_OPTIONS = JsPropertyMap.of(
            "debug", false);

    /**
     * True if https isn't available (as a proxy for h2).
     */
    public static final boolean USE_WEBSOCKETS;

    static {
        // TODO configurable, let us support this even when ssl?
        if (DomGlobal.window.location.protocol.equals("http:")) {
            USE_WEBSOCKETS = true;
            Grpc.setDefaultTransport.onInvoke(options -> new MultiplexedWebsocketTransport(options, () -> {
                Grpc.setDefaultTransport.onInvoke(Grpc.WebsocketTransport.onInvoke());
            }));
        } else {
            USE_WEBSOCKETS = false;
        }
    }
}
