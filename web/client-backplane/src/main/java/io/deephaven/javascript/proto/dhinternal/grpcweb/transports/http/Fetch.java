//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.http;

import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportFactory;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.grpcWeb.transports.http.fetch",
        namespace = JsPackage.GLOBAL)
public class Fetch {
    public static native TransportFactory FetchReadableStreamTransport(Object init);

    public static native boolean detectFetchSupport();
}
