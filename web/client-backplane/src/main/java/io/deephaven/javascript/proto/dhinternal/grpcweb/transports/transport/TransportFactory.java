//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport;

import jsinterop.annotations.JsFunction;

@JsFunction
public interface TransportFactory {
    Transport onInvoke(TransportOptions options);
}
