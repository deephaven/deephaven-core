//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.grpcweb.grpc;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.grpc.Client", namespace = JsPackage.GLOBAL)
public interface Client<TRequest, TResponse>
        extends io.deephaven.javascript.proto.dhinternal.grpcweb.client.Client<TRequest, TResponse> {
}
