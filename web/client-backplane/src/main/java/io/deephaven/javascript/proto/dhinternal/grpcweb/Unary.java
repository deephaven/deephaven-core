package io.deephaven.javascript.proto.dhinternal.grpcweb;

import io.deephaven.javascript.proto.dhinternal.grpcweb.invoke.Request;
import io.deephaven.javascript.proto.dhinternal.grpcweb.unary.UnaryRpcOptions;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.unary", namespace = JsPackage.GLOBAL)
public class Unary {
    public static native <TRequest, TResponse, M> Request unary(
        M methodDescriptor, UnaryRpcOptions<TRequest, TResponse> props);
}
